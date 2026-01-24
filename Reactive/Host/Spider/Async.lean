/-
  Reactive/Host/Spider/Async.lean

  Async combinators for SpiderM: push-based state, async IO,
  event-driven async, and retry logic.
-/
import Reactive.Host.Spider.Core
import Reactive.Host.Spider.WorkerPool
import Reactive.Core.AsyncState
import Reactive.Core.Retry
import Std.Data.HashMap

namespace Reactive.Host

open Reactive
open Std (HashMap)

/-! ## Pattern 1: Push-Based State

Simple composition of existing primitives for creating
stateful dynamics with external update handles. -/

/-- Create a Dynamic with a push-based update function.
    Returns the dynamic and a function to update it. -/
def pushState (initial : a) : SpiderM (Dyn a × (a → IO Unit)) := ⟨fun env => do
  let (dyn, update) ← createDynamic env.timelineCtx initial
  let framedUpdate := fun v => env.withFrame (update v)
  pure (dyn, framedUpdate)⟩

/-- Create a Dynamic with both set and modify update functions.
    Returns (dynamic, set, modify) where:
    - set: directly set the value
    - modify: apply a function to the current value -/
def pushStateWithModify (initial : a) : SpiderM (Dyn a × (a → IO Unit) × ((a → a) → IO Unit)) := ⟨fun env => do
  let ref ← IO.mkRef initial
  let (dyn, update) ← createDynamic env.timelineCtx initial
  let framedUpdate := fun v => env.withFrame (update v)

  let set := fun v => do
    ref.set v
    framedUpdate v

  let modify := fun f => do
    let v := f (← ref.get)
    ref.set v
    framedUpdate v

  pure (dyn, set, modify)⟩

/-! ## Pattern 2: Async Resource

Run IO operations asynchronously and track their state as a Dynamic. -/

/-- Handle for canceling an async operation -/
structure AsyncHandle where
  /-- Cancel the async operation. Idempotent. -/
  cancel : IO Unit
  /-- Check if the operation has been canceled -/
  isCanceled : IO Bool

namespace AsyncHandle

/-- A no-op handle that can't be canceled -/
def noop : AsyncHandle where
  cancel := pure ()
  isCanceled := pure false

end AsyncHandle

/-- Run an async IO action, returning a Dynamic of its state.
    Transitions: loading → ready/error -/
def asyncIO (action : IO a) : SpiderM (Dyn (AsyncState String a)) := ⟨fun env => do
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.loading : AsyncState String a)
  let framedUpdate := fun v => env.withFrame (update v)
  let _ ← IO.asTask (prio := .dedicated) do
    try
      let result ← action
      framedUpdate (AsyncState.ready result)
    catch e =>
      framedUpdate (AsyncState.error (toString e))
  pure dyn⟩

/-- Run an async IO action with typed errors.
    The action returns Except for explicit error handling.

    **Exception handling**: If the action throws an unexpected IO exception (not via Except),
    it is logged to stderr and the state remains `loading`. This prevents app crashes but
    callers waiting on the result will not receive updates. Prefer handling all errors
    explicitly within the action using `Except`. -/
def asyncIOE (action : IO (Except e a)) : SpiderM (Dyn (AsyncState e a)) := ⟨fun env => do
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.loading : AsyncState e a)
  let framedUpdate := fun v => env.withFrame (update v)
  let _ ← IO.asTask (prio := .dedicated) do
    try
      match ← action with
      | .ok result => framedUpdate (AsyncState.ready result)
      | .error err => framedUpdate (AsyncState.error err)
    catch ex =>
      -- Unexpected exception - log it, state stays loading
      IO.eprintln s!"asyncIOE: Unexpected exception: {ex}"
  pure dyn⟩

/-- Run an async IO action with explicit cancellation handle.

    **Cancellation semantics**: Calling `cancel` on the handle prevents state updates
    but does NOT abort the underlying IO operation. The task continues to run in the
    background; only the result is discarded. This is a "soft" cancellation that avoids
    race conditions in state management.

    For true task abortion, consider cooperative cancellation within the action itself. -/
def asyncIOCancelable (action : IO a) : SpiderM (Dyn (AsyncState String a) × AsyncHandle) := ⟨fun env => do
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.loading : AsyncState String a)
  let framedUpdate := fun v => env.withFrame (update v)
  let canceledRef ← IO.mkRef false

  let _ ← IO.asTask (prio := .dedicated) do
    try
      let result ← action
      let canceled ← canceledRef.get
      if !canceled then
        framedUpdate (AsyncState.ready result)
    catch e =>
      let canceled ← canceledRef.get
      if !canceled then
        framedUpdate (AsyncState.error (toString e))

  let handle : AsyncHandle := {
    cancel := canceledRef.set true
    isCanceled := canceledRef.get
  }

  pure (dyn, handle)⟩

/-- Event-driven async: run an async action for each event occurrence.
    Each new event "cancels" the previous pending operation (if any).
    Uses generation counters for staleness detection.

    **Cancellation semantics**: "Cancellation" here means the result is ignored via
    generation check. Previous tasks are NOT aborted; they continue running but their
    results are discarded. This prevents stale results from overwriting newer state.

    For resource-intensive operations, consider adding cooperative cancellation within
    the action to avoid wasted work. -/
def asyncOnEvent (event : Evt a) (action : a → IO b) : SpiderM (Dyn (AsyncState String b)) := ⟨fun env => do
  let generationRef ← IO.mkRef (0 : Nat)
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.pending : AsyncState String b)
  let framedUpdate := fun v => env.withFrame (update v)

  let unsub ← Reactive.Event.subscribe event fun value => do
    -- Increment generation, canceling any previous operation
    let generation ← generationRef.modifyGet fun g => (g + 1, g + 1)
    update AsyncState.loading

    let _ ← IO.asTask (prio := .dedicated) do
      try
        let result ← action value
        let currentGen ← generationRef.get
        if currentGen == generation then
          framedUpdate (AsyncState.ready result)
      catch e =>
        let currentGen ← generationRef.get
        if currentGen == generation then
          framedUpdate (AsyncState.error (toString e))

  env.currentScope.register unsub
  pure dyn⟩

/-! ## Pattern 3: Async with Retry

Retry logic wrapped around async operations with exponential backoff. -/

/-- Internal: attempt loop for asyncWithRetry -/
private partial def asyncWithRetryLoop (framedUpdate : AsyncState (RetryState × String) a → IO Unit)
    (config : RetryConfig) (action : IO a) (state : RetryState) : IO Unit := do
  try
    let result ← action
    framedUpdate (AsyncState.ready result)
  catch e =>
    let errMsg := toString e
    let now ← IO.monoMsNow
    -- Check exhaustion with current state BEFORE incrementing
    if state.retryCount >= config.maxRetries then
      let finalState := { state with lastAttemptTime := now, lastError := some errMsg }
      framedUpdate (AsyncState.error (finalState, errMsg))
    else
      -- Increment retry count and retry
      let newState : RetryState := {
        retryCount := state.retryCount + 1
        lastAttemptTime := now
        lastError := some errMsg
      }
      let delayMs := state.backoffDelayMs config
      IO.sleep (UInt32.ofNat delayMs)
      asyncWithRetryLoop framedUpdate config action newState

/-- Run an async IO action with retry logic.
    On failure, retries with exponential backoff up to maxRetries.
    Error type includes RetryState for debugging. -/
def asyncWithRetry (config : RetryConfig) (action : IO a)
    : SpiderM (Dyn (AsyncState (RetryState × String) a)) := ⟨fun env => do
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.loading : AsyncState (RetryState × String) a)
  let framedUpdate := fun v => env.withFrame (update v)
  let _ ← IO.asTask (prio := .dedicated) do
    asyncWithRetryLoop framedUpdate config action RetryState.initial
  pure dyn⟩

/-- Internal: attempt loop for asyncOnEventWithRetry -/
private partial def asyncOnEventWithRetryLoop
    (framedUpdate : AsyncState (RetryState × String) b → IO Unit)
    (config : RetryConfig) (generationRef : IO.Ref Nat) (action : a → IO b)
    (value : a) (generation : Nat) (state : RetryState) : IO Unit := do
  -- Check for cancellation before each attempt
  let currentGen ← generationRef.get
  if currentGen != generation then
    return  -- Canceled by newer event

  try
    let result ← action value
    let currentGen ← generationRef.get
    if currentGen == generation then
      framedUpdate (AsyncState.ready result)
  catch e =>
    let currentGen ← generationRef.get
    if currentGen != generation then
      return  -- Canceled during execution

    let errMsg := toString e
    let now ← IO.monoMsNow
    -- Check exhaustion with current state BEFORE incrementing
    if state.retryCount >= config.maxRetries then
      let finalState := { state with lastAttemptTime := now, lastError := some errMsg }
      framedUpdate (AsyncState.error (finalState, errMsg))
    else
      -- Increment retry count and retry
      let newState : RetryState := {
        retryCount := state.retryCount + 1
        lastAttemptTime := now
        lastError := some errMsg
      }
      let delayMs := state.backoffDelayMs config
      IO.sleep (UInt32.ofNat delayMs)
      asyncOnEventWithRetryLoop framedUpdate config generationRef action value generation newState

/-- Event-driven async with retry: runs action for each event with retry logic.
    Each new event "cancels" any pending operation (including retries).

    **Cancellation semantics**: Same as `asyncOnEvent` - previous tasks are NOT aborted,
    only their results are ignored. Retry delays still run but generation checks prevent
    stale updates. -/
def asyncOnEventWithRetry (config : RetryConfig) (event : Evt a) (action : a → IO b)
    : SpiderM (Dyn (AsyncState (RetryState × String) b)) := ⟨fun env => do
  let generationRef ← IO.mkRef (0 : Nat)
  let (dyn, update) ← createDynamic env.timelineCtx (AsyncState.pending : AsyncState (RetryState × String) b)
  let framedUpdate := fun v => env.withFrame (update v)

  let unsub ← Reactive.Event.subscribe event fun value => do
    -- Increment generation, canceling any previous operation
    let generation ← generationRef.modifyGet fun g => (g + 1, g + 1)
    update AsyncState.loading

    let _ ← IO.asTask (prio := .dedicated) do
      asyncOnEventWithRetryLoop framedUpdate config generationRef action value generation RetryState.initial

  env.currentScope.register unsub
  pure dyn⟩

/-! ## Pattern 4: Pool-Level Retry Scheduling

Combinator that wraps pool output to add retry logic with exponential backoff. -/

/-- State for tracking pending retry operations per job -/
private structure RetryJobState where
  /-- Current retry count -/
  retryCount : Nat := 0
  /-- Task handle for delayed retry (for cancellation) -/
  retryTask : Option (Task (Except IO.Error Unit)) := none

/-- Output from the retry scheduler -/
structure RetryScheduler (jobId : Type) where
  /-- Fires when a retry is scheduled: (jobId, delayMs) -/
  retryScheduled : Evt (jobId × Nat)
  /-- Fires when retries are exhausted: (jobId, lastError) -/
  exhausted : Evt (jobId × String)
  /-- Cancel any pending retry for a job -/
  cancelRetry : jobId → IO Unit

/-- Add retry scheduling to a pool output.

    When the pool's `errored` event fires, this combinator:
    1. Checks if `shouldRetry` returns true for the error
    2. Schedules a delayed resubmit with exponential backoff
    3. Fires `retryScheduled` when scheduling, `exhausted` when giving up

    **Cancel behavior**: Calling `cancelRetry` cancels any pending retry for that job.
    New errors for the same job ID reset the retry state. -/
def withRetryScheduling [BEq jobId] [Hashable jobId]
    (config : RetryConfig)
    (shouldRetry : jobId × String → Bool)
    (resubmitJob : jobId → job)
    (poolOutput : PoolOutput jobId job result)
    (commandTrigger : PoolCommand jobId job → IO Unit)
    : SpiderM (RetryScheduler jobId) := ⟨fun env => do
  let (retryScheduledEvt, fireRetryScheduled) ← Event.newTrigger env.timelineCtx
  let (exhaustedEvt, fireExhausted) ← Event.newTrigger env.timelineCtx

  -- Track retry state per job
  let stateRef ← IO.mkRef ({} : HashMap jobId RetryJobState)

  let cancelRetryImpl := fun (id : jobId) => do
    let state ← stateRef.get
    match state[id]? with
    | some _ =>
      -- We can't actually cancel the Task, but we can remove the state
      -- so when the retry fires, it will be ignored
      stateRef.modify (·.erase id)
    | none => pure ()

  -- Subscribe to errors and schedule retries
  let unsub ← Reactive.Event.subscribe poolOutput.errored fun (id, errMsg) => do
    if shouldRetry (id, errMsg) then
      let state ← stateRef.get
      let currentRetry := (state[id]?.map (·.retryCount)).getD 0

      if currentRetry >= config.maxRetries then
        -- Exhausted - fire event and clear state
        stateRef.modify (·.erase id)
        env.withFrame (fireExhausted (id, errMsg))
      else
        -- Schedule retry
        let retryState : RetryState := { retryCount := currentRetry }
        let delayMs := retryState.backoffDelayMs config

        -- Spawn delayed retry task
        let retryTask ← IO.asTask (prio := .default) do
          IO.sleep (UInt32.ofNat delayMs)
          -- Check if this retry is still valid (not cancelled)
          let state ← stateRef.get
          match state[id]? with
          | some jobState =>
            if jobState.retryCount == currentRetry then
              -- Still valid, increment retry count and resubmit
              stateRef.modify (·.insert id { retryCount := currentRetry + 1, retryTask := none })
              let job := resubmitJob id
              commandTrigger (.resubmit id job 0)
          | none => pure ()  -- Cancelled

        -- Store state with task handle
        stateRef.modify (·.insert id { retryCount := currentRetry, retryTask := some retryTask })

        -- Fire scheduled event
        env.withFrame (fireRetryScheduled (id, delayMs))
    else
      -- Not retryable - just clear any existing state
      stateRef.modify (·.erase id)

  -- Clear retry state on successful completion or explicit cancellation
  let unsubComplete ← Reactive.Event.subscribe poolOutput.completed fun (id, _, _) =>
    stateRef.modify (·.erase id)
  let unsubCancel ← Reactive.Event.subscribe poolOutput.cancelled fun id =>
    stateRef.modify (·.erase id)

  env.currentScope.register unsub
  env.currentScope.register unsubComplete
  env.currentScope.register unsubCancel

  return {
    retryScheduled := retryScheduledEvt
    exhausted := exhaustedEvt
    cancelRetry := cancelRetryImpl
  }⟩

/-! ## Pattern 5: Two-Tier Cache Combinator

Wraps a processor to check fast/slow caches before processing. -/

/-- Configuration for two-tier caching -/
structure CacheConfig (key result cached : Type) where
  /-- Try to get from fast cache (e.g., memory) -/
  tryFastCache : key → IO (Option cached)
  /-- Try to get from slow cache (e.g., disk) -/
  trySlowCache : key → IO (Option cached)
  /-- Promote from slow cache to fast cache -/
  promoteToFast : key → cached → IO Unit
  /-- Save result to both caches -/
  saveToCache : key → result → cached → IO Unit
  /-- Convert cached value to result -/
  cachedToResult : cached → IO result

/-- Wrap a processor with two-tier caching.

    Cache lookup order:
    1. Fast cache (memory) - returns immediately if hit
    2. Slow cache (disk) - promotes to fast cache on hit
    3. Process job - saves to both caches on completion

    **Error handling**: Cache errors are logged but don't fail the job.
    Processing errors propagate normally. -/
def withTwoTierCache
    (cacheConfig : CacheConfig key result cached)
    (getKey : job → key)
    (processor : job → IO result)
    (onResult : result → IO cached)
    : job → IO result := fun theJob => do
  let key := getKey theJob

  -- Try fast cache
  try
    match ← cacheConfig.tryFastCache key with
    | some cachedVal => return ← cacheConfig.cachedToResult cachedVal
    | none => pure ()
  catch e =>
    IO.eprintln s!"Fast cache error for key: {e}"

  -- Try slow cache
  try
    match ← cacheConfig.trySlowCache key with
    | some cachedVal =>
      -- Promote to fast cache
      try cacheConfig.promoteToFast key cachedVal catch _ => pure ()
      return ← cacheConfig.cachedToResult cachedVal
    | none => pure ()
  catch e =>
    IO.eprintln s!"Slow cache error for key: {e}"

  -- Process and cache
  let result ← processor theJob
  try
    let cached ← onResult result
    cacheConfig.saveToCache key result cached
  catch e =>
    IO.eprintln s!"Cache save error for key: {e}"

  return result

end Reactive.Host
