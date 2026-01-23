/-
  Reactive/Host/Spider/WorkerPool.lean

  FRP-based worker pool for async job processing.
  Jobs can be submitted, cancelled, and resubmitted through event streams.
  Uses generation-based soft cancellation and priority queue ordering.
-/
import Reactive.Host.Spider.Core
import Std.Data.HashMap
import Std.Sync.Channel
import Std.Sync.Mutex

namespace Reactive.Host

open Reactive
open Std (HashMap)

/-- Command for controlling the worker pool -/
inductive PoolCommand (jobId job : Type) where
  | submit (id : jobId) (job : job) (priority : Int)
  | cancel (id : jobId)
  | updatePriority (id : jobId) (newPriority : Int)
  | resubmit (id : jobId) (job : job) (priority : Int)
  deriving Repr

/-- Status of a job in the pool -/
inductive JobStatus where
  | pending    -- In queue, not yet started
  | running    -- Currently being processed
  | completed  -- Finished successfully
  | cancelled  -- Cancelled (either from queue or soft-cancelled while running)
  | error      -- Failed with exception
  deriving Repr, BEq, Inhabited

/-- Priority for a job, with sequence number for FIFO tiebreaking -/
structure JobPriority where
  /-- Higher priority values are processed first -/
  priority : Int
  /-- Sequence number for FIFO ordering within same priority -/
  sequence : Nat
  deriving Repr, BEq, Inhabited

/-- A pending job in the queue -/
private structure PendingJob (jobId job : Type) where
  id : jobId
  priority : JobPriority
  payload : job
  deriving Repr

instance [Inhabited jobId] [Inhabited job] : Inhabited (PendingJob jobId job) where
  default := { id := default, priority := default, payload := default }

/-- Configuration for the worker pool -/
structure WorkerPoolConfig where
  /-- Number of worker threads -/
  workerCount : Nat := 4
  deriving Repr, BEq, Inhabited

/-- Output structure from the FRP worker pool -/
structure PoolOutput (jobId job result : Type) [BEq jobId] [Hashable jobId] where
  /-- Fires when a job completes successfully -/
  completed : Evt (jobId × job × result)
  /-- Fires when a job is successfully cancelled -/
  cancelled : Evt jobId
  /-- Fires when a job fails with an error -/
  errored : Evt (jobId × String)
  /-- Observable state of all jobs -/
  jobStates : Dyn (HashMap jobId JobStatus)
  /-- Number of pending jobs -/
  pendingCount : Dyn Nat
  /-- Number of running jobs -/
  runningCount : Dyn Nat

namespace WorkerPool

/-- Internal state for the priority queue (max-heap by priority) -/
private structure PoolState (jobId job : Type) [BEq jobId] [Hashable jobId] where
  /-- Binary heap of pending jobs -/
  pendingQueue : Array (PendingJob jobId job) := #[]
  /-- Currently running jobs with generation counters -/
  runningJobs : HashMap jobId (job × Nat) := {}
  /-- Job statuses for external observation -/
  statuses : HashMap jobId JobStatus := {}
  /-- Global generation counter per job ID -/
  generations : HashMap jobId Nat := {}
  /-- Next sequence number for FIFO tiebreaking -/
  nextSequence : Nat := 0
  /-- Whether the pool is closed -/
  closed : Bool := false
  /-- Monotonically increasing version for observable update ordering -/
  version : Nat := 0

/-- Check if job a has higher priority than job b -/
@[inline] private def jobHigher (a b : PendingJob jobId job) : Bool :=
  if a.priority.priority == b.priority.priority then
    a.priority.sequence < b.priority.sequence
  else
    a.priority.priority > b.priority.priority

@[inline] private def parentIdx (i : Nat) : Nat := (i - 1) / 2
@[inline] private def leftChildIdx (i : Nat) : Nat := 2 * i + 1
@[inline] private def rightChildIdx (i : Nat) : Nat := 2 * i + 2

@[inline] private def swapJob [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (i j : Nat) : Array (PendingJob jobId job) :=
  let vi := arr[i]!
  let vj := arr[j]!
  (arr.set! i vj).set! j vi

private partial def siftUp [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (i : Nat) : Array (PendingJob jobId job) :=
  if i == 0 then arr
  else
    let pi := parentIdx i
    if pi < arr.size && i < arr.size then
      if jobHigher arr[i]! arr[pi]! then
        siftUp (swapJob arr i pi) pi
      else
        arr
    else
      arr

private partial def siftDown [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (i : Nat) : Array (PendingJob jobId job) :=
  let left := leftChildIdx i
  let right := rightChildIdx i
  let size := arr.size
  let best :=
    let b1 := if left < size && jobHigher arr[left]! arr[i]! then left else i
    if right < size && jobHigher arr[right]! arr[b1]! then right else b1
  if best != i then
    siftDown (swapJob arr i best) best
  else
    arr

private def heapInsert [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (pendingJob : PendingJob jobId job)
    : Array (PendingJob jobId job) :=
  siftUp (arr.push pendingJob) arr.size

private def heapPop? [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job))
    : Option (PendingJob jobId job) × Array (PendingJob jobId job) :=
  if arr.isEmpty then
    (none, arr)
  else if arr.size == 1 then
    (some arr[0]!, #[])
  else
    let root := arr[0]!
    let last := arr[arr.size - 1]!
    let arr' := arr.pop
    let arr'' := siftDown (arr'.set! 0 last) 0
    (some root, arr'')

/-- Remove a job with given ID from the heap (returns updated heap and whether found) -/
private def heapRemove [BEq jobId] [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (id : jobId)
    : Array (PendingJob jobId job) × Bool :=
  match arr.findIdx? (fun j => j.id == id) with
  | none => (arr, false)
  | some idx =>
    if arr.size == 1 then
      (#[], true)
    else if idx == arr.size - 1 then
      (arr.pop, true)
    else
      -- Replace with last element and re-heapify
      let last := arr[arr.size - 1]!
      let arr' := (arr.set! idx last).pop
      -- Could need to sift up or down depending on priorities
      let arr'' := siftDown (siftUp arr' idx) idx
      (arr'', true)

/-- Update priority for a job in the heap (re-heapify) -/
private def heapUpdatePriority [BEq jobId] [Inhabited jobId] [Inhabited job]
    (arr : Array (PendingJob jobId job)) (id : jobId) (newPriority : Int)
    : Array (PendingJob jobId job) :=
  match arr.findIdx? (fun j => j.id == id) with
  | none => arr
  | some idx =>
    let job := arr[idx]!
    let updated := { job with priority := { job.priority with priority := newPriority } }
    let arr' := arr.set! idx updated
    -- Re-heapify by trying both directions
    siftDown (siftUp arr' idx) idx

/-- Internal mutex-protected state for the pool -/
private structure MutexState (jobId job : Type) [BEq jobId] [Hashable jobId] where
  state : PoolState jobId job
  signal : Std.CloseableChannel.Sync Unit

/-- Create a new mutex state -/
private def MutexState.new [BEq jobId] [Hashable jobId] : IO (Std.Mutex (MutexState jobId job)) := do
  let signal ← Std.CloseableChannel.Sync.new
  Std.Mutex.new { state := {}, signal }

/-- Get next generation for a job ID -/
private def nextGeneration [BEq jobId] [Hashable jobId]
    (generations : HashMap jobId Nat) (id : jobId) : Nat × HashMap jobId Nat :=
  let current := generations[id]?.getD 0
  let next := current + 1
  (next, generations.insert id next)

/-- Result of attempting to cancel a job -/
inductive CancelResult (jobId job : Type) [BEq jobId] [Hashable jobId] where
  | cancelled (newState : PoolState jobId job)
  | notFound

/-- Try to cancel a job from state (either pending or running).
    Returns the new state if cancelled, or notFound if job doesn't exist. -/
private def tryCancelJob [BEq jobId] [Hashable jobId] [Inhabited jobId] [Inhabited job]
    (st : PoolState jobId job) (id : jobId) : CancelResult jobId job :=
  let (queue', foundPending) := heapRemove st.pendingQueue id
  if foundPending then
    let (_, gens') := nextGeneration st.generations id
    let statuses' := st.statuses.insert id JobStatus.cancelled
    .cancelled { st with
      pendingQueue := queue'
      statuses := statuses'
      generations := gens'
      version := st.version + 1
    }
  else if st.runningJobs.contains id then
    let (_, gens') := nextGeneration st.generations id
    let running' := st.runningJobs.erase id
    let statuses' := st.statuses.insert id JobStatus.cancelled
    .cancelled { st with
      runningJobs := running'
      statuses := statuses'
      generations := gens'
      version := st.version + 1
    }
  else
    .notFound

/-- Result of attempting to submit a job -/
inductive SubmitResult (jobId job : Type) [BEq jobId] [Hashable jobId] where
  | success (newState : PoolState jobId job)
  | duplicate
  | poolClosed

/-- Try to submit a job to state.
    Returns the new state if successful, or an error indicator. -/
private def trySubmitJob [BEq jobId] [Hashable jobId] [Inhabited jobId] [Inhabited job]
    (st : PoolState jobId job) (id : jobId) (theJob : job) (priority : Int)
    (checkDuplicate : Bool := true) : SubmitResult jobId job :=
  if st.closed then
    .poolClosed
  else if checkDuplicate && (st.pendingQueue.any (·.id == id) || st.runningJobs.contains id) then
    .duplicate
  else
    let seq := st.nextSequence
    let pendingJob : PendingJob jobId job := {
      id := id
      priority := { priority := priority, sequence := seq }
      payload := theJob
    }
    let queue' := heapInsert st.pendingQueue pendingJob
    let statuses' := st.statuses.insert id JobStatus.pending
    .success { st with
      pendingQueue := queue'
      statuses := statuses'
      nextSequence := seq + 1
      version := st.version + 1
    }

/-- Worker loop with versioned observable updates.
    Uses monotonic version numbers to ensure only the latest state is published,
    preventing stale snapshot races when multiple threads update concurrently. -/
private partial def workerLoopImplFixed [BEq jobId] [Hashable jobId] [Inhabited jobId] [Inhabited job]
    (mutexState : Std.Mutex (MutexState jobId job))
    (env : SpiderEnv)
    (lastPublishedVersion : Std.Mutex Nat)
    (updateAllInFrame : PoolState jobId job → Nat → IO Unit → IO Unit)
    (process : job → IO result)
    (fireCompleted : (jobId × job × result) → IO Unit)
    (fireErrored : (jobId × String) → IO Unit)
    (fireCancelled : jobId → IO Unit)
    : IO Unit := do
  -- Wait for signal
  let signal? ← mutexState.atomically do
    let ms ← get
    return ms.signal
  let sig? ← signal?.recv
  match sig? with
  | none => return ()  -- Pool closed
  | some () =>
    -- Try to claim a job - atomically update state with incremented version
    let claimed? ← mutexState.atomically do
      let ms ← get
      let st := ms.state
      if st.closed then return none
      let (theJob?, queue') := heapPop? st.pendingQueue
      match theJob? with
      | none => return none
      | some pendingJob =>
        let gen := st.generations[pendingJob.id]?.getD 0
        let running' := st.runningJobs.insert pendingJob.id (pendingJob.payload, gen)
        let statuses' := st.statuses.insert pendingJob.id JobStatus.running
        let newVersion := st.version + 1
        let st' := { st with
          pendingQueue := queue'
          runningJobs := running'
          statuses := statuses'
          version := newVersion
        }
        modify fun ms => { ms with state := st' }
        return some (pendingJob.id, pendingJob.payload, gen, st', newVersion)

    match claimed? with
    | none =>
      workerLoopImplFixed mutexState env lastPublishedVersion updateAllInFrame process fireCompleted fireErrored fireCancelled
    | some (id, theJob, generation, stateAfterClaim, stateVersion) =>
      -- Update observables with version check to skip if stale
      updateAllInFrame stateAfterClaim stateVersion (pure ())

      -- Process the job
      try
        let theResult ← process theJob
        -- Atomically check generation, update state with new version
        let shouldFire ← mutexState.atomically do
          let ms ← get
          let gen := ms.state.generations[id]?.getD 0
          if gen == generation then
            let running' := ms.state.runningJobs.erase id
            let statuses' := ms.state.statuses.insert id JobStatus.completed
            let newVersion := ms.state.version + 1
            let st' := { ms.state with runningJobs := running', statuses := statuses', version := newVersion }
            modify fun ms => { ms with state := st' }
            return some (st', newVersion)
          else
            return none

        match shouldFire with
        | some (st, ver) => updateAllInFrame st ver (fireCompleted (id, theJob, theResult))
        | none => pure ()
      catch e =>
        -- Same pattern for error case
        let shouldFire ← mutexState.atomically do
          let ms ← get
          let gen := ms.state.generations[id]?.getD 0
          if gen == generation then
            let running' := ms.state.runningJobs.erase id
            let statuses' := ms.state.statuses.insert id JobStatus.error
            let newVersion := ms.state.version + 1
            let st' := { ms.state with runningJobs := running', statuses := statuses', version := newVersion }
            modify fun ms => { ms with state := st' }
            return some (st', newVersion)
          else
            return none

        match shouldFire with
        | some (st, ver) => updateAllInFrame st ver (fireErrored (id, toString e))
        | none => pure ()

      workerLoopImplFixed mutexState env lastPublishedVersion updateAllInFrame process fireCompleted fireErrored fireCancelled

/-- Shutdown handle for graceful pool termination -/
structure PoolHandle where
  /-- Gracefully shutdown the pool -/
  shutdown : IO Unit

/-- Create an FRP-based worker pool with shutdown handle.
    Same as `fromCommands` but also returns a handle to gracefully shutdown the pool. -/
def fromCommandsWithShutdown [BEq jobId] [Hashable jobId] [Inhabited jobId] [Inhabited job]
    (config : WorkerPoolConfig)
    (process : job → IO result)
    (commands : Evt (PoolCommand jobId job))
    : SpiderM (PoolOutput jobId job result × PoolHandle) := ⟨fun env => do
  -- Create trigger events for outputs
  let (completedEvt, fireCompleted) ← Event.newTrigger env.timelineCtx
  let (cancelledEvt, fireCancelled) ← Event.newTrigger env.timelineCtx
  let (erroredEvt, fireErrored) ← Event.newTrigger env.timelineCtx

  -- Create dynamics for observable state
  let (jobStatesDyn, updateJobStates) ← createDynamic env.timelineCtx ({} : HashMap jobId JobStatus)
  let (pendingCountDyn, updatePendingCount) ← createDynamic env.timelineCtx (0 : Nat)
  let (runningCountDyn, updateRunningCount) ← createDynamic env.timelineCtx (0 : Nat)

  -- Internal mutex-protected state
  let mutexState ← MutexState.new (jobId := jobId) (job := job)

  -- Track the last published version to prevent stale state from overwriting newer state
  -- Uses a mutex for atomic compare-and-swap semantics
  let lastPublishedVersion ← Std.Mutex.new (0 : Nat)

  -- Frame update helper with correct lock order and no event drops:
  -- 1. Enter frame FIRST (consistent lock order - frame lock before version mutex)
  -- 2. Always fire events (they represent real occurrences, never skip)
  -- 3. Only update observables if this is the latest version (prevents stale state overwrite)
  let updateAllInFrame := fun (st : PoolState jobId job) (stateVersion : Nat) (fireAction : IO Unit) =>
    env.withFrame do
      -- Always fire events - they represent things that happened, never drop
      fireAction

      -- Update observables only if this is the latest version (prevents stale overwrites)
      let shouldUpdateState ← lastPublishedVersion.atomically do
        let lastVer ← get
        if stateVersion > lastVer then
          set stateVersion
          return true
        else
          return false

      if shouldUpdateState then
        updateJobStates st.statuses
        updatePendingCount st.pendingQueue.size
        updateRunningCount st.runningJobs.size

  -- Helper to update observables only (no event firing)
  let updateObservablesInFrame := fun (st : PoolState jobId job) (ver : Nat) =>
    updateAllInFrame st ver (pure ())

  -- Worker loop using versioned updates
  let workerLoop : IO Unit := workerLoopImplFixed mutexState env lastPublishedVersion updateAllInFrame process fireCompleted fireErrored fireCancelled

  -- Spawn workers
  for _ in [0:config.workerCount] do
    let _ ← IO.asTask (prio := .dedicated) workerLoop

  -- Helper to signal a worker (ignores errors from closed channel)
  let signalWorker := fun (sig : Std.CloseableChannel.Sync Unit) => do
    try let _ ← sig.send () catch _ => pure ()

  -- Process commands with versioned updates
  let processCommand := fun (cmd : PoolCommand jobId job) => do
    match cmd with
    | .submit id theJob priority =>
      let (submitResult, sig) ← mutexState.atomically do
        let ms ← get
        let result := trySubmitJob ms.state id theJob priority
        match result with
        | .success st' =>
          modify fun ms => { ms with state := st' }
          return (result, ms.signal)
        | _ => return (result, ms.signal)

      match submitResult with
      | .poolClosed => pure ()
      | .duplicate => env.withFrame (fireErrored (id, "duplicate job ID"))
      | .success st =>
        updateObservablesInFrame st st.version
        signalWorker sig

    | .cancel id =>
      let cancelResult ← mutexState.atomically do
        let ms ← get
        let result := tryCancelJob ms.state id
        match result with
        | .cancelled st' =>
          modify fun ms => { ms with state := st' }
          return result
        | .notFound => return result

      match cancelResult with
      | .cancelled st => updateAllInFrame st st.version (fireCancelled id)
      | .notFound => pure ()

    | .updatePriority id newPriority =>
      mutexState.atomically do
        let ms ← get
        let queue' := heapUpdatePriority ms.state.pendingQueue id newPriority
        modify fun ms => { ms with state := { ms.state with pendingQueue := queue' } }

    | .resubmit id theJob priority =>
      -- Cancel any existing job first
      let cancelResult ← mutexState.atomically do
        let ms ← get
        let result := tryCancelJob ms.state id
        match result with
        | .cancelled st' =>
          modify fun ms => { ms with state := st' }
          return result
        | .notFound => return result

      match cancelResult with
      | .cancelled st => updateAllInFrame st st.version (fireCancelled id)
      | .notFound => pure ()

      -- Now submit fresh (no duplicate check since we just cancelled)
      let (submitResult, sig) ← mutexState.atomically do
        let ms ← get
        let result := trySubmitJob ms.state id theJob priority (checkDuplicate := false)
        match result with
        | .success st' =>
          modify fun ms => { ms with state := st' }
          return (result, ms.signal)
        | _ => return (result, ms.signal)

      match submitResult with
      | .poolClosed => pure ()
      | .duplicate => pure ()  -- Can't happen with checkDuplicate=false
      | .success st =>
        updateObservablesInFrame st st.version
        signalWorker sig

  let unsub ← Reactive.Event.subscribe commands processCommand
  env.currentScope.register unsub

  -- Shutdown handle that cancels all pending jobs with proper ordering
  let shutdownHandle : PoolHandle := {
    shutdown := do
      -- Atomically: mark closed, get pending job IDs, update their statuses to cancelled with version
      let (sigChan, pendingIds, finalState, finalVersion) ← mutexState.atomically do
        let ms ← get
        let pendingIds := ms.state.pendingQueue.map (·.id)
        -- Update all pending statuses to cancelled with incremented version
        let statuses' := pendingIds.foldl (fun acc id => acc.insert id JobStatus.cancelled) ms.state.statuses
        let newVersion := ms.state.version + 1
        let st' := { ms.state with
          closed := true
          pendingQueue := #[]  -- Clear queue
          statuses := statuses'
          version := newVersion
        }
        modify fun ms => { ms with state := st' }
        return (ms.signal, pendingIds, st', newVersion)

      -- Use correct lock order (frame first) and never drop events
      env.withFrame do
        -- Always fire events first - they represent real cancellations
        for id in pendingIds do
          fireCancelled id

        -- Update observables only if this is the latest version
        let shouldUpdateState ← lastPublishedVersion.atomically do
          let lastVer ← get
          if finalVersion > lastVer then
            set finalVersion
            return true
          else
            return false

        if shouldUpdateState then
          updateJobStates finalState.statuses
          updatePendingCount finalState.pendingQueue.size
          updateRunningCount finalState.runningJobs.size

      try
        let _ ← sigChan.close
      catch _ => pure ()
  }

  return ({
    completed := completedEvt
    cancelled := cancelledEvt
    errored := erroredEvt
    jobStates := jobStatesDyn
    pendingCount := pendingCountDyn
    runningCount := runningCountDyn
  }, shutdownHandle)⟩

/-- Create an FRP-based worker pool from a command stream.

    Jobs are submitted, cancelled, and managed through the `commands` event stream.
    Results are exposed through the returned `PoolOutput` structure.

    **Cancellation semantics**: Running jobs use soft cancellation via generation counters.
    The underlying IO operation continues but its result is discarded. -/
def fromCommands [BEq jobId] [Hashable jobId] [Inhabited jobId] [Inhabited job]
    (config : WorkerPoolConfig)
    (process : job → IO result)
    (commands : Evt (PoolCommand jobId job))
    : SpiderM (PoolOutput jobId job result) := do
  let (output, _) ← fromCommandsWithShutdown config process commands
  return output

end WorkerPool

end Reactive.Host
