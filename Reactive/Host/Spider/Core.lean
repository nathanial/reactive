/-
  Reactive/Host/Spider/Core.lean

  Core types and monad for the Spider FRP runtime.
-/
import Lean
import Reactive.Core
import Reactive.Class
import Reactive.Combinators
import Chronos
import Std.Data.HashMap
import Std.Sync.RecursiveMutex

namespace Reactive.Host

/-- The Spider timeline marker type.
    Spider is an IO-based push propagation runtime. -/
structure Spider where
  private mk ::

instance : Timeline Spider where

/-! ## Type Abbreviations

Short aliases for Spider-parameterized FRP types.
After `open Reactive.Host`, use `Dyn a`, `Evt a`, `Beh a` instead of
`Dynamic Spider a`, `Event Spider a`, `Behavior Spider a`. -/

/-- Short alias for `Dynamic Spider a` -/
abbrev Dyn := Dynamic Spider

/-- Short alias for `Event Spider a` -/
abbrev Evt := Event Spider

/-- Short alias for `Behavior Spider a` -/
abbrev Beh := Behavior Spider

/-- Error handler for subscriber callback exceptions.
    Receives the error that occurred during event propagation.
    Return `true` to continue processing remaining subscribers,
    `false` to stop propagation (re-raises the error). -/
abbrev PropagationErrorHandler := IO.Error → IO Bool

/-- Default error handler: logs error to stderr and continues propagation -/
def defaultErrorHandler : PropagationErrorHandler := fun err => do
  IO.eprintln s!"[Reactive] Error in subscriber callback: {err}"
  pure true

/-- Strict error handler: re-raises the first error, stopping propagation -/
def strictErrorHandler : PropagationErrorHandler := fun _ => pure false

/-- Maximum construction depth before throwing an error (detects infinite loops) -/
def maxConstructionDepth : Nat := 10000

/-- Maximum propagation depth before throwing an error (detects infinite event loops) -/
def maxPropagationDepth : Nat := 10000

structure SpiderEnv where
  /-- Timeline context for type-safe event creation -/
  timelineCtx : TimelineCtx Spider
  /-- Actions to run after the network is fully built -/
  postBuildActions : IO.Ref (Array (IO Unit))
  /-- The post-build event (fires once after construction) -/
  postBuildEvent : Event Spider Unit
  /-- Trigger for the post-build event -/
  postBuildTrigger : Unit → IO Unit
  /-- Propagation queue for frame-based event handling -/
  propagationQueue : PropagationQueue
  /-- Current subscription scope for automatic cleanup -/
  currentScope : SubscriptionScope
  /-- Error handler for subscriber callback exceptions -/
  errorHandler : IO.Ref PropagationErrorHandler
  /-- Construction depth counter for infinite loop detection -/
  constructionDepth : IO.Ref Nat
  /-- Propagation depth counter for infinite event loop detection -/
  propagationDepth : IO.Ref Nat
  /-- Recursive mutex to serialize frame execution across threads.
      Uses BaseRecursiveMutex to allow same-thread reentrant locking without deadlock. -/
  frameMutex : Std.BaseRecursiveMutex

namespace SpiderEnv

/-- Create a new Spider environment -/
def new (errorHandler : PropagationErrorHandler := defaultErrorHandler) : IO SpiderEnv := do
  let timelineCtx ← TimelineCtx.new
  let postBuildActions ← IO.mkRef #[]
  let (postBuildEvent, postBuildTrigger) ← Event.newTriggerWithId ⟨0⟩
  let propagationQueue ← PropagationQueue.new
  let currentScope ← SubscriptionScope.new
  let errorHandlerRef ← IO.mkRef errorHandler
  let constructionDepth ← IO.mkRef 0
  let propagationDepth ← IO.mkRef 0
  let frameMutex ← Std.BaseRecursiveMutex.new
  -- Set global propagation context for frame-based firing
  setPropagationContext propagationQueue
  pure { timelineCtx, postBuildActions, postBuildEvent, postBuildTrigger, propagationQueue, currentScope, errorHandler := errorHandlerRef, constructionDepth, propagationDepth, frameMutex }

/-- Increment construction depth and throw if exceeded. Returns the new depth. -/
def incrementDepth (env : SpiderEnv) (operation : String) : IO Nat := do
  let depth ← env.constructionDepth.modifyGet fun d => (d + 1, d + 1)
  if depth > maxConstructionDepth then
    throw <| IO.userError s!"[Reactive] Infinite loop detected during FRP network construction (depth {depth} exceeded {maxConstructionDepth}). Last operation: {operation}"
  pure depth

/-- Decrement construction depth -/
def decrementDepth (env : SpiderEnv) : IO Unit := do
  env.constructionDepth.modify (· - 1)

/-- Process all pending fires in height order until queue is empty.
    When current frame is empty, processes nextFramePending in a new sub-frame.
    Errors in subscriber callbacks are handled by the configured error handler.
    Throws if total events processed exceeds maxPropagationDepth (detects infinite event loops). -/
partial def drainQueue (env : SpiderEnv) : IO Unit := do
  -- Cache error handler outside hot loop
  let errorHandler ← env.errorHandler.get
  loop errorHandler 0
where
  loop (errorHandler : PropagationErrorHandler) (count : Nat) : IO Unit := do
    let pendingOpt ← env.propagationQueue.popMin?
    match pendingOpt with
    | none =>
      -- Current frame empty, check for next-frame events (from delayFrame)
      let started ← env.propagationQueue.startNextFrame
      if started then
        loop errorHandler count
      else
        return ()  -- All done
    | some pending =>
      -- Check total events processed for infinite loop detection
      let count' := count + 1
      if count' > maxPropagationDepth then
        throw <| IO.userError s!"[Reactive] Infinite loop detected during event propagation ({count'} events processed, exceeded {maxPropagationDepth}). This usually means an event subscriber is triggering events recursively."
      -- Execute the fire action with error handling
      try
        pending.fire
      catch e =>
        let shouldContinue ← errorHandler e
        if !shouldContinue then
          throw e
      loop errorHandler count'

/-- Execute an action within a propagation frame.
    If already in a frame, just runs the action (it will enqueue).
    If not in a frame, starts a new frame, runs action, then drains queue.

    Thread-safety: Frame execution is serialized via a recursive mutex to prevent
    concurrent async completions from interleaving frame operations. The recursive
    mutex allows same-thread reentrant locking without deadlock, while blocking
    other threads until the frame completes. -/
def withFrame (env : SpiderEnv) (action : IO Unit) : IO Unit := do
  -- Acquire recursive mutex - same thread can lock multiple times without blocking,
  -- but other threads will wait until we fully release
  env.frameMutex.lock
  let inFrame ← env.propagationQueue.isInFrame
  if inFrame then
    -- Already in a frame (reentrant call from same thread), just run action
    try
      action
    finally
      env.frameMutex.unlock
  else
    -- Starting a new frame
    env.propagationDepth.set 0
    env.propagationQueue.setInFrame true
    try
      action
      env.drainQueue
    finally
      env.propagationQueue.setInFrame false
      env.frameMutex.unlock

end SpiderEnv

/-- Internal helper to create a Dynamic with an update function.
    This is private to prevent downstream consumers from creating Dynamics
    with exposed setters, which can lead to anti-patterns like subscribe/sample/set. -/
def createDynamic (ctx : TimelineCtx Spider) (initial : a) : IO (Dynamic Spider a × (a → IO Unit)) := do
  let valueRef ← IO.mkRef initial
  let nodeId ← ctx.freshNodeId
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let update := fun newValue => do
    valueRef.set newValue
    trigger newValue
  pure (Dynamic.mk valueRef changeEvent trigger, update)

/-- The Spider monad for building reactive networks.

    SpiderM provides:
    - Node ID generation for the reactive graph
    - Post-build action registration
    - All FRP typeclass instances -/
structure SpiderM (a : Type) where
  run : SpiderEnv → IO a

namespace SpiderM

/-- Run a SpiderM action with a fresh environment.
    Disposes the root scope when done, cleaning up all subscriptions.
    @param errorHandler Optional error handler for subscriber exceptions (default: log and continue) -/
def runFresh (m : SpiderM a) (errorHandler : PropagationErrorHandler := defaultErrorHandler) : IO a := do
  let env ← SpiderEnv.new errorHandler
  let result ← m.run env
  -- Fire post-build event
  env.postBuildTrigger ()
  -- Dispose root scope to clean up all subscriptions
  env.currentScope.dispose
  pure result

/-- Get a fresh node ID -/
def freshNodeId : SpiderM NodeId := ⟨fun env => do
  env.timelineCtx.freshNodeId⟩

/-- Get the timeline context. Useful for calling IO-based functions that require TimelineCtx. -/
def getTimelineCtx : SpiderM (TimelineCtx Spider) :=
  ⟨fun env => pure env.timelineCtx⟩

/-- Register a post-build action -/
def registerPostBuild (action : IO Unit) : SpiderM Unit := ⟨fun env => do
  env.postBuildActions.modify (·.push action)⟩

/-- Get the current error handler -/
def getErrorHandler : SpiderM PropagationErrorHandler :=
  ⟨fun env => env.errorHandler.get⟩

/-- Set the error handler for subscriber callback exceptions.
    - `defaultErrorHandler`: logs errors and continues (default)
    - `strictErrorHandler`: re-raises first error, stopping propagation
    - Custom handler: receives `IO.Error`, returns `Bool` (true = continue) -/
def setErrorHandler (handler : PropagationErrorHandler) : SpiderM Unit :=
  ⟨fun env => env.errorHandler.set handler⟩

instance : Monad SpiderM where
  pure a := ⟨fun _ => pure a⟩
  bind ma f := ⟨fun env => do
    let a ← ma.run env
    (f a).run env⟩

instance : MonadLiftT IO SpiderM where
  monadLift io := ⟨fun _ => io⟩

/-- Explicit ForIn instance for SpiderM to avoid runtime issues with the derived one -/
instance [ForIn IO ρ α] : ForIn SpiderM ρ α where
  forIn x init f := ⟨fun env => do
    ForIn.forIn x init fun a b => do
      let result ← (f a b).run env
      pure result⟩

/-- Lift IO actions into SpiderM. Shorter alias for `liftM (m := IO)`. -/
def liftIO (action : IO α) : SpiderM α := liftM (m := IO) action

/-- Get the current subscription scope -/
def getScope : SpiderM SubscriptionScope :=
  ⟨fun env => pure env.currentScope⟩

/-- Get the current SpiderEnv (for advanced use cases like testing) -/
def getEnv : SpiderM SpiderEnv :=
  ⟨fun env => pure env⟩

/-- Run an action with a new child scope.
    Returns the result and the child scope (for manual disposal if needed). -/
def withScope (action : SpiderM a) : SpiderM (a × SubscriptionScope) :=
  ⟨fun env => do
    let childScope ← env.currentScope.child
    let result ← action.run { env with currentScope := childScope }
    pure (result, childScope)⟩

/-- Run an action with a child scope that is automatically disposed when the action completes.
    Useful for temporary subscriptions in a bounded context. -/
def withAutoDisposeScope (action : SpiderM a) : SpiderM a :=
  ⟨fun env => do
    let childScope ← env.currentScope.child
    let result ← action.run { env with currentScope := childScope }
    childScope.dispose
    pure result⟩

instance : MonadSample Spider SpiderM where
  sample b := ⟨fun _ => b.sample⟩

instance : MonadHold Spider SpiderM where
  hold initial event := ⟨fun env => do
    let _ ← env.incrementDepth "hold"
    -- Create a behavior that holds the latest value
    let valueRef ← IO.mkRef initial
    let unsub ← Reactive.Event.subscribe event fun a => valueRef.set a
    env.currentScope.register unsub
    env.decrementDepth
    pure (Behavior.fromSample valueRef.get)⟩

  holdDyn initial event := ⟨fun env => do
    let _ ← env.incrementDepth "holdDyn"
    let (dyn, update) ← createDynamic env.timelineCtx initial
    let unsub ← Reactive.Event.subscribe event fun a => update a
    env.currentScope.register unsub
    env.decrementDepth
    pure dyn⟩

  foldDyn f initial event := ⟨fun env => do
    let _ ← env.incrementDepth "foldDyn"
    let (dyn, update) ← createDynamic env.timelineCtx initial
    let unsub ← Reactive.Event.subscribe event fun a => do
      let old ← dyn.sample
      update (f a old)
    env.currentScope.register unsub
    env.decrementDepth
    pure dyn⟩

  foldDynM f initial event := ⟨fun env => do
    let _ ← env.incrementDepth "foldDynM"
    -- For monadic fold, we create a dynamic and update it with each event
    let (dyn, update) ← createDynamic env.timelineCtx initial
    let unsub ← Reactive.Event.subscribe event fun a => do
      let old ← dyn.sample
      -- Run the SpiderM action to get the new value
      let newM := f a old
      let new ← newM.run env
      update new
    env.currentScope.register unsub
    env.decrementDepth
    pure dyn⟩

instance : TriggerEvent Spider SpiderM where
  newTriggerEvent := ⟨fun env => do
    let (event, rawTrigger) ← Event.newTrigger env.timelineCtx
    -- Wrap trigger to use frame semantics for glitch-free propagation
    let framedTrigger := fun a => env.withFrame (rawTrigger a)
    pure (event, framedTrigger)⟩

  newEventWithTrigger setup := ⟨fun env => do
    let (event, rawTrigger) ← Event.newTrigger env.timelineCtx
    -- Wrap trigger to use frame semantics
    let framedTrigger := fun a => env.withFrame (rawTrigger a)
    setup framedTrigger
    pure event⟩

instance : PostBuild Spider SpiderM where
  getPostBuild := ⟨fun env => pure env.postBuildEvent⟩

instance : Adjustable Spider SpiderM where
  runWithReplace initial replaceEvent := ⟨fun env => do
    -- Create a child scope for the current computation (will be disposed on replacement)
    let currentChildScope ← IO.mkRef (← env.currentScope.child)

    -- Run the initial computation in its own child scope
    let initialResult ← do
      let childScope ← currentChildScope.get
      let childEnv := { env with currentScope := childScope }
      initial.run childEnv

    -- Create result event for replacement outputs
    let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx

    -- Subscribe to replacement events - when fired, tear down old network and run new
    let unsub ← Reactive.Event.subscribe replaceEvent fun replacementM => do
      -- Dispose the old computation's scope (tears down its subscriptions)
      let oldScope ← currentChildScope.get
      oldScope.dispose
      -- Create a new child scope for the replacement
      let newScope ← env.currentScope.child
      currentChildScope.set newScope
      -- Run replacement in the new scope
      let childEnv := { env with currentScope := newScope }
      let result ← replacementM.run childEnv
      fireResult result
    env.currentScope.register unsub

    pure (initialResult, resultEvent)
  ⟩

/-- Convenience function for runWithReplace with explicit types.
    Direct implementation to avoid universe inference issues.
    Subscription is registered with current scope.

    **Replacement semantics**: When the replacement event fires, the old
    computation's subscriptions are disposed before running the new one.
    This ensures clean teardown of replaced FRP networks. -/
def runWithReplaceM (initial : SpiderM a) (replaceEvent : Event Spider (SpiderM a))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  -- Create a child scope for the current computation (will be disposed on replacement)
  let currentChildScope ← IO.mkRef (← env.currentScope.child)

  -- Run the initial computation in its own child scope
  let initialResult ← do
    let childScope ← currentChildScope.get
    let childEnv := { env with currentScope := childScope }
    initial.run childEnv

  let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx

  -- Subscribe to replacement events - when fired, tear down old network and run new
  let unsub ← Reactive.Event.subscribe replaceEvent fun replacementM => do
    -- Dispose the old computation's scope (tears down its subscriptions)
    let oldScope ← currentChildScope.get
    oldScope.dispose
    -- Create a new child scope for the replacement
    let newScope ← env.currentScope.child
    currentChildScope.set newScope
    -- Run replacement in the new scope
    let childEnv := { env with currentScope := newScope }
    let result ← replacementM.run childEnv
    fireResult result
  env.currentScope.register unsub

  pure (initialResult, resultEvent)
⟩

/-! ## Recursive Binding Combinators

These enable circular dependencies between events and dynamics.
Since Lean 4 is strict (unlike Haskell), we use lazy IO.Ref placeholders
that get filled after network construction completes. -/

/-- Create a self-referential dynamic.

    The function `f` receives a Behavior that will sample from the Dynamic
    being created. This enables circular dependencies where the dynamic's
    value depends on events that filter based on the dynamic's current value.

    Example - counter that stops at maxValue:
    ```
    fixDynM fun counterBehavior => do
      let (clicks, fire) ← newTriggerEvent
      let gated ← Event.filterM (fun _ => do
        let c ← sample counterBehavior
        pure (c < maxValue)) clicks
      foldDyn (fun _ n => n + 1) 0 gated
    ```

    The behavior samples from a lazy ref that gets filled after `f` completes.
    This works because behaviors are only sampled inside event handlers,
    which run after network construction finishes.

    IMPORTANT: The behavior should only be sampled inside event handlers,
    not during network construction. Sampling during construction returns
    the default value. -/
def fixDynM [Inhabited a] (f : Behavior Spider a → SpiderM (Dynamic Spider a))
    : SpiderM (Dynamic Spider a) := ⟨fun env => do
  -- Create ref to hold the real dynamic (initially none)
  let dynRef : IO.Ref (Option (Dynamic Spider a)) ← IO.mkRef none

  -- Create a "lazy" behavior that samples from the ref
  let lazyBehavior := Behavior.fromSample do
    match ← dynRef.get with
    | some d => d.sample
    | none => pure default  -- Before wiring, return default

  -- Run f with the lazy behavior
  let realDyn ← (f lazyBehavior).run env

  -- Store the real dynamic in the ref
  dynRef.set (some realDyn)

  pure realDyn⟩

/-- Create two mutually recursive dynamics.

    Example - toggle and counter that depend on each other:
    ```
    fixDyn2M fun toggleB countB => do
      let (event, fire) ← newTriggerEvent
      -- Toggle flips when count is even
      let toggle ← foldDyn (fun _ b => !b) false =<<
        Event.filterM (fun _ => (· % 2 == 0) <$> sample countB) event
      -- Count increments when toggle is true
      let count ← foldDyn (fun _ n => n + 1) 0 =<<
        Event.filterM (fun _ => sample toggleB) event
      pure (toggle, count)
    ``` -/
def fixDyn2M [Inhabited a] [Inhabited b]
    (f : Behavior Spider a → Behavior Spider b → SpiderM (Dynamic Spider a × Dynamic Spider b))
    : SpiderM (Dynamic Spider a × Dynamic Spider b) := ⟨fun env => do
  let refA : IO.Ref (Option (Dynamic Spider a)) ← IO.mkRef none
  let refB : IO.Ref (Option (Dynamic Spider b)) ← IO.mkRef none

  let lazyA := Behavior.fromSample do
    match ← refA.get with
    | some d => d.sample
    | none => pure default

  let lazyB := Behavior.fromSample do
    match ← refB.get with
    | some d => d.sample
    | none => pure default

  let (dynA, dynB) ← (f lazyA lazyB).run env

  refA.set (some dynA)
  refB.set (some dynB)

  pure (dynA, dynB)⟩

/-- Create a self-referential event.

    Similar to fixDynM but for events. The function receives an IO action
    that will eventually provide access to the event being created.

    The IO action should only be called inside event handlers, not during
    network construction. -/
def fixEventM (f : IO (Event Spider a) → SpiderM (Event Spider a))
    : SpiderM (Event Spider a) := ⟨fun env => do
  let eventRef : IO.Ref (Option (Event Spider a)) ← IO.mkRef none

  let getEvent : IO (Event Spider a) := do
    match ← eventRef.get with
    | some e => pure e
    | none => panic! "fixEventM: event accessed before wiring complete"

  let realEvent ← (f getEvent).run env
  eventRef.set (some realEvent)
  pure realEvent⟩

end SpiderM

end Reactive.Host
