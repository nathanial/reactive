/-
  Reactive/Host/Spider.lean

  Spider: An IO-based push runtime for the Reactive FRP library.
  Named after Reflex's Spider host.
-/
import Lean
import Reactive.Core
import Reactive.Class
import Reactive.Combinators
import Chronos
import Std.Data.HashMap

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
  -- Set global propagation context for frame-based firing
  setPropagationContext propagationQueue
  pure { timelineCtx, postBuildActions, postBuildEvent, postBuildTrigger, propagationQueue, currentScope, errorHandler := errorHandlerRef, constructionDepth, propagationDepth }

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
    If not in a frame, starts a new frame, runs action, then drains queue. -/
def withFrame (env : SpiderEnv) (action : IO Unit) : IO Unit := do
  let inFrame ← env.propagationQueue.isInFrame
  if inFrame then
    -- Already in a frame, just run (fires will enqueue)
    action
  else
    -- Start new frame, reset propagation counter for infinite loop detection
    env.propagationDepth.set 0
    env.propagationQueue.setInFrame true
    action
    env.drainQueue
    env.propagationQueue.setInFrame false

end SpiderEnv

/-- Internal helper to create a Dynamic with an update function.
    This is private to prevent downstream consumers from creating Dynamics
    with exposed setters, which can lead to anti-patterns like subscribe/sample/set. -/
private def createDynamic (ctx : TimelineCtx Spider) (initial : a) : IO (Dynamic Spider a × (a → IO Unit)) := do
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
    -- Run the initial computation
    let initialResult ← initial.run env

    -- Create result event for replacement outputs
    let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx

    -- Subscribe to replacement events - when fired, run the new computation
    let unsub ← Reactive.Event.subscribe replaceEvent fun replacementM => do
      let result ← replacementM.run env
      fireResult result
    env.currentScope.register unsub

    pure (initialResult, resultEvent)
  ⟩

  traverseWithAdjust f inputs := ⟨fun env => do
    -- Run all computations over the input list
    let results ← inputs.mapM fun a => (f a).run env

    -- For a minimal implementation, return a never-firing update event
    -- A full implementation would track per-item replacement events
    let neverEvent ← Event.never env.timelineCtx
    pure (results, neverEvent)
  ⟩

/-- Convenience function for runWithReplace with explicit types.
    Direct implementation to avoid universe inference issues.
    Subscription is registered with current scope. -/
def runWithReplaceM (initial : SpiderM a) (replaceEvent : Event Spider (SpiderM a))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  let initialResult ← initial.run env
  let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx
  let unsub ← Reactive.Event.subscribe replaceEvent fun replacementM => do
    let result ← replacementM.run env
    fireResult result
  env.currentScope.register unsub
  pure (initialResult, resultEvent)
⟩

/-- Convenience function for traverseWithAdjust with explicit types.
    Direct implementation to avoid universe inference issues. -/
def traverseWithAdjustM (f : a → SpiderM b) (inputs : List a)
    : SpiderM (List b × Event Spider (List b)) := ⟨fun env => do
  let results ← inputs.mapM fun x => (f x).run env
  let neverEvent ← Event.never env.timelineCtx
  pure (results, neverEvent)
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

/-! ## Dynamic SpiderM Combinators

These provide ergonomic versions of Dynamic operations that auto-allocate NodeIds
and register subscriptions with the current scope for automatic cleanup.

Note: These use the existing IO-based Dynamic functions and wrap them to track
subscriptions. The subscriptions created internally by Dynamic.map etc. are
registered with the scope via a post-creation subscription to the updated event. -/

namespace Dynamic

/-- Map a function over a Dynamic, auto-allocating NodeId and registering with scope.
    Only fires the change event when the mapped value actually changes. -/
def mapM [BEq b] (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.mapM"
  let nodeId ← env.timelineCtx.freshNodeId
  -- Use existing IO-based map (which creates its own internal subscription)
  let result ← Dynamic.mapWithId f da nodeId
  -- Register a subscription to the source's updated event
  -- This tracks the subscription for cleanup
  let unsub ← Reactive.Event.subscribe da.updated fun _ => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Combine two Dynamics with a function, auto-allocating NodeId and registering with scope.
    Only fires the change event when the combined value actually changes. -/
def zipWithM [BEq c] (f : a → b → c) (da : Dynamic Spider a) (db : Dynamic Spider b)
    : SpiderM (Dynamic Spider c) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.zipWithM"
  let nodeId ← env.timelineCtx.freshNodeId
  -- Use existing IO-based zipWith
  let result ← Dynamic.zipWithId f da db nodeId
  -- Register subscriptions to track both sources
  let unsub1 ← Reactive.Event.subscribe da.updated fun _ => pure ()
  let unsub2 ← Reactive.Event.subscribe db.updated fun _ => pure ()
  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure result⟩

/-- Combine three Dynamics with a function, auto-allocating NodeIds and registering with scope.
    Only fires the change event when the combined value actually changes. -/
def zipWith3M [BEq a] [BEq b] [BEq d] (f : a → b → c → d)
    (da : Dynamic Spider a) (db : Dynamic Spider b) (dc : Dynamic Spider c)
    : SpiderM (Dynamic Spider d) := do
  -- Implemented using zipWithM which already handles scope registration
  let ab ← Dynamic.zipWithM Prod.mk da db
  Dynamic.zipWithM (fun (a, b) c => f a b c) ab dc

/-- Create a constant Dynamic that never changes.
    Uses TimelineCtx for type-safe event creation. -/
def pureM (x : a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  Dynamic.constant env.timelineCtx x⟩

/-- Applicative apply for Dynamics, auto-allocating NodeId and registering with scope. -/
def apM [BEq b] (df : Dynamic Spider (a → b)) (da : Dynamic Spider a)
    : SpiderM (Dynamic Spider b) :=
  Dynamic.zipWithM (fun f a => f a) df da

/-! ### Fluent Chainable Combinators for Dynamic

Extension methods enabling dot-notation chaining:
```lean
dynA.map' f |>.zipWith' g dynB
-- or with bind
dynA.map' f >>= (·.zipWith' g dynB)
``` -/

/-- Map a function over a Dynamic (fluent style).
    Enables: `dynamic.map' f` -/
def map' [BEq b] (da : Dynamic Spider a) (f : a → b) : SpiderM (Dynamic Spider b) :=
  mapM f da

/-- Combine with another Dynamic (fluent style).
    Enables: `dynA.zipWith' f dynB` -/
def zipWith' [BEq c] (da : Dynamic Spider a) (f : a → b → c) (db : Dynamic Spider b)
    : SpiderM (Dynamic Spider c) :=
  zipWithM f da db

/-- Pair with another Dynamic (fluent style).
    Enables: `dynA.zip' dynB` -/
def zip' [BEq a] [BEq b] (da : Dynamic Spider a) (db : Dynamic Spider b) : SpiderM (Dynamic Spider (a × b)) :=
  zipWithM Prod.mk da db

/-- Combine with two other Dynamics (fluent style).
    Enables: `dynA.zipWith3' f dynB dynC` -/
def zipWith3' [BEq a] [BEq b] [BEq d] (da : Dynamic Spider a) (f : a → b → c → d)
    (db : Dynamic Spider b) (dc : Dynamic Spider c) : SpiderM (Dynamic Spider d) :=
  zipWith3M f da db dc

/-- Apply a Dynamic function (fluent style).
    Enables: `dynF.ap' dynA` -/
def ap' [BEq b] (df : Dynamic Spider (a → b)) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) :=
  apM df da

/-- Get an event that fires with (oldValue, newValue) pairs on each change.
    Auto-allocates NodeId and registers subscription with current scope.

    This is useful for detecting changes in a Dynamic's value, e.g., to
    determine when a component gains or loses focus.

    Example:
    ```
    let focusChanges ← Dynamic.changesM focusDynamic
    let gainFocus ← Event.filterM (fun (old, new) => !old && new) focusChanges
    ```
-/
def changesM (d : Dynamic Spider a) : SpiderM (Event Spider (a × a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.changesM"
  let result ← Dynamic.changesId d (← env.timelineCtx.freshNodeId)
  env.decrementDepth
  pure result⟩

/-- Deduplicate a Dynamic's updates.
    Only fires when the value actually changes.
    Subscribes within the current scope for cleanup. -/
def holdUniqDynM [BEq a] (d : Dynamic Spider a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.holdUniqDynM"
  let initial ← d.sample
  let currentRef ← IO.mkRef initial
  let (result, updateResult) ← createDynamic env.timelineCtx initial
  let unsub ← Reactive.Event.subscribe d.updated fun newVal => do
    let current ← currentRef.get
    if newVal != current then
      currentRef.set newVal
      updateResult newVal
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Switch/join a Dynamic of Dynamics into a single Dynamic.
    The result updates when either the outer changes or the current inner changes.
    All subscriptions are registered with the current scope. -/
def switchM (dd : Dynamic Spider (Dynamic Spider a)) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.switchM"
  let initialInner ← dd.sample
  let initialValue ← initialInner.sample
  let (result, updateResult) ← createDynamic env.timelineCtx initialValue
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  let subscribeToInner := fun (inner : Dynamic Spider a) => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe inner.updated fun newValue => updateResult newValue
    currentUnsubRef.set unsub
    let currentValue ← inner.sample
    updateResult currentValue

  let unsubInner ← Reactive.Event.subscribe initialInner.updated fun newValue => updateResult newValue
  currentUnsubRef.set unsubInner

  let unsubOuter ← Reactive.Event.subscribe dd.updated subscribeToInner

  -- Register both the outer subscription and a cleanup for the current inner
  env.currentScope.register unsubOuter
  env.currentScope.register do
    let unsub ← currentUnsubRef.get
    unsub

  env.decrementDepth
  pure result⟩

/-- Switch a Dynamic of Dynamics (fluent style).
    Enables: `dynOfDyn.switch'` -/
def switch' (dd : Dynamic Spider (Dynamic Spider a)) : SpiderM (Dynamic Spider a) :=
  switchM dd

end Dynamic

/-! ## Behavior SpiderM Combinators

These provide ergonomic versions of Behavior operations that register
subscriptions with the current scope for automatic cleanup. -/

namespace Behavior

/-- Create a behavior that holds the most recent value from an event.
    Registers subscription with current scope for automatic cleanup.

    This is a SpiderM alternative to `MonadHold.hold` when you only need
    sampling capability without the `Dynamic`'s update event. -/
def holdM (initial : a) (event : Event Spider a) : SpiderM (Behavior Spider a) := ⟨fun env => do
  let valueRef ← IO.mkRef initial
  let unsub ← Reactive.Event.subscribe event fun a => valueRef.set a
  env.currentScope.register unsub
  pure (Behavior.fromSample valueRef.get)⟩

/-- Create a behavior by folding over event occurrences.
    Registers subscription with current scope for automatic cleanup. -/
def foldBM (f : a → b → b) (initial : b) (event : Event Spider a) : SpiderM (Behavior Spider b) := ⟨fun env => do
  let valueRef ← IO.mkRef initial
  let unsub ← Reactive.Event.subscribe event fun a => do
    let old ← valueRef.get
    valueRef.set (f a old)
  env.currentScope.register unsub
  pure (Behavior.fromSample valueRef.get)⟩

end Behavior

/-! ## Event SpiderM Combinators

These provide ergonomic versions of Event operations that auto-allocate NodeIds
and register subscriptions with the current scope for automatic cleanup. -/

namespace Event

/-- Subscribe to an event within SpiderM, auto-registering with the current scope.
    The subscription is automatically cleaned up when the scope is disposed.

    WARNING: This is protected for internal use by combinators. Application code
    should use pure FRP combinators like `foldDyn`, `holdDyn`, `mapM`, `filterM`,
    `attachWith`, etc. instead of manual subscriptions. Manual subscribe can lead
    to imperative patterns that break FRP semantics. -/
protected def subscribeM (e : Event Spider a) (callback : Subscriber a) : SpiderM (IO Unit) :=
  ⟨fun env => Reactive.Event.subscribeScoped e env.currentScope callback⟩

/-- The event that never fires.
    Useful as a placeholder or identity for merge operations. -/
def neverM : SpiderM (Event Spider a) := ⟨fun env => do
  let ctx := env.timelineCtx
  Event.never ctx⟩

/-- The event that never fires (fluent style).
    Enables: `Event.never'` -/
abbrev never' : SpiderM (Event Spider a) := neverM

/-- Map a function over an Event, auto-allocating NodeId and registering with scope. -/
def mapM (f : a → b) (e : Event Spider a) : SpiderM (Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.mapWithId f e nodeId
  env.decrementDepth
  pure derived⟩

/-- Map a constant addition over an Event, fusing successive add-const maps. -/
def mapAddConstM [HAdd a a a] (c : a) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapAddConstM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.mapAddConstWithId c e nodeId
  env.decrementDepth
  pure derived⟩

/-- Filter an Event by a predicate, auto-allocating NodeId and registering with scope. -/
def filterM (p : a → Bool) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.filterM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    if p a then derived.fire a else pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Filter and map an Event, auto-allocating NodeId and registering with scope. -/
def mapMaybeM (f : a → Option b) (e : Event Spider a) : SpiderM (Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapMaybeM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    match f a with
    | some b => derived.fire b
    | none => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Discard event values, mapping all occurrences to Unit. -/
def voidM (e : Event Spider a) : SpiderM (Event Spider Unit) :=
  mapM (fun _ => ()) e

/-- Map an event to a constant value, ignoring the original values. -/
def mapConstM (b : β) (e : Event Spider α) : SpiderM (Event Spider β) :=
  mapM (fun _ => b) e

/-- Merge two Events, auto-allocating NodeId and registering with scope. -/
def mergeM (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeM"
  let nodeId ← env.timelineCtx.freshNodeId
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height
  let unsub1 ← Reactive.Event.subscribe e1 derived.fire
  let unsub2 ← Reactive.Event.subscribe e2 derived.fire
  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Tag an Event with a Behavior's current value, auto-allocating NodeId and registering with scope. -/
def tagM (beh : Behavior Spider a) (e : Event Spider b) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.tagM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun _ => do
    let v ← beh.sample
    derived.fire v
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Sample a behavior at event occurrence (SpiderM version, alias for tagM). -/
abbrev sampleM := @tagM

/-- Attach a Behavior's value to an Event, auto-allocating NodeId and registering with scope. -/
def attachM (b : Behavior Spider a) (e : Event Spider c) : SpiderM (Event Spider (a × c)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.attachM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (a, c)
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Snapshot a behavior at event occurrence (SpiderM version, alias for attachM). -/
abbrev snapshotM := @attachM

/-- Attach with a combining function, auto-allocating NodeId and registering with scope. -/
def attachWithM (f : a → c → d) (b : Behavior Spider a) (e : Event Spider c)
    : SpiderM (Event Spider d) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.attachWithM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (f a c)
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Gate an Event by a Boolean Behavior, auto-allocating NodeId and registering with scope. -/
def gateM (beh : Behavior Spider Bool) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.gateM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let isOpen ← beh.sample
    if isOpen then derived.fire a else pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Merge a list of Events into a list Event, auto-allocating NodeId and registering with scope. -/
def mergeListM (events : List (Event Spider a)) : SpiderM (Event Spider (List a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeListM"
  let nodeId ← env.timelineCtx.freshNodeId
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)
  -- Use Array for O(1) push instead of List O(n) append
  let bufferRef ← IO.mkRef (#[] : Array a)
  let flushScheduledRef ← IO.mkRef false

  for e in events do
    let unsub ← Reactive.Event.subscribe e fun a => do
      bufferRef.modify (·.push a)
      let alreadyScheduled ← flushScheduledRef.get
      if !alreadyScheduled then
        flushScheduledRef.set true
        let flushAction : IO Unit := do
          flushScheduledRef.set false
          let values ← bufferRef.modifyGet fun vs => (vs.toList, #[])
          if !values.isEmpty then derived.fire values
        match ← getPropagationContext with
        | some queue =>
          if ← queue.isInFrame then
            let pending : PendingFire := ⟨derived.height, nodeId, flushAction⟩
            queue.insert pending
          else flushAction
        | none => flushAction
    env.currentScope.register unsub

  env.decrementDepth
  pure derived⟩

/-- Take the leftmost firing Event from a list, auto-allocating NodeId and registering with scope. -/
def leftmostM (events : List (Event Spider a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.leftmostM"
  let nodeId ← env.timelineCtx.freshNodeId
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)
  for e in events do
    let unsub ← Reactive.Event.subscribe e derived.fire
    env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Fan out an Event of HashMaps into per-key Events, using a single subscription.
    The fan-out subscription is registered with the current scope. -/
def fanM [BEq k] [Hashable k] (e : Event Spider (Std.HashMap k v))
    : SpiderM (Event.Fan Spider k v) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.fanM"
  let selectedRef ← IO.mkRef (∅ : Std.HashMap k (Event Spider v))
  let ctx := env.timelineCtx
  let unsub ← Reactive.Event.subscribe e fun values => do
    let selected ← selectedRef.get
    for (k, v) in values do
      match selected.get? k with
      | some target => target.fire v
      | none => pure ()
  env.currentScope.register unsub
  let selectFn := fun key => do
    let selected ← selectedRef.get
    match selected.get? key with
    | some target => pure target
    | none =>
        let nodeId ← ctx.freshNodeId
        let target ← Event.newNodeWithId nodeId (e.height.inc)
        selectedRef.modify (·.insert key target)
        pure target
  env.decrementDepth
  pure ⟨selectFn⟩⟩

/-- Select a keyed Event from a fan-out selector within SpiderM. -/
def selectM (fan : Event.Fan Spider k v) (key : k) : SpiderM (Event Spider v) :=
  SpiderM.liftIO <| Event.select fan key

/-- Fan out a Sum Event into two Events, auto-allocating NodeIds and registering with scope. -/
def fanEitherM (e : Event Spider (Sum a b)) : SpiderM (Event Spider a × Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.fanEitherM"
  let nodeIdL ← env.timelineCtx.freshNodeId
  let nodeIdR ← env.timelineCtx.freshNodeId
  let leftEvent ← Event.newNodeWithId nodeIdL (e.height.inc)
  let rightEvent ← Event.newNodeWithId nodeIdR (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun ab =>
    match ab with
    | .inl a => leftEvent.fire a
    | .inr b => rightEvent.fire b
  env.currentScope.register unsub
  env.decrementDepth
  pure (leftEvent, rightEvent)⟩

/-- Split an event into two based on a predicate.
    Returns (trueEvent, falseEvent) where trueEvent fires when predicate is true,
    and falseEvent fires when predicate is false. -/
def splitEM (p : a → Bool) (e : Event Spider a)
    : SpiderM (Event Spider a × Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.splitEM"
  let nodeIdT ← env.timelineCtx.freshNodeId
  let nodeIdF ← env.timelineCtx.freshNodeId
  let trueEvent ← Event.newNodeWithId nodeIdT (e.height.inc)
  let falseEvent ← Event.newNodeWithId nodeIdF (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    if p a then trueEvent.fire a else falseEvent.fire a
  env.currentScope.register unsub
  env.decrementDepth
  pure (trueEvent, falseEvent)⟩

/-- Split an event into two based on a predicate (fluent style).
    Enables: `event.splitE' predicate` -/
def splitE' (e : Event Spider a) (p : a → Bool)
    : SpiderM (Event Spider a × Event Spider a) :=
  splitEM p e

/-- Partition an event into two based on a predicate.
    Returns (matching, nonMatching) where matching fires when predicate is true.
    Alias for splitEM with Haskell-style naming. -/
abbrev partitionEM := @splitEM

/-- Partition an event into two based on a predicate (fluent style).
    Enables: `event.partitionE' predicate`
    Alias for splitE' with Haskell-style naming. -/
abbrev partitionE' := @splitE'

/-- Delay an Event by one propagation frame, auto-allocating NodeId and registering with scope.
    Useful for breaking dependency cycles. -/
def delayFrameM (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    match ← getPropagationContext with
    | some queue =>
      let action := derived.fire a
      let pending : PendingFire := ⟨derived.height, nodeId, action⟩
      queue.insertNextFrame pending
    | none => derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Take at most n occurrences from an Event, auto-allocating NodeId and registering with scope. -/
def takeNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    if count < n then
      countRef.set (count + 1)
      derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Take only the first occurrence of an event.
    Specialization of `takeNM 1` for convenience. -/
abbrev onceM (e : Event Spider a) : SpiderM (Event Spider a) :=
  takeNM 1 e

/-- Take only the first occurrence of an event (fluent style).
    Enables: `event.once'` -/
abbrev once' (e : Event Spider a) : SpiderM (Event Spider a) :=
  onceM e

/-- Drop the first n occurrences from an Event, auto-allocating NodeId and registering with scope. -/
def dropNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    countRef.set (count + 1)
    if count >= n then derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Accumulate values over an Event, auto-allocating NodeId and registering with scope.
    Like foldDyn but returns an Event instead of a Dynamic. -/
def accumulateM (f : a → b → b) (initial : b) (e : Event Spider a)
    : SpiderM (Event Spider b) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let stateRef ← IO.mkRef initial
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let old ← stateRef.get
    let new := f a old
    stateRef.set new
    derived.fire new
  env.currentScope.register unsub
  pure derived⟩

/-- Alias for accumulateM (familiar name from other FRP libraries). -/
abbrev scanM := @accumulateM

/-- Emit (previous, current) pairs on each event occurrence.
    Skips the first occurrence since there's no previous value.
    Useful for detecting changes and computing deltas. -/
def withPreviousM (e : Event Spider a) : SpiderM (Event Spider (a × a)) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    prevRef.set (some curr)
    match prev with
    | some p => derived.fire (p, curr)
    | none => pure ()
  env.currentScope.register unsub
  pure derived⟩

/-- Emit (previous, current) pairs on each event occurrence (fluent style).
    Enables: `event.withPrevious'` -/
def withPrevious' (e : Event Spider a) : SpiderM (Event Spider (a × a)) :=
  withPreviousM e

/-- Skip consecutive duplicate values.
    Requires BEq to compare values. Only fires when value differs from previous. -/
def distinctM [BEq a] (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    let shouldFire := match prev with
      | none => true
      | some p => curr != p
    if shouldFire then
      prevRef.set (some curr)
      derived.fire curr
  env.currentScope.register unsub
  pure derived⟩

/-- Skip consecutive duplicate values (fluent style).
    Enables: `event.distinct'` -/
def distinct' [BEq a] (e : Event Spider a) : SpiderM (Event Spider a) :=
  distinctM e

/-- Alias for distinctM (familiar name from RxJS/reactive libraries). -/
abbrev dedupeM := @distinctM

/-- Alias for distinct' (familiar name from RxJS/reactive libraries). -/
abbrev dedupe' := @distinct'

/-- Collect n events before emitting them as a batch.
    Useful for batch processing patterns. -/
def bufferM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider (Array a)) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let bufRef ← IO.mkRef (#[] : Array a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let buf ← bufRef.get
    let newBuf := buf.push a
    if newBuf.size >= n then
      bufRef.set #[]
      derived.fire newBuf
    else
      bufRef.set newBuf
  env.currentScope.register unsub
  pure derived⟩

/-- Collect n events before emitting them as a batch (fluent style).
    Enables: `event.buffer' 5` -/
def buffer' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider (Array a)) :=
  bufferM n e

/-- Combine two Events that fire simultaneously, auto-allocating NodeId
    and registering with scope.
    If both events fire in the same frame, fires once with paired values.
    If only one fires, nothing is emitted for that occurrence. -/
def zipEM (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider (a × b)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.zipEM"
  let nodeId ← env.timelineCtx.freshNodeId
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height

  let value1Ref ← IO.mkRef (none : Option a)
  let value2Ref ← IO.mkRef (none : Option b)
  let flushScheduledRef ← IO.mkRef false

  let scheduleFlush : IO Unit := do
    let alreadyScheduled ← flushScheduledRef.get
    if !alreadyScheduled then
      flushScheduledRef.set true
      let flushAction : IO Unit := do
        flushScheduledRef.set false
        let v1opt ← value1Ref.get
        let v2opt ← value2Ref.get
        -- Always clear values after checking - values only valid within same frame
        value1Ref.set none
        value2Ref.set none
        match (v1opt, v2opt) with
        | (some v1, some v2) => derived.fire (v1, v2)
        | _ => pure ()
      match ← getPropagationContext with
      | some queue =>
        if ← queue.isInFrame then
          let pending : PendingFire := ⟨derived.height, nodeId, flushAction⟩
          queue.insert pending
        else flushAction
      | none => flushAction

  let unsub1 ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush
  let unsub2 ← Reactive.Event.subscribe e2 fun b => do
    value2Ref.set (some b)
    scheduleFlush

  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Combine two Events that fire simultaneously (fluent style).
    Enables: `event1.zipE' event2` -/
def zipE' (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider (a × b)) :=
  zipEM e1 e2

/-- Fire when e1 occurs but e2 does not (in the same frame).
    Auto-allocates NodeId and registers subscriptions with scope. -/
def differenceM (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.differenceM"
  let nodeId ← env.timelineCtx.freshNodeId
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height

  let value1Ref ← IO.mkRef (none : Option a)
  let value2FiredRef ← IO.mkRef false
  let flushScheduledRef ← IO.mkRef false

  let scheduleFlush : IO Unit := do
    let alreadyScheduled ← flushScheduledRef.get
    if !alreadyScheduled then
      flushScheduledRef.set true
      let flushAction : IO Unit := do
        flushScheduledRef.set false
        let v1opt ← value1Ref.get
        let v2fired ← value2FiredRef.get
        value1Ref.set none
        value2FiredRef.set false
        match v1opt with
        | some v1 => if !v2fired then derived.fire v1 else pure ()
        | none => pure ()
      match ← getPropagationContext with
      | some queue =>
        if ← queue.isInFrame then
          queue.insert ⟨derived.height, nodeId, flushAction⟩
        else flushAction
      | none => flushAction

  let unsub1 ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush
  let unsub2 ← Reactive.Event.subscribe e2 fun _ => do
    value2FiredRef.set true
    scheduleFlush

  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Fire when e1 occurs but e2 does not (fluent style).
    Enables: `event1.difference' event2` -/
def difference' (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider a) :=
  differenceM e1 e2

/-- Switch events based on a Dynamic selector.
    Fires from whichever event the Dynamic currently holds.
    All subscriptions are registered with the current scope. -/
def switchDynM (de : Dynamic Spider (Event Spider a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.switchDynM"
  let nodeId ← env.timelineCtx.freshNodeId
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  let initialEvent ← de.sample
  let derivedHeight := Height.inc (max initialEvent.height de.updated.height)
  let derived ← Event.newNodeWithId nodeId derivedHeight
  let unsub ← Reactive.Event.subscribe initialEvent derived.fire
  currentUnsubRef.set unsub

  let unsubOuter ← Reactive.Event.subscribe de.updated fun newEvent => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe newEvent derived.fire
    currentUnsubRef.set unsub

  env.currentScope.register unsubOuter
  env.currentScope.register do
    let unsub ← currentUnsubRef.get
    unsub

  env.decrementDepth
  pure derived⟩

/-- Switch events based on a Dynamic selector (fluent style).
    Enables: `dynEvent.switchDyn'` -/
def switchDyn' (de : Dynamic Spider (Event Spider a)) : SpiderM (Event Spider a) :=
  switchDynM de

end Event

/-! ## Map Fusion Macros

These macros rewrite common add-constant map patterns to the fused
`Event.mapAddConstM` combinator. This avoids deep chains of `x + c`
calls by collapsing constants at construction time.

Note: Fusion assumes `x + c1 + c2` is equivalent to `x + (c1 + c2)`.
-/

set_option linter.unusedVariables false in
macro_rules
| `(Event.mapM (· + $c) $e) => `(Event.mapAddConstM $c $e)
| `(Event.mapM (fun $x => $x + $c) $e) => `(Event.mapAddConstM $c $e)
| `(Event.mapM Nat.succ $e) => `(Event.mapAddConstM 1 $e)
| `(Reactive.Host.Event.mapM (· + $c) $e) => `(Reactive.Host.Event.mapAddConstM $c $e)
| `(Reactive.Host.Event.mapM (fun $x => $x + $c) $e) => `(Reactive.Host.Event.mapAddConstM $c $e)
| `(Reactive.Host.Event.mapM Nat.succ $e) => `(Reactive.Host.Event.mapAddConstM 1 $e)

/-! ## Temporal Combinators

Time-based event combinators for delay, debounce, and throttle.
These use async tasks with IO.sleep for timing. -/

namespace Event

/-- Delay event firings by a specified duration.
    Each firing is independently delayed - if the source fires
    at times t1, t2, the derived fires at t1+d, t2+d.

    Uses an async task with IO.sleep. The delayed fire happens
    in a new propagation frame. Subscription is registered with current scope. -/
def delayDurationM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let unsub ← Reactive.Event.subscribe e fun a => do
      let _ ← IO.asTask (prio := .dedicated) do
        let ms := d.toMilliseconds.toNat
        IO.sleep (UInt32.ofNat ms)
        env.withFrame (derived.fire a)
    env.currentScope.register unsub
    pure derived⟩

/-- Debounce: only fire after the source has been quiet for the specified duration.

    If the source fires rapidly (t1, t2, t3 where gaps < d), only one fire
    occurs at t3+d with the value from t3.

    Useful for text input where you want to wait until the user stops typing.
    Subscription is registered with current scope. -/
def debounceM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let generationRef ← IO.mkRef (0 : Nat)
    let pendingValueRef ← IO.mkRef (none : Option a)

    let unsub ← Reactive.Event.subscribe e fun a => do
      -- Increment generation to "cancel" any pending fires
      let gen ← generationRef.modifyGet fun g => (g + 1, g + 1)
      pendingValueRef.set (some a)

      -- Spawn async task that fires after quiet period
      let _ ← IO.asTask (prio := .dedicated) do
        let ms := d.toMilliseconds.toNat
        IO.sleep (UInt32.ofNat ms)
        -- Check if still the latest generation
        let currentGen ← generationRef.get
        if gen == currentGen then
          match ← pendingValueRef.get with
          | some v => env.withFrame (derived.fire v)
          | none => pure ()
    env.currentScope.register unsub
    pure derived⟩

/-- Throttle: rate limit to at most one fire per interval.

    @param leading If true (default), fire immediately on first occurrence in interval
    @param trailing If true (default), fire at end of interval if events occurred during cooldown
    Subscription is registered with current scope. -/
def throttleM (d : Chronos.Duration) (e : Event Spider a)
    (leading : Bool := true) (trailing : Bool := true) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let lastFireTimeRef ← IO.mkRef (none : Option Nat)  -- milliseconds since start
    let trailingValueRef ← IO.mkRef (none : Option a)
    let trailingScheduledRef ← IO.mkRef false
    let startTime ← IO.monoMsNow

    let unsub ← Reactive.Event.subscribe e fun a => do
      let now ← IO.monoMsNow
      let elapsed := now - startTime
      let lastFire ← lastFireTimeRef.get
      let intervalMs := d.toMilliseconds.toNat

      let shouldFireNow := match lastFire with
        | none => leading
        | some t => (elapsed - t >= intervalMs) && leading

      if shouldFireNow then
        lastFireTimeRef.set (some elapsed)
        derived.fire a
      else if trailing then
        trailingValueRef.set (some a)
        let alreadyScheduled ← trailingScheduledRef.get
        if !alreadyScheduled then
          trailingScheduledRef.set true
          let _ ← IO.asTask (prio := .dedicated) do
            let ms := intervalMs
            IO.sleep (UInt32.ofNat ms)
            trailingScheduledRef.set false
            match ← trailingValueRef.get with
            | some v =>
              trailingValueRef.set none
              let now' ← IO.monoMsNow
              lastFireTimeRef.set (some (now' - startTime))
              env.withFrame (derived.fire v)
            | none => pure ()
    env.currentScope.register unsub
    pure derived⟩

/-- Collect events within a time window and emit as batch.
    Uses tumbling windows: collects events for the duration, emits, then resets.
    The window starts when the first event arrives.
    Subscription is registered with current scope. -/
def windowM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider (Array a)) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let bufferRef ← IO.mkRef (#[] : Array a)
    let windowActiveRef ← IO.mkRef false
    let generationRef ← IO.mkRef (0 : Nat)

    let unsub ← Reactive.Event.subscribe e fun a => do
      -- Add to buffer
      bufferRef.modify (·.push a)

      -- Start window timer if not already active
      let wasActive ← windowActiveRef.modifyGet fun active => (active, true)
      if !wasActive then
        let gen ← generationRef.modifyGet fun g => (g + 1, g + 1)
        -- Spawn timer that fires after window duration
        let _ ← IO.asTask (prio := .dedicated) do
          let ms := d.toMilliseconds.toNat
          IO.sleep (UInt32.ofNat ms)
          -- Check generation to ensure we haven't been disposed
          let currentGen ← generationRef.get
          if gen == currentGen then
            -- Collect and emit buffer
            let batch ← bufferRef.modifyGet fun buf => (buf, #[])
            windowActiveRef.set false
            if batch.size > 0 then
              env.withFrame (derived.fire batch)

    env.currentScope.register unsub
    pure derived⟩

/-! ### Fluent Chainable Combinators

Extension methods enabling dot-notation chaining:
```lean
event.map' f |>.filter' p |>.gate' behavior
-- or with bind
event.map' f >>= (·.filter' p)
```

These are wrappers around the standard combinators with flipped argument order. -/

/-- Map a function over an Event (fluent style).
    Enables: `event.map' f` instead of `Event.mapM f event` -/
def map' (e : Event Spider a) (f : a → b) : SpiderM (Event Spider b) :=
  mapM f e

/-- Filter an Event by a predicate (fluent style).
    Enables: `event.filter' p` -/
def filter' (e : Event Spider a) (p : a → Bool) : SpiderM (Event Spider a) :=
  filterM p e

/-- Filter and map an Event (fluent style).
    Enables: `event.mapMaybe' f` -/
def mapMaybe' (e : Event Spider a) (f : a → Option b) : SpiderM (Event Spider b) :=
  mapMaybeM f e

/-- Map to a constant value (fluent style).
    Enables: `event.mapConst' value` -/
def mapConst' (e : Event Spider α) (b : β) : SpiderM (Event Spider β) :=
  mapConstM b e

/-- Merge with another Event (fluent style).
    Enables: `event1.merge' event2` -/
def merge' (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) :=
  mergeM e1 e2

/-- Tag with a Behavior's value (fluent style).
    Enables: `event.tag' behavior` -/
def tag' (e : Event Spider b) (beh : Behavior Spider a) : SpiderM (Event Spider a) :=
  tagM beh e

/-- Sample a behavior at event occurrence (fluent style, alias for tag'). -/
abbrev sample' := @tag'

/-- Attach a Behavior's value (fluent style).
    Enables: `event.attach' behavior` -/
def attach' (e : Event Spider c) (b : Behavior Spider a) : SpiderM (Event Spider (a × c)) :=
  attachM b e

/-- Snapshot a behavior at event occurrence (fluent style, alias for attach'). -/
abbrev snapshot' := @attach'

/-- Attach with a combining function (fluent style).
    Enables: `event.attachWith' f behavior` -/
def attachWith' (e : Event Spider c) (f : a → c → d) (b : Behavior Spider a)
    : SpiderM (Event Spider d) :=
  attachWithM f b e

/-- Gate by a Boolean Behavior (fluent style).
    Enables: `event.gate' boolBehavior` -/
def gate' (e : Event Spider a) (beh : Behavior Spider Bool) : SpiderM (Event Spider a) :=
  gateM beh e

/-- Take first n occurrences (fluent style).
    Enables: `event.take' 5` -/
def take' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider a) :=
  takeNM n e

/-- Drop first n occurrences (fluent style).
    Enables: `event.drop' 5` -/
def drop' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider a) :=
  dropNM n e

/-- Accumulate values (fluent style).
    Enables: `event.scan' f initial` -/
def scan' (e : Event Spider a) (f : a → b → b) (initial : b) : SpiderM (Event Spider b) :=
  accumulateM f initial e

/-- Delay by one frame (fluent style).
    Enables: `event.delayFrame'` -/
def delayFrame' (e : Event Spider a) : SpiderM (Event Spider a) :=
  delayFrameM e

/-- Delay by duration (fluent style).
    Enables: `event.delay' duration` -/
def delay' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider a) :=
  delayDurationM d e

/-- Debounce (fluent style).
    Enables: `event.debounce' duration` -/
def debounce' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider a) :=
  debounceM d e

/-- Throttle (fluent style).
    Enables: `event.throttle' duration` -/
def throttle' (e : Event Spider a) (d : Chronos.Duration)
    (leading : Bool := true) (trailing : Bool := true) : SpiderM (Event Spider a) :=
  throttleM d e leading trailing

/-- Collect events within a time window and emit as batch (fluent style).
    Enables: `event.window' duration` -/
def window' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider (Array a)) :=
  windowM d e

/-- Split Sum into two events (fluent style).
    Enables: `event.fanEither'` -/
def fanEither' (e : Event Spider (Sum a b)) : SpiderM (Event Spider a × Event Spider b) :=
  fanEitherM e

end Event

/-! ## Adjustable Helpers

Additional combinators for higher-order FRP patterns. -/

/-- Run a computation that can request its own replacement.
    The computation returns both a result and an event carrying replacement computations.
    This is useful for self-replacing widgets or state machines.
    Subscription is registered with current scope. -/
def runWithReplaceRequester (computation : SpiderM (a × Event Spider (SpiderM a)))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx

  -- Run the initial computation
  let (initialResult, selfReplaceEvent) ← computation.run env

  -- Subscribe to the replacement event
  let unsub ← Reactive.Event.subscribe selfReplaceEvent fun replacementM => do
    let newResult ← replacementM.run env
    fireResult newResult
  env.currentScope.register unsub

  pure (initialResult, resultEvent)
⟩

/-- Traverse a dynamic list, rebuilding results when the list changes.
    Returns a Dynamic of results that updates whenever the input list changes.

    Note: This rebuilds all results on each change. For incremental updates,
    a more sophisticated implementation would be needed.
    Subscription is registered with current scope. -/
def traverseDynList (f : a → SpiderM b) (dynList : Dynamic Spider (List a))
    : SpiderM (Dynamic Spider (List b)) := ⟨fun env => do
  -- Get initial list and compute initial results
  let initialList ← dynList.sample
  let initialResults ← initialList.mapM fun a => (f a).run env

  -- Create result dynamic
  let (resultDyn, updateResult) ← createDynamic env.timelineCtx initialResults

  -- Subscribe to list changes and rebuild results
  let unsub ← Reactive.Event.subscribe dynList.updated fun newList => do
    let newResults ← newList.mapM fun a => (f a).run env
    updateResult newResults
  env.currentScope.register unsub

  pure resultDyn
⟩

/-! ## Integration Helpers

Common patterns for integrating reactive networks with external systems. -/

/-- Create a poll-based event source.
    The provided IO action is polled repeatedly. When it returns `some value`,
    the event fires with that value. When it returns `none`, no event fires.

    Use with `runSpiderLoop` for continuous polling, or call the returned
    poll action manually.

    Example:
    ```
    -- Create event that fires when stdin has input
    let (inputEvent, pollInput) ← fromIO do
      if ← IO.getStdin.anyAvailable then
        some <$> IO.getStdin.getLine
      else
        pure none
    ```
-/
def fromIO (poll : IO (Option a)) : SpiderM (Event Spider a × IO Unit) := do
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let pollAction : IO Unit := do
    match ← poll with
    | some value => fire value
    | none => pure ()
  pure (event, pollAction)

/-- Export an event as a callback.
    Subscribes to the event and calls the provided callback whenever it fires.
    The subscription is registered with the current scope for automatic cleanup.

    Example:
    ```
    -- Forward clicks to an external system
    toCallback clickEvent fun pos =>
      ExternalUI.handleClick pos
    ```
-/
def toCallback (event : Event Spider a) (callback : a → IO Unit) : SpiderM Unit := do
  let _ ← Event.subscribeM event callback
  pure ()

/-- Run IO actions from an event and return an event of results.
    This is the core pattern for performing effects in response to FRP events.

    The IO action is executed synchronously when the source event fires,
    and the resulting event fires with the action's result.
    Subscription is registered with current scope for automatic cleanup.

    Example:
    ```
    -- Save to file whenever document changes
    let saveResults ← performEvent (saveEvent.map' fun doc => saveDocument doc)
    -- saveResults fires with the save success/failure after each save
    ```
-/
def performEvent (event : Event Spider (IO a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId event.height.inc
  let unsub ← Reactive.Event.subscribe event fun action => do
    let result ← action
    derived.fire result
  env.currentScope.register unsub
  pure derived⟩

/-- Run IO actions from an event, discarding results.
    Use this when you only care about the side effect, not the result.
    Subscription is registered with current scope for automatic cleanup.

    Example:
    ```
    -- Log every button click
    let logEvents ← clickEvent.map' fun _ => IO.println "Button clicked!"
    performEvent_ logEvents
    ```
-/
def performEvent_ (event : Event Spider (IO Unit)) : SpiderM Unit := ⟨fun env => do
  let unsub ← Reactive.Event.subscribe event fun action => action
  env.currentScope.register unsub⟩

/-- Create an event from an IO.Ref.
    Returns an event that fires whenever the ref is modified, plus a function
    to trigger updates. The event fires with the new value after modification.

    Example:
    ```
    let (stateEvent, updateState) ← fromRef initialState
    -- Later:
    updateState (· + 1)  -- Fires stateEvent with new value
    ```
-/
def fromRef (initial : a) : SpiderM (Event Spider a × (a → IO Unit) × IO.Ref a) := do
  let ref ← SpiderM.liftIO <| IO.mkRef initial
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let update := fun newValue => do
    ref.set newValue
    fire newValue
  pure (event, update, ref)

/-- Create an event and behavior pair from a mutable ref.
    The behavior always samples the current ref value.
    The event fires when update is called.

    This is similar to `holdDyn` but gives you direct control over when
    updates happen via the returned update function.
-/
def fromRefWithBehavior (initial : a) : SpiderM (Event Spider a × Behavior Spider a × (a → IO Unit)) := do
  let ref ← SpiderM.liftIO <| IO.mkRef initial
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let behavior := Behavior.fromSample ref.get
  let update := fun newValue => do
    ref.set newValue
    fire newValue
  pure (event, behavior, update)

/-- Run a Spider network and return the result -/
def runSpider (network : SpiderM a) : IO a :=
  SpiderM.runFresh network

/-- Run a Spider network with a custom error handler.
    - `defaultErrorHandler`: logs errors and continues (default for runSpider)
    - `strictErrorHandler`: re-raises first error, stopping propagation -/
def runSpiderWithErrorHandler (network : SpiderM a)
    (errorHandler : PropagationErrorHandler) : IO a :=
  SpiderM.runFresh network errorHandler

/-- Run a Spider network with an event loop.

    The eventSource function is called repeatedly to get external events.
    It should return:
    - `some action` to fire an event (action is the trigger)
    - `none` when there are no more events

    The loop runs until shouldQuit returns true. -/
partial def runSpiderLoop (network : SpiderM a) (eventSource : IO (Option (IO Unit)))
    (shouldQuit : IO Bool) : IO a := do
  let result ← runSpider network

  -- Simple event loop
  let rec loop : IO Unit := do
    if ← shouldQuit then
      pure ()
    else
      match ← eventSource with
      | some action => do
          action
          loop
      | none => do
          -- Small delay to avoid busy-waiting
          IO.sleep 10
          loop

  loop
  pure result

end Reactive.Host
