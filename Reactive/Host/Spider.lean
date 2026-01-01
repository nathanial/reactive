/-
  Reactive/Host/Spider.lean

  Spider: An IO-based push runtime for the Reactive FRP library.
  Named after Reflex's Spider host.
-/
import Reactive.Core
import Reactive.Class
import Reactive.Combinators
import Chronos

namespace Reactive.Host

/-- The Spider timeline marker type.
    Spider is an IO-based push propagation runtime. -/
structure Spider where
  private mk ::

instance : Timeline Spider where

/-- Environment for the Spider monad -/
structure SpiderEnv where
  /-- Counter for generating unique node IDs -/
  nextNodeId : IO.Ref Nat
  /-- Actions to run after the network is fully built -/
  postBuildActions : IO.Ref (Array (IO Unit))
  /-- The post-build event (fires once after construction) -/
  postBuildEvent : Event Spider Unit
  /-- Trigger for the post-build event -/
  postBuildTrigger : Unit → IO Unit
  /-- Propagation queue for frame-based event handling -/
  propagationQueue : IO.Ref PropagationQueue

namespace SpiderEnv

/-- Create a new Spider environment -/
def new : IO SpiderEnv := do
  let nextNodeId ← IO.mkRef 1  -- Start at 1, reserve 0 for special use
  let postBuildActions ← IO.mkRef #[]
  let (postBuildEvent, postBuildTrigger) ← Event.newTrigger ⟨0⟩
  let propagationQueue ← IO.mkRef {}
  -- Set global propagation context for frame-based firing
  setPropagationContext propagationQueue
  pure { nextNodeId, postBuildActions, postBuildEvent, postBuildTrigger, propagationQueue }

/-- Process all pending fires in height order until queue is empty.
    When current frame is empty, processes nextFramePending in a new sub-frame. -/
partial def drainQueue (env : SpiderEnv) : IO Unit := do
  let q ← env.propagationQueue.get
  match q.popMin? with
  | none =>
    -- Current frame empty, check for next-frame events (from delayFrame)
    if q.nextFramePending.isEmpty then
      return ()  -- All done
    else
      -- Start new sub-frame with next-frame events
      env.propagationQueue.set {
        pending := q.nextFramePending,
        nextFramePending := #[],
        inFrame := true
      }
      env.drainQueue
  | some (pending, q') =>
    env.propagationQueue.set q'
    pending.fire  -- Execute the fire action
    env.drainQueue  -- Continue draining

/-- Execute an action within a propagation frame.
    If already in a frame, just runs the action (it will enqueue).
    If not in a frame, starts a new frame, runs action, then drains queue. -/
def withFrame (env : SpiderEnv) (action : IO Unit) : IO Unit := do
  let q ← env.propagationQueue.get
  if q.inFrame then
    -- Already in a frame, just run (fires will enqueue)
    action
  else
    -- Start new frame
    env.propagationQueue.set { q with inFrame := true }
    action
    env.drainQueue
    env.propagationQueue.modify fun q => { q with inFrame := false }

end SpiderEnv

/-- The Spider monad for building reactive networks.

    SpiderM provides:
    - Node ID generation for the reactive graph
    - Post-build action registration
    - All FRP typeclass instances -/
structure SpiderM (a : Type) where
  run : SpiderEnv → IO a

namespace SpiderM

/-- Run a SpiderM action with a fresh environment -/
def runFresh (m : SpiderM a) : IO a := do
  let env ← SpiderEnv.new
  let result ← m.run env
  -- Fire post-build event
  env.postBuildTrigger ()
  pure result

/-- Get a fresh node ID -/
def freshNodeId : SpiderM NodeId := ⟨fun env => do
  env.nextNodeId.modifyGet fun n => (⟨n⟩, n + 1)⟩

/-- Register a post-build action -/
def registerPostBuild (action : IO Unit) : SpiderM Unit := ⟨fun env => do
  env.postBuildActions.modify (·.push action)⟩

instance : Monad SpiderM where
  pure a := ⟨fun _ => pure a⟩
  bind ma f := ⟨fun env => do
    let a ← ma.run env
    (f a).run env⟩

instance : MonadLiftT IO SpiderM where
  monadLift io := ⟨fun _ => io⟩

/-- Lift IO actions into SpiderM. Shorter alias for `liftM (m := IO)`. -/
def liftIO (action : IO α) : SpiderM α := liftM (m := IO) action

instance : MonadSample Spider SpiderM where
  sample b := ⟨fun _ => b.sample⟩

private def nextNodeIdIO (env : SpiderEnv) : IO NodeId := do
  env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)

instance : MonadHold Spider SpiderM where
  hold initial event := ⟨fun env => do
    let _nodeId ← nextNodeIdIO env
    -- Create a behavior that holds the latest value
    let valueRef ← IO.mkRef initial
    let _ ← event.subscribe fun a => valueRef.set a
    pure (Behavior.fromSample valueRef.get)⟩

  holdDyn initial event := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    Dynamic.hold initial event nodeId⟩

  foldDyn f initial event := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    Dynamic.foldDyn f initial event nodeId⟩

  foldDynM f initial event := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    -- For monadic fold, we create a dynamic and update it with each event
    let (dyn, update) ← Dynamic.new initial nodeId
    let _ ← event.subscribe fun a => do
      let old ← dyn.sample
      -- Run the SpiderM action to get the new value
      let newM := f a old
      let new ← newM.run env
      update new
    pure dyn⟩

instance : TriggerEvent Spider SpiderM where
  newTriggerEvent := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    let (event, rawTrigger) ← Event.newTrigger nodeId
    -- Wrap trigger to use frame semantics for glitch-free propagation
    let framedTrigger := fun a => env.withFrame (rawTrigger a)
    pure (event, framedTrigger)⟩

  newEventWithTrigger setup := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    let (event, rawTrigger) ← Event.newTrigger nodeId
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
    let resultNodeId ← nextNodeIdIO env
    let (resultEvent, fireResult) ← Event.newTrigger resultNodeId

    -- Subscribe to replacement events - when fired, run the new computation
    let _ ← replaceEvent.subscribe fun replacementM => do
      let result ← replacementM.run env
      fireResult result

    pure (initialResult, resultEvent)
  ⟩

  traverseWithAdjust f inputs := ⟨fun env => do
    -- Run all computations over the input list
    let results ← inputs.mapM fun a => (f a).run env

    -- For a minimal implementation, return a never-firing update event
    -- A full implementation would track per-item replacement events
    let neverEvent ← Event.never (t := Spider)
    pure (results, neverEvent)
  ⟩

/-- Convenience function for runWithReplace with explicit types.
    Direct implementation to avoid universe inference issues. -/
def runWithReplaceM (initial : SpiderM a) (replaceEvent : Event Spider (SpiderM a))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  let initialResult ← initial.run env
  let resultNodeId ← nextNodeIdIO env
  let (resultEvent, fireResult) ← Event.newTrigger resultNodeId
  let _ ← replaceEvent.subscribe fun replacementM => do
    let result ← replacementM.run env
    fireResult result
  pure (initialResult, resultEvent)
⟩

/-- Convenience function for traverseWithAdjust with explicit types.
    Direct implementation to avoid universe inference issues. -/
def traverseWithAdjustM (f : a → SpiderM b) (inputs : List a)
    : SpiderM (List b × Event Spider (List b)) := ⟨fun env => do
  let results ← inputs.mapM fun x => (f x).run env
  let neverEvent ← Event.never (t := Spider)
  pure (results, neverEvent)
⟩

end SpiderM

/-! ## Dynamic SpiderM Combinators

These provide ergonomic versions of Dynamic operations that auto-allocate NodeIds,
enabling Functor/Applicative-like usage within the SpiderM monad. -/

namespace Dynamic

/-- Map a function over a Dynamic, auto-allocating NodeId. -/
def mapM (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Dynamic.map f da nodeId

/-- Combine two Dynamics with a function, auto-allocating NodeId. -/
def zipWithM (f : a → b → c) (da : Dynamic Spider a) (db : Dynamic Spider b)
    : SpiderM (Dynamic Spider c) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Dynamic.zipWith f da db nodeId

/-- Combine three Dynamics with a function, auto-allocating NodeIds. -/
def zipWith3M (f : a → b → c → d)
    (da : Dynamic Spider a) (db : Dynamic Spider b) (dc : Dynamic Spider c)
    : SpiderM (Dynamic Spider d) := do
  -- Implemented using zipWithM to avoid importing Combinators
  let ab ← Dynamic.zipWithM Prod.mk da db
  Dynamic.zipWithM (fun (a, b) c => f a b c) ab dc

/-- Create a constant Dynamic that never changes.
    Note: No NodeId needed since constant dynamics use Event.never. -/
def pureM (x : a) : SpiderM (Dynamic Spider a) :=
  SpiderM.liftIO <| Dynamic.constant x

/-- Applicative apply for Dynamics, auto-allocating NodeId. -/
def apM (df : Dynamic Spider (a → b)) (da : Dynamic Spider a)
    : SpiderM (Dynamic Spider b) :=
  Dynamic.zipWithM (fun f a => f a) df da

end Dynamic

/-! ## Event SpiderM Combinators

These provide ergonomic versions of Event operations that auto-allocate NodeIds,
enabling cleaner composition within the SpiderM monad. -/

namespace Event

/-- Map a function over an Event, auto-allocating NodeId. -/
def mapM (f : a → b) (e : Event Spider a) : SpiderM (Event Spider b) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.map f e nodeId

/-- Filter an Event by a predicate, auto-allocating NodeId. -/
def filterM (p : a → Bool) (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.filter p e nodeId

/-- Filter and map an Event, auto-allocating NodeId. -/
def mapMaybeM (f : a → Option b) (e : Event Spider a) : SpiderM (Event Spider b) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.mapMaybe f e nodeId

/-- Merge two Events, auto-allocating NodeId. -/
def mergeM (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.merge e1 e2 nodeId

/-- Tag an Event with a Behavior's current value, auto-allocating NodeId. -/
def tagM (beh : Behavior Spider a) (e : Event Spider b) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.tag beh e nodeId

/-- Attach a Behavior's value to an Event, auto-allocating NodeId. -/
def attachM (b : Behavior Spider a) (e : Event Spider c) : SpiderM (Event Spider (a × c)) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.attach b e nodeId

/-- Attach with a combining function, auto-allocating NodeId. -/
def attachWithM (f : a → c → d) (b : Behavior Spider a) (e : Event Spider c)
    : SpiderM (Event Spider d) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.attachWith f b e nodeId

/-- Gate an Event by a Boolean Behavior, auto-allocating NodeId. -/
def gateM (beh : Behavior Spider Bool) (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.gate beh e nodeId

/-- Merge a list of Events into a list Event, auto-allocating NodeId. -/
def mergeListM (events : List (Event Spider a)) : SpiderM (Event Spider (List a)) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.mergeList events nodeId

/-- Take the leftmost firing Event from a list, auto-allocating NodeId. -/
def leftmostM (events : List (Event Spider a)) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.leftmost events nodeId

/-- Fan out a Sum Event into two Events, auto-allocating NodeIds. -/
def fanEitherM (e : Event Spider (Sum a b)) : SpiderM (Event Spider a × Event Spider b) := do
  let nodeIdL ← SpiderM.freshNodeId
  let nodeIdR ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.fanEither e nodeIdL nodeIdR

/-- Delay an Event by one propagation frame, auto-allocating NodeId.
    Useful for breaking dependency cycles. -/
def delayFrameM (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.delayFrame e nodeId

/-- Take at most n occurrences from an Event, auto-allocating NodeId. -/
def takeNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.takeN n e nodeId

/-- Drop the first n occurrences from an Event, auto-allocating NodeId. -/
def dropNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.dropN n e nodeId

/-- Accumulate values over an Event, auto-allocating NodeId.
    Like foldDyn but returns an Event instead of a Dynamic. -/
def accumulateM (f : a → b → b) (initial : b) (e : Event Spider a)
    : SpiderM (Event Spider b) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.accumulate f initial e nodeId

/-- Alias for accumulateM (familiar name from other FRP libraries). -/
abbrev scanM := @accumulateM

end Event

/-! ## Temporal Combinators

Time-based event combinators for delay, debounce, and throttle.
These use async tasks with IO.sleep for timing. -/

namespace Event

/-- Delay event firings by a specified duration.
    Each firing is independently delayed - if the source fires
    at times t1, t2, the derived fires at t1+d, t2+d.

    Uses an async task with IO.sleep. The delayed fire happens
    in a new propagation frame. -/
def delayDurationM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)
    let derived ← Event.newNode nodeId (e.height.inc)
    let _ ← e.subscribe fun a => do
      let _ ← IO.asTask (prio := .dedicated) do
        let ms := d.toMilliseconds.toNat
        IO.sleep (UInt32.ofNat ms)
        env.withFrame (derived.fire a)
    pure derived⟩

/-- Debounce: only fire after the source has been quiet for the specified duration.

    If the source fires rapidly (t1, t2, t3 where gaps < d), only one fire
    occurs at t3+d with the value from t3.

    Useful for text input where you want to wait until the user stops typing. -/
def debounceM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)
    let derived ← Event.newNode nodeId (e.height.inc)
    let generationRef ← IO.mkRef (0 : Nat)
    let pendingValueRef ← IO.mkRef (none : Option a)

    let _ ← e.subscribe fun a => do
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
    pure derived⟩

/-- Throttle: rate limit to at most one fire per interval.

    @param leading If true (default), fire immediately on first occurrence in interval
    @param trailing If true (default), fire at end of interval if events occurred during cooldown -/
def throttleM (d : Chronos.Duration) (e : Event Spider a)
    (leading : Bool := true) (trailing : Bool := true) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)
    let derived ← Event.newNode nodeId (e.height.inc)
    let lastFireTimeRef ← IO.mkRef (none : Option Nat)  -- milliseconds since start
    let trailingValueRef ← IO.mkRef (none : Option a)
    let trailingScheduledRef ← IO.mkRef false
    let startTime ← IO.monoMsNow

    let _ ← e.subscribe fun a => do
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
    pure derived⟩

end Event

/-! ## Adjustable Helpers

Additional combinators for higher-order FRP patterns. -/

/-- Run a computation that can request its own replacement.
    The computation returns both a result and an event carrying replacement computations.
    This is useful for self-replacing widgets or state machines. -/
def runWithReplaceRequester (computation : SpiderM (a × Event Spider (SpiderM a)))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  let resultNodeId ← env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)
  let (resultEvent, fireResult) ← Event.newTrigger resultNodeId

  -- Run the initial computation
  let (initialResult, selfReplaceEvent) ← computation.run env

  -- Subscribe to the replacement event
  let _ ← selfReplaceEvent.subscribe fun replacementM => do
    let newResult ← replacementM.run env
    fireResult newResult

  pure (initialResult, resultEvent)
⟩

/-- Traverse a dynamic list, rebuilding results when the list changes.
    Returns a Dynamic of results that updates whenever the input list changes.

    Note: This rebuilds all results on each change. For incremental updates,
    a more sophisticated implementation would be needed. -/
def traverseDynList (f : a → SpiderM b) (dynList : Dynamic Spider (List a))
    : SpiderM (Dynamic Spider (List b)) := ⟨fun env => do
  -- Get initial list and compute initial results
  let initialList ← dynList.sample
  let initialResults ← initialList.mapM fun a => (f a).run env

  -- Create result dynamic
  let nodeId ← env.nextNodeId.modifyGet fun n => (NodeId.mk n, n + 1)
  let (resultDyn, updateResult) ← Dynamic.new initialResults nodeId

  -- Subscribe to list changes and rebuild results
  let _ ← dynList.updated.subscribe fun newList => do
    let newResults ← newList.mapM fun a => (f a).run env
    updateResult newResults

  pure resultDyn
⟩

/-- Run a Spider network and return the result -/
def runSpider (network : SpiderM a) : IO a :=
  SpiderM.runFresh network

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
