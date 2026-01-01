/-
  Reactive/Host/Spider.lean

  Spider: An IO-based push runtime for the Reactive FRP library.
  Named after Reflex's Spider host.
-/
import Reactive.Core
import Reactive.Class
import Reactive.Combinators

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

/-- Create a new Spider environment -/
def SpiderEnv.new : IO SpiderEnv := do
  let nextNodeId ← IO.mkRef 1  -- Start at 1, reserve 0 for special use
  let postBuildActions ← IO.mkRef #[]
  let (postBuildEvent, postBuildTrigger) ← Event.newTrigger ⟨0⟩
  pure { nextNodeId, postBuildActions, postBuildEvent, postBuildTrigger }

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
    Event.newTrigger nodeId⟩

  newEventWithTrigger setup := ⟨fun env => do
    let nodeId ← nextNodeIdIO env
    let (event, trigger) ← Event.newTrigger nodeId
    setup trigger
    pure event⟩

instance : PostBuild Spider SpiderM where
  getPostBuild := ⟨fun env => pure env.postBuildEvent⟩

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

/-- Delay an Event by one frame, auto-allocating NodeId.
    Note: Currently a no-op pass-through (see ROADMAP). -/
def delayM (e : Event Spider a) : SpiderM (Event Spider a) := do
  let nodeId ← SpiderM.freshNodeId
  SpiderM.liftIO <| Event.delay e nodeId

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
