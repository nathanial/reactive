/-
  Reactive/Host/Spider.lean

  Spider: An IO-based push runtime for the Reactive FRP library.
  Named after Reflex's Spider host.
-/
import Reactive.Core
import Reactive.Class

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
