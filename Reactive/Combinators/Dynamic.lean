/-
  Reactive/Combinators/Dynamic.lean

  Combinators for working with Dynamics.
-/
import Reactive.Core

namespace Reactive

namespace Dynamic

/-- Get the initial value of a Dynamic (samples immediately) -/
def value (d : Dynamic t a) : IO a :=
  d.sample

/-- Convert a Dynamic to a Behavior (discards change events) -/
def toBehavior (d : Dynamic t a) : Behavior t a :=
  d.current

/-- Combine three dynamics (with explicit NodeIds). -/
def zipWith3Id [Timeline t] (f : a → b → c → d) (da : Dynamic t a) (db : Dynamic t b)
    (dc : Dynamic t c) (nodeId1 : NodeId) (nodeId2 : NodeId) : IO (Dynamic t d) := do
  let ab ← Dynamic.zipWithId Prod.mk da db nodeId1
  Dynamic.zipWithId (fun (a, b) c => f a b c) ab dc nodeId2

/-- Combine three dynamics.
    Requires TimelineCtx for type-safe timeline separation. -/
def zipWith3 [Timeline t] (ctx : TimelineCtx t) (f : a → b → c → d) (da : Dynamic t a) (db : Dynamic t b)
    (dc : Dynamic t c) : IO (Dynamic t d) := do
  let nodeId1 ← ctx.freshNodeId
  let nodeId2 ← ctx.freshNodeId
  zipWith3Id f da db dc nodeId1 nodeId2

/-- Create a Dynamic from a constant value (with explicit NodeId). -/
def pure'Id [Timeline t] (x : a) (nodeId : NodeId) : IO (Dynamic t a) :=
  Dynamic.constantWithId x nodeId

/-- Create a Dynamic from a constant value.
    Requires TimelineCtx for type-safe timeline separation. -/
def pure' [Timeline t] (ctx : TimelineCtx t) (x : a) : IO (Dynamic t a) :=
  Dynamic.constant ctx x

/-- Tag Dynamic's update event with a value (with explicit NodeId). -/
def tagUpdatedId [Timeline t] (b : a) (d : Dynamic t c) (nodeId : NodeId) : IO (Event t a) := do
  let updateEvent := d.updated
  Event.mapWithId (fun _ => b) updateEvent nodeId

/-- Tag Dynamic's update event with a value.
    Requires TimelineCtx for type-safe timeline separation. -/
def tagUpdated [Timeline t] (ctx : TimelineCtx t) (b : a) (d : Dynamic t c) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  tagUpdatedId b d nodeId

/-- Get an event that fires with the old and new values on each change (with explicit NodeId). -/
def changesId [Timeline t] (d : Dynamic t a) (nodeId : NodeId) : IO (Event t (a × a)) := do
  let oldRef ← IO.mkRef (← d.sample)
  let derived ← Event.newNodeWithId nodeId (d.updated.height.inc)
  let _ ← d.updated.subscribe fun newVal => do
    let oldVal ← oldRef.get
    oldRef.set newVal
    derived.fire (oldVal, newVal)
  pure derived

/-- Get an event that fires with the old and new values on each change.
    Requires TimelineCtx for type-safe timeline separation. -/
def changes [Timeline t] (ctx : TimelineCtx t) (d : Dynamic t a) : IO (Event t (a × a)) := do
  let nodeId ← ctx.freshNodeId
  changesId d nodeId

end Dynamic

end Reactive
