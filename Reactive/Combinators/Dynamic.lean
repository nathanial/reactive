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

/-- Combine three dynamics -/
def zipWith3 [Timeline t] (f : a → b → c → d) (da : Dynamic t a) (db : Dynamic t b)
    (dc : Dynamic t c) (nodeId1 : NodeId) (nodeId2 : NodeId) : IO (Dynamic t d) := do
  let ab ← Dynamic.zipWith Prod.mk da db nodeId1
  Dynamic.zipWith (fun (a, b) c => f a b c) ab dc nodeId2

/-- Create a Dynamic from a constant value -/
def pure' [Timeline t] (x : a) : IO (Dynamic t a) :=
  Dynamic.constant x

/-- Tag Dynamic's update event with a value -/
def tagUpdated [Timeline t] (b : a) (d : Dynamic t c) (nodeId : NodeId) : IO (Event t a) := do
  let updateEvent := d.updated
  Event.map (fun _ => b) updateEvent nodeId

/-- Get an event that fires with the old and new values on each change -/
def changes [Timeline t] (d : Dynamic t a) (nodeId : NodeId) : IO (Event t (a × a)) := do
  let oldRef ← IO.mkRef (← d.sample)
  let derived ← Event.newNode nodeId (d.updated.height.inc)
  let _ ← d.updated.subscribe fun newVal => do
    let oldVal ← oldRef.get
    oldRef.set newVal
    derived.fire (oldVal, newVal)
  pure derived

end Dynamic

end Reactive
