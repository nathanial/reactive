/-
  Reactive/Combinators/Switch.lean

  Switching combinators for higher-order FRP.
-/
import Reactive.Core

namespace Reactive

/-- Switch to the event inside a behavior of events.
    The resulting event fires whenever the "current" inner event fires.
    When the behavior changes to a new event, the old subscription is dropped. -/
def switch [Timeline t] (be : Behavior t (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId ⟨0⟩  -- Height depends on inner events
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Initial subscription
  let initialEvent ← be.sample
  let unsub ← initialEvent.subscribe derived.fire
  currentUnsubRef.set unsub

  -- Note: This simplified version doesn't handle behavior changes
  -- A full implementation would track when the behavior changes
  -- and resubscribe to the new event

  pure derived

/-- Switch using a Dynamic of events.
    Uses the Dynamic's change event to know when to switch. -/
def switchDyn [Timeline t] (de : Dynamic t (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId ⟨0⟩
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Subscribe to the initial event
  let initialEvent ← de.sample
  let unsub ← initialEvent.subscribe derived.fire
  currentUnsubRef.set unsub

  -- When the dynamic changes, switch to the new event
  let _ ← de.updated.subscribe fun newEvent => do
    -- Unsubscribe from old event
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    -- Subscribe to new event
    let unsub ← newEvent.subscribe derived.fire
    currentUnsubRef.set unsub

  pure derived

/-- Switch behaviors - sample from whichever behavior the outer behavior currently holds -/
def switchBehavior (bb : Behavior t (Behavior t a)) : Behavior t a :=
  Behavior.fromSample do
    let inner ← bb.sample
    inner.sample

/-- Switch dynamics - like switchBehavior but preserves change events -/
def switchDynamic [Timeline t] (dd : Dynamic t (Dynamic t a)) (nodeId : NodeId)
    : IO (Dynamic t a) := do
  let initialInner ← dd.sample
  let (result, _) ← Dynamic.new (← initialInner.sample) nodeId
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Helper to subscribe to an inner dynamic
  let subscribeToInner := fun (inner : Dynamic t a) => do
    -- Unsubscribe from old
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    -- Subscribe to inner changes
    let unsub ← inner.updated.subscribe fun _ => do
      -- We can't easily update the result dynamic here
      -- This is a limitation of the simplified implementation
      pure ()
    currentUnsubRef.set unsub

  -- Subscribe to initial inner dynamic
  let unsub ← initialInner.updated.subscribe fun _ => pure ()
  currentUnsubRef.set unsub

  -- When outer changes, switch to new inner
  let _ ← dd.updated.subscribe subscribeToInner

  pure result

/-- Hold an event, switching to newer events as they arrive -/
def switchHold [Timeline t] (initial : Event t a) (updates : Event t (Event t a))
    (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId ⟨0⟩
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Subscribe to initial
  let unsub ← initial.subscribe derived.fire
  currentUnsubRef.set unsub

  -- On each update event, switch to the new event
  let _ ← updates.subscribe fun newEvent => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← newEvent.subscribe derived.fire
    currentUnsubRef.set unsub

  pure derived

end Reactive
