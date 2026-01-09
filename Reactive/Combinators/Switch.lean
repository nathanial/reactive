/-
  Reactive/Combinators/Switch.lean

  Switching combinators for higher-order FRP.
-/
import Reactive.Core

namespace Reactive

/-- Switch using a Dynamic of events (with explicit NodeId).
    Uses the Dynamic's change event to know when to switch. -/
def switchDynWithId [Timeline t] (de : Dynamic t (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId nodeId ⟨0⟩
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Subscribe to the initial event
  let initialEvent ← de.sample
  let unsub ← Reactive.Event.subscribe initialEvent derived.fire
  currentUnsubRef.set unsub

  -- When the dynamic changes, switch to the new event
  let _ ← Reactive.Event.subscribe de.updated fun newEvent => do
    -- Unsubscribe from old event
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    -- Subscribe to new event
    let unsub ← Reactive.Event.subscribe newEvent derived.fire
    currentUnsubRef.set unsub

  pure derived

/-- Switch using a Dynamic of events.
    Uses the Dynamic's change event to know when to switch.
    Requires TimelineCtx for type-safe timeline separation. -/
def switchDyn [Timeline t] (ctx : TimelineCtx t) (de : Dynamic t (Event t a)) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  switchDynWithId de nodeId

/-- Switch behaviors - sample from whichever behavior the outer behavior currently holds -/
def switchBehavior (bb : Behavior t (Behavior t a)) : Behavior t a :=
  Behavior.fromSample do
    let inner ← bb.sample
    inner.sample

/-- Switch dynamics (with explicit NodeId) - like switchBehavior but preserves change events.
    The result dynamic updates whenever:
    1. The outer dynamic changes to a new inner dynamic
    2. The current inner dynamic's value changes -/
def switchDynamicWithId [Timeline t] (dd : Dynamic t (Dynamic t a)) (nodeId : NodeId)
    : IO (Dynamic t a) := do
  let initialInner ← dd.sample
  let initialValue ← initialInner.sample
  let (result, updateResult) ← Reactive.Dynamic.newWithId initialValue nodeId
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Helper to subscribe to an inner dynamic
  let subscribeToInner := fun (inner : Dynamic t a) => do
    -- Unsubscribe from old inner
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    -- Subscribe to new inner's changes
    let unsub ← Reactive.Event.subscribe inner.updated fun newValue => do
      updateResult newValue
    currentUnsubRef.set unsub
    -- Update with current value of new inner
    let currentValue ← inner.sample
    updateResult currentValue

  -- Subscribe to initial inner dynamic's changes
  let unsub ← Reactive.Event.subscribe initialInner.updated fun newValue => updateResult newValue
  currentUnsubRef.set unsub

  -- When outer changes to new inner dynamic, resubscribe
  let _ ← Reactive.Event.subscribe dd.updated subscribeToInner

  pure result

/-- Switch dynamics - like switchBehavior but preserves change events.
    The result dynamic updates whenever:
    1. The outer dynamic changes to a new inner dynamic
    2. The current inner dynamic's value changes
    Requires TimelineCtx for type-safe timeline separation. -/
def switchDynamic [Timeline t] (ctx : TimelineCtx t) (dd : Dynamic t (Dynamic t a)) : IO (Dynamic t a) := do
  let nodeId ← ctx.freshNodeId
  switchDynamicWithId dd nodeId

/-- Hold an event, switching to newer events as they arrive (with explicit NodeId). -/
def switchHoldWithId [Timeline t] (initial : Event t a) (updates : Event t (Event t a))
    (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId nodeId ⟨0⟩
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  -- Subscribe to initial
  let unsub ← Reactive.Event.subscribe initial derived.fire
  currentUnsubRef.set unsub

  -- On each update event, switch to the new event
  let _ ← Reactive.Event.subscribe updates fun newEvent => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe newEvent derived.fire
    currentUnsubRef.set unsub

  pure derived

/-- Hold an event, switching to newer events as they arrive.
    Requires TimelineCtx for type-safe timeline separation. -/
def switchHold [Timeline t] (ctx : TimelineCtx t) (initial : Event t a) (updates : Event t (Event t a)) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  switchHoldWithId initial updates nodeId

end Reactive
