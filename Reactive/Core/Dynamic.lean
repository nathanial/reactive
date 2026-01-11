/-
  Reactive/Core/Dynamic.lean

  Dynamic type combining Behavior with Event change notifications.
  A Dynamic is a Behavior that also tells you when it changes.
-/
import Reactive.Core.Types
import Reactive.Core.Event
import Reactive.Core.Behavior

namespace Reactive

/-- A Dynamic combines a Behavior with an Event that fires when the value changes.
    It's essentially a Behavior that also provides change notifications.

    Parameterized by:
    - `t`: The timeline (phantom type for type-safe separation)
    - `a`: The type of values the dynamic holds

    Key operations:
    - `current`: Get the behavior (for sampling the current value)
    - `updated`: Get the event that fires with new values on change -/
structure Dynamic (t : Type) (a : Type) where
  protected mk ::
  /-- Reference holding the current value -/
  private valueRef : IO.Ref a
  /-- Event that fires when the value changes, carrying the new value -/
  private changeEvent : Event t a
  /-- Trigger function for the change event -/
  private triggerChange : a → IO Unit

namespace Dynamic

/-- Get the current value as a Behavior -/
def current (d : Dynamic t a) : Behavior t a :=
  Behavior.fromSample d.valueRef.get

/-- Get the event that fires when the value changes -/
def updated (d : Dynamic t a) : Event t a :=
  d.changeEvent

/-- Sample the current value -/
def sample (d : Dynamic t a) : IO a :=
  d.valueRef.get

/-- Internal: Create a new Dynamic with an initial value (with explicit NodeId).
    Returns the Dynamic and a function to update its value.

    WARNING: This is protected for internal use by combinators. Application code
    should use `holdDyn`, `foldDyn`, or other SpiderM combinators instead.
    Using this directly with the returned setter can lead to anti-patterns
    like subscribe/sample/set which may cause unexpected behavior. -/
protected def newWithId [Timeline t] (initial : a) (nodeId : NodeId) : IO (Dynamic t a × (a → IO Unit)) := do
  let valueRef ← IO.mkRef initial
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let update := fun newValue => do
    valueRef.set newValue
    trigger newValue
  pure (⟨valueRef, changeEvent, trigger⟩, update)

/-- Create a constant Dynamic that never changes (with explicit NodeId). -/
def constantWithId [Timeline t] (x : a) (nodeId : NodeId) : IO (Dynamic t a) := do
  let valueRef ← IO.mkRef x
  let neverEvent ← Event.newNodeWithId (t := t) nodeId
  pure ⟨valueRef, neverEvent, fun _ => pure ()⟩

/-- Create a constant Dynamic that never changes.
    Requires TimelineCtx for type-safe timeline separation. -/
def constant [Timeline t] (ctx : TimelineCtx t) (x : a) : IO (Dynamic t a) := do
  let nodeId ← ctx.freshNodeId
  constantWithId x nodeId

/-- Map a function over a Dynamic's values (with explicit NodeId).
    Only fires the change event when the mapped value actually changes.
    Requires BEq to detect duplicate values. -/
def mapWithId [Timeline t] [BEq b] (f : a → b) (da : Dynamic t a) (nodeId : NodeId) : IO (Dynamic t b) := do
  let initial ← da.sample
  let initialMapped := f initial
  let valueRef ← IO.mkRef initialMapped
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  -- Subscribe to source changes and only fire if mapped value changed
  let _ ← Reactive.Event.subscribe da.changeEvent fun newA => do
    let newB := f newA
    let oldB ← valueRef.get
    if newB != oldB then
      valueRef.set newB
      trigger newB
  pure ⟨valueRef, changeEvent, trigger⟩

/-- Map a function over a Dynamic's values (with explicit NodeId).
    Fires on every source update (no BEq-based deduplication). -/
def mapWithIdRaw [Timeline t] (f : a → b) (da : Dynamic t a) (nodeId : NodeId) : IO (Dynamic t b) := do
  let initial ← da.sample
  let valueRef ← IO.mkRef (f initial)
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let _ ← Reactive.Event.subscribe da.changeEvent fun newA => do
    let newB := f newA
    valueRef.set newB
    trigger newB
  pure ⟨valueRef, changeEvent, trigger⟩

/-- Map a function over a Dynamic's values.
    Requires TimelineCtx for type-safe timeline separation.
    Requires BEq to detect duplicate values. -/
def map [Timeline t] [BEq b] (ctx : TimelineCtx t) (f : a → b) (da : Dynamic t a) : IO (Dynamic t b) := do
  let nodeId ← ctx.freshNodeId
  mapWithId f da nodeId

/-- Map a function over a Dynamic's values.
    Requires TimelineCtx for type-safe timeline separation.
    Fires on every source update (no BEq-based deduplication). -/
def mapRaw [Timeline t] (ctx : TimelineCtx t) (f : a → b) (da : Dynamic t a) : IO (Dynamic t b) := do
  let nodeId ← ctx.freshNodeId
  mapWithIdRaw f da nodeId

/-- Combine two Dynamics with a function (with explicit NodeId).
    Only fires the change event when the combined value actually changes.
    Requires BEq to detect duplicate values. -/
def zipWithId [Timeline t] [BEq c] (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b)
    (nodeId : NodeId) : IO (Dynamic t c) := do
  let a ← da.sample
  let b ← db.sample
  let valueRef ← IO.mkRef (f a b)
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId

  -- Subscribe to changes in da
  let _ ← Reactive.Event.subscribe da.changeEvent fun newA => do
    let currentB ← db.sample
    let newC := f newA currentB
    let oldC ← valueRef.get
    if newC != oldC then
      valueRef.set newC
      trigger newC

  -- Subscribe to changes in db
  let _ ← Reactive.Event.subscribe db.changeEvent fun newB => do
    let currentA ← da.sample
    let newC := f currentA newB
    let oldC ← valueRef.get
    if newC != oldC then
      valueRef.set newC
      trigger newC

  pure ⟨valueRef, changeEvent, trigger⟩

/-- Combine two Dynamics with a function (with explicit NodeId).
    Fires on every source update (no BEq-based deduplication). -/
def zipWithIdRaw [Timeline t] (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b)
    (nodeId : NodeId) : IO (Dynamic t c) := do
  let a ← da.sample
  let b ← db.sample
  let valueRef ← IO.mkRef (f a b)
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId

  -- Subscribe to changes in da
  let _ ← Reactive.Event.subscribe da.changeEvent fun newA => do
    let currentB ← db.sample
    let newC := f newA currentB
    valueRef.set newC
    trigger newC

  -- Subscribe to changes in db
  let _ ← Reactive.Event.subscribe db.changeEvent fun newB => do
    let currentA ← da.sample
    let newC := f currentA newB
    valueRef.set newC
    trigger newC

  pure ⟨valueRef, changeEvent, trigger⟩

/-- Combine two Dynamics with a function.
    Requires TimelineCtx for type-safe timeline separation.
    Requires BEq to detect duplicate values. -/
def zipWith [Timeline t] [BEq c] (ctx : TimelineCtx t) (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b) : IO (Dynamic t c) := do
  let nodeId ← ctx.freshNodeId
  zipWithId f da db nodeId

/-- Combine two Dynamics with a function.
    Requires TimelineCtx for type-safe timeline separation.
    Fires on every source update (no BEq-based deduplication). -/
def zipWithRaw [Timeline t] (ctx : TimelineCtx t) (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b) : IO (Dynamic t c) := do
  let nodeId ← ctx.freshNodeId
  zipWithIdRaw f da db nodeId

/-- Pair two Dynamics (with explicit NodeId). -/
def zipId [Timeline t] [BEq a] [BEq b] (da : Dynamic t a) (db : Dynamic t b) (nodeId : NodeId) : IO (Dynamic t (a × b)) :=
  zipWithId Prod.mk da db nodeId

/-- Pair two Dynamics.
    Requires TimelineCtx for type-safe timeline separation. -/
def zip [Timeline t] [BEq a] [BEq b] (ctx : TimelineCtx t) (da : Dynamic t a) (db : Dynamic t b) : IO (Dynamic t (a × b)) :=
  zipWith ctx Prod.mk da db

/-- Create a Dynamic that holds the most recent value from an Event (with explicit NodeId). -/
def holdWithId [Timeline t] (initial : a) (event : Event t a) (nodeId : NodeId) : IO (Dynamic t a) := do
  let valueRef ← IO.mkRef initial
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let _ ← Reactive.Event.subscribe event fun a => do
    valueRef.set a
    trigger a
  pure ⟨valueRef, changeEvent, trigger⟩

/-- Create a Dynamic that holds the most recent value from an Event.
    Requires TimelineCtx for type-safe timeline separation. -/
def hold [Timeline t] (ctx : TimelineCtx t) (initial : a) (event : Event t a) : IO (Dynamic t a) := do
  let nodeId ← ctx.freshNodeId
  holdWithId initial event nodeId

/-- Fold over event occurrences to create a Dynamic (with explicit NodeId). -/
def foldDynWithId [Timeline t] (f : a → b → b) (initial : b) (event : Event t a)
    (nodeId : NodeId) : IO (Dynamic t b) := do
  let valueRef ← IO.mkRef initial
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let _ ← Reactive.Event.subscribe event fun a => do
    let old ← valueRef.get
    let new := f a old
    valueRef.set new
    trigger new
  pure ⟨valueRef, changeEvent, trigger⟩

/-- Fold over event occurrences to create a Dynamic.
    Requires TimelineCtx for type-safe timeline separation. -/
def foldDyn [Timeline t] (ctx : TimelineCtx t) (f : a → b → b) (initial : b) (event : Event t a) : IO (Dynamic t b) := do
  let nodeId ← ctx.freshNodeId
  foldDynWithId f initial event nodeId

end Dynamic

end Reactive
