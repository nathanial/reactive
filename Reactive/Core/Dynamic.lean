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
  private mk ::
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

/-- Create a new Dynamic with an initial value (with explicit NodeId).
    Returns the Dynamic and a function to update its value. -/
def newWithId [Timeline t] (initial : a) (nodeId : NodeId) : IO (Dynamic t a × (a → IO Unit)) := do
  let valueRef ← IO.mkRef initial
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let update := fun newValue => do
    valueRef.set newValue
    trigger newValue
  pure (⟨valueRef, changeEvent, trigger⟩, update)

/-- Create a new Dynamic with an initial value.
    Returns the Dynamic and a function to update its value.
    Requires TimelineCtx for type-safe timeline separation. -/
def new [Timeline t] (ctx : TimelineCtx t) (initial : a) : IO (Dynamic t a × (a → IO Unit)) := do
  let nodeId ← ctx.freshNodeId
  newWithId initial nodeId

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

/-- Map a function over a Dynamic's values (with explicit NodeId). -/
def mapWithId [Timeline t] (f : a → b) (da : Dynamic t a) (nodeId : NodeId) : IO (Dynamic t b) := do
  let initial ← da.sample
  let valueRef ← IO.mkRef (f initial)
  let mappedEvent ← Event.mapWithId f da.changeEvent nodeId
  let _ ← mappedEvent.subscribe fun b => valueRef.set b
  pure ⟨valueRef, mappedEvent, fun _ => pure ()⟩

/-- Map a function over a Dynamic's values.
    Requires TimelineCtx for type-safe timeline separation. -/
def map [Timeline t] (ctx : TimelineCtx t) (f : a → b) (da : Dynamic t a) : IO (Dynamic t b) := do
  let nodeId ← ctx.freshNodeId
  mapWithId f da nodeId

/-- Combine two Dynamics with a function (with explicit NodeId). -/
def zipWithId [Timeline t] (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b)
    (nodeId : NodeId) : IO (Dynamic t c) := do
  let a ← da.sample
  let b ← db.sample
  let valueRef ← IO.mkRef (f a b)
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId

  -- Subscribe to changes in da
  let _ ← da.changeEvent.subscribe fun newA => do
    let currentB ← db.sample
    let newC := f newA currentB
    valueRef.set newC
    trigger newC

  -- Subscribe to changes in db
  let _ ← db.changeEvent.subscribe fun newB => do
    let currentA ← da.sample
    let newC := f currentA newB
    valueRef.set newC
    trigger newC

  pure ⟨valueRef, changeEvent, trigger⟩

/-- Combine two Dynamics with a function.
    Requires TimelineCtx for type-safe timeline separation. -/
def zipWith [Timeline t] (ctx : TimelineCtx t) (f : a → b → c) (da : Dynamic t a) (db : Dynamic t b) : IO (Dynamic t c) := do
  let nodeId ← ctx.freshNodeId
  zipWithId f da db nodeId

/-- Pair two Dynamics (with explicit NodeId). -/
def zipId [Timeline t] (da : Dynamic t a) (db : Dynamic t b) (nodeId : NodeId) : IO (Dynamic t (a × b)) :=
  zipWithId Prod.mk da db nodeId

/-- Pair two Dynamics.
    Requires TimelineCtx for type-safe timeline separation. -/
def zip [Timeline t] (ctx : TimelineCtx t) (da : Dynamic t a) (db : Dynamic t b) : IO (Dynamic t (a × b)) :=
  zipWith ctx Prod.mk da db

/-- Create a Dynamic that holds the most recent value from an Event (with explicit NodeId). -/
def holdWithId [Timeline t] (initial : a) (event : Event t a) (nodeId : NodeId) : IO (Dynamic t a) := do
  let valueRef ← IO.mkRef initial
  let (changeEvent, trigger) ← Event.newTriggerWithId nodeId
  let _ ← event.subscribe fun a => do
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
  let _ ← event.subscribe fun a => do
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
