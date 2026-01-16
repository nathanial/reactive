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
def zipWith3Id [Timeline t] [BEq a] [BEq b] [BEq d] (f : a → b → c → d) (da : Dynamic t a) (db : Dynamic t b)
    (dc : Dynamic t c) (nodeId1 : NodeId) (nodeId2 : NodeId) : IO (Dynamic t d) := do
  let ab ← Dynamic.zipWithId Prod.mk da db nodeId1
  Dynamic.zipWithId (fun (a, b) c => f a b c) ab dc nodeId2

/-- Combine three dynamics.
    Requires TimelineCtx for type-safe timeline separation. -/
def zipWith3 [Timeline t] [BEq a] [BEq b] [BEq d] (ctx : TimelineCtx t) (f : a → b → c → d) (da : Dynamic t a) (db : Dynamic t b)
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
  let _ ← Reactive.Event.subscribe d.updated fun newVal => do
    let oldVal ← oldRef.get
    oldRef.set newVal
    derived.fire (oldVal, newVal)
  pure derived

/-- Get an event that fires with the old and new values on each change.
    Requires TimelineCtx for type-safe timeline separation. -/
def changes [Timeline t] (ctx : TimelineCtx t) (d : Dynamic t a) : IO (Event t (a × a)) := do
  let nodeId ← ctx.freshNodeId
  changesId d nodeId

/-- Deduplicate a Dynamic's updates (with explicit NodeId).
    Only fires when the value actually changes. -/
def holdUniqDynWithId [Timeline t] [BEq a] (d : Dynamic t a) (nodeId : NodeId) : IO (Dynamic t a) := do
  let initial ← d.sample
  let currentRef ← IO.mkRef initial
  let (result, updateResult) ← Reactive.Dynamic.newWithId initial nodeId
  let _ ← Reactive.Event.subscribe d.updated fun newVal => do
    let current ← currentRef.get
    if newVal != current then
      currentRef.set newVal
      updateResult newVal
  pure result

/-- Deduplicate a Dynamic's updates.
    Only fires when the value actually changes.
    Requires TimelineCtx for type-safe timeline separation. -/
def holdUniqDyn [Timeline t] [BEq a] (ctx : TimelineCtx t) (d : Dynamic t a) : IO (Dynamic t a) := do
  let nodeId ← ctx.freshNodeId
  holdUniqDynWithId d nodeId

/-- Builder monad for creating derived Dynamics with Functor/Applicative syntax.
    Carries a TimelineCtx for NodeId allocation.
    Note: uses raw Dynamic combinators (no BEq-based deduplication). -/
abbrev Builder (t : Type) [Timeline t] (a : Type) := ReaderT (TimelineCtx t) IO (Dynamic t a)

namespace Builder

variable {t : Type} [Timeline t]

/-- Run a Dynamic builder with an explicit TimelineCtx. -/
def run (ctx : TimelineCtx t) (m : Builder t a) : IO (Dynamic t a) :=
  m ctx

/-- Lift an existing Dynamic into the builder. -/
def of (d : Dynamic t a) : Builder t a :=
  fun _ => pure d

/-- Lift an IO-built Dynamic into the builder. -/
def liftIO (action : IO (Dynamic t a)) : Builder t a :=
  fun _ => action

instance : Functor (Builder t) where
  map f m := fun ctx => do
    let d ← m ctx
    Dynamic.mapRaw ctx f d

instance : Applicative (Builder t) where
  pure x := fun ctx => Dynamic.constant ctx x
  seq mf mx := fun ctx => do
    let df ← mf ctx
    let dx ← (mx ()) ctx
    Dynamic.zipWithRaw ctx (fun f x => f x) df dx

end Builder

end Dynamic

end Reactive
