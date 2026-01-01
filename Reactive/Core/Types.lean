/-
  Reactive/Core/Types.lean

  Core type definitions for the Reactive FRP library.
  Defines the Timeline phantom type and related primitives.
-/

namespace Reactive

/-- Phantom type for timeline/host identification.
    Different timelines represent different reactive networks that cannot interact. -/
class Timeline (t : Type) where

/-- Subscriber identifier for managing subscriptions -/
structure SubscriberId where
  id : Nat
  deriving BEq, Hashable, Repr, Inhabited

/-- Node identifier for tracking nodes in the reactive graph -/
structure NodeId where
  id : Nat
  deriving BEq, Hashable, Repr, Inhabited, Ord

/-- Height in the dependency graph for topological ordering.
    Higher nodes depend on lower nodes. Processing in height order prevents glitches.

    Events are queued by (height, nodeId) in the propagation queue and processed
    in ascending order. This ensures all lower-height events fire before higher-height
    ones, preventing glitches where derived nodes see inconsistent intermediate states. -/
structure Height where
  value : Nat := 0
  deriving BEq, Repr, Inhabited, Ord

instance : LE Height where
  le a b := a.value ≤ b.value

instance : LT Height where
  lt a b := a.value < b.value

instance : Max Height where
  max a b := ⟨Nat.max a.value b.value⟩

instance : HAdd Height Nat Height where
  hAdd h n := ⟨h.value + n⟩

/-- Increment height by 1 -/
def Height.inc (h : Height) : Height := ⟨h.value + 1⟩

/-- Frame represents a single propagation cycle.
    All events fired in the same frame are considered simultaneous. -/
structure Frame where
  number : Nat
  deriving BEq, Repr, Inhabited

/-! ## Propagation Queue Infrastructure

The propagation queue enables glitch-free event handling by processing events
in height order within each frame. -/

/-- A pending event occurrence waiting to be propagated.
    Stores the height and nodeId for ordering, plus the fire action as a closure. -/
structure PendingFire where
  height : Height
  nodeId : NodeId
  fire : IO Unit
  deriving Inhabited

/-- Compare pending fires for priority queue ordering.
    Lower height fires first; ties broken by nodeId for determinism. -/
instance : Ord PendingFire where
  compare a b :=
    match compare a.height b.height with
    | .eq => compare a.nodeId b.nodeId
    | other => other

instance : LT PendingFire where
  lt a b := compare a b == .lt

instance : LE PendingFire where
  le a b := compare a b != .gt

/-- Propagation state during a frame.
    The pending array is kept sorted by (height, nodeId). -/
structure PropagationQueue where
  /-- Priority queue of pending fires, ordered by (height, nodeId) -/
  pending : Array PendingFire := #[]
  /-- Pending fires for the next frame (used by delayFrame) -/
  nextFramePending : Array PendingFire := #[]
  /-- Whether we're currently inside a propagation frame -/
  inFrame : Bool := false
  deriving Inhabited

namespace PropagationQueue

/-- Find insertion index for a pending fire in sorted array.
    Uses stable insertion: equal elements are inserted after existing ones (FIFO). -/
private def findInsertIdx (arr : Array PendingFire) (p : PendingFire) : Nat :=
  -- Linear search for simplicity; could optimize to binary search later
  -- Use != .gt (i.e., ≤) for stable insertion
  arr.foldl (init := 0) fun idx existing =>
    if compare existing p != .gt then idx + 1 else idx

/-- Insert a pending fire into the sorted queue -/
def insert (q : PropagationQueue) (p : PendingFire) : PropagationQueue :=
  let idx := findInsertIdx q.pending p
  -- Build new array with element inserted at idx
  let before := q.pending.extract 0 idx
  let after := q.pending.extract idx q.pending.size
  { q with pending := before.push p ++ after }

/-- Pop the minimum (lowest height) pending fire -/
def popMin? (q : PropagationQueue) : Option (PendingFire × PropagationQueue) :=
  if h : 0 < q.pending.size then
    some (q.pending[0], { q with pending := q.pending.eraseIdx 0 })
  else
    none

/-- Check if the queue is empty -/
def isEmpty (q : PropagationQueue) : Bool := q.pending.isEmpty

end PropagationQueue

end Reactive
