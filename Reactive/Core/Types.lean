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

/-- Evidence of operating within a specific timeline's host context.

    This type enforces type-safe timeline separation: you can only create events
    on a timeline if you have the corresponding `TimelineCtx`. The host monad
    (e.g., SpiderM) provides this context, preventing accidental creation of
    events outside the proper infrastructure.

    The private constructor ensures only the host implementation can create contexts. -/
structure TimelineCtx (t : Type) [Timeline t] where
  private mk ::
  /-- Node ID generator for this context -/
  nodeIdGen : IO.Ref Nat

namespace TimelineCtx

/-- Create a new timeline context. Internal use only - called by host implementations. -/
protected def new [Timeline t] : IO (TimelineCtx t) := do
  let gen ← IO.mkRef 0
  pure ⟨gen⟩

/-- Generate a fresh NodeId within this context -/
def freshNodeId [Timeline t] (ctx : TimelineCtx t) : IO NodeId := do
  ctx.nodeIdGen.modifyGet fun n => (NodeId.mk n, n + 1)

end TimelineCtx

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
in height order within each frame.

Implementation uses a binary min-heap for O(log n) insert and pop operations. -/

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
    Uses a binary min-heap for efficient priority queue operations. -/
structure PropagationQueue where
  /-- Binary min-heap of pending fires, ordered by (height, nodeId) -/
  pending : IO.Ref (Array PendingFire)
  /-- Pending fires for the next frame (used by delayFrame) -/
  nextFramePending : IO.Ref (Array PendingFire)
  /-- Whether we're currently inside a propagation frame -/
  inFrame : IO.Ref Bool

namespace PropagationQueue

/-! ### Binary Heap Operations

Array-based binary min-heap where:
- Parent of node i: (i - 1) / 2
- Left child of i: 2*i + 1
- Right child of i: 2*i + 2

Heap property: parent ≤ children (for all nodes) -/

/-- Get parent index -/
@[inline] private def parentIdx (i : Nat) : Nat := (i - 1) / 2

/-- Get left child index -/
@[inline] private def leftChildIdx (i : Nat) : Nat := 2 * i + 1

/-- Get right child index -/
@[inline] private def rightChildIdx (i : Nat) : Nat := 2 * i + 2

/-- Swap two elements in an array (unchecked for performance) -/
@[inline] private def swap (arr : Array PendingFire) (i j : Nat) : Array PendingFire :=
  let vi := arr[i]!
  let vj := arr[j]!
  (arr.set! i vj).set! j vi

/-- Sift up: restore heap property after inserting at end.
    Bubbles the element at index i up until heap property is satisfied.
    O(log n) -/
partial def siftUp (arr : Array PendingFire) (i : Nat) : Array PendingFire :=
  if i == 0 then arr
  else
    let pi := parentIdx i
    if pi < arr.size && i < arr.size then
      if compare arr[i]! arr[pi]! == .lt then
        siftUp (swap arr i pi) pi
      else arr
    else arr

/-- Sift down: restore heap property after replacing root.
    Bubbles the element at index i down until heap property is satisfied.
    O(log n) -/
partial def siftDown (arr : Array PendingFire) (i : Nat) : Array PendingFire :=
  let left := leftChildIdx i
  let right := rightChildIdx i
  let size := arr.size

  -- Find smallest among node and its children
  let smallest :=
    let s1 := if left < size && compare arr[left]! arr[i]! == .lt then left else i
    if right < size && compare arr[right]! arr[s1]! == .lt then right else s1

  -- If smallest is not the current node, swap and continue
  if smallest != i && smallest < size then
    siftDown (swap arr i smallest) smallest
  else
    arr

/-- Insert a pending fire into a heap array. O(log n) -/
@[inline] private def heapInsert (arr : Array PendingFire) (p : PendingFire) : Array PendingFire :=
  let arr := arr.push p
  siftUp arr (arr.size - 1)

/-- Pop the minimum (lowest height) pending fire from a heap array. O(log n) -/
@[inline] private def heapPopMin? (arr : Array PendingFire) : Option (PendingFire × Array PendingFire) :=
  if arr.size == 0 then
    none
  else
    let minElem := arr[0]!
    if arr.size == 1 then
      some (minElem, #[])
    else
      let last := arr.back!
      let arr := arr.pop
      let arr := arr.set! 0 last
      let arr := siftDown arr 0
      some (minElem, arr)

/-- Create a new empty propagation queue. -/
def new : IO PropagationQueue := do
  let pending ← IO.mkRef #[]
  let nextFramePending ← IO.mkRef #[]
  let inFrame ← IO.mkRef false
  pure { pending, nextFramePending, inFrame }

/-- Check if the queue is currently inside a frame. -/
@[inline] def isInFrame (q : PropagationQueue) : IO Bool :=
  q.inFrame.get

/-- Set the in-frame flag. -/
@[inline] def setInFrame (q : PropagationQueue) (value : Bool) : IO Unit :=
  q.inFrame.set value

/-- Insert a pending fire into the current frame heap. -/
@[inline] def insert (q : PropagationQueue) (p : PendingFire) : IO Unit := do
  q.pending.modify fun arr => heapInsert arr p

/-- Insert a pending fire into the next-frame heap. -/
@[inline] def insertNextFrame (q : PropagationQueue) (p : PendingFire) : IO Unit := do
  q.nextFramePending.modify fun arr => heapInsert arr p

/-- Pop the minimum (lowest height) pending fire from the current frame. -/
def popMin? (q : PropagationQueue) : IO (Option PendingFire) := do
  let result ← q.pending.modifyGet fun arr =>
    match heapPopMin? arr with
    | none => (none, arr)
    | some (minElem, arr') => (some minElem, arr')
  pure result

/-- Check if the current frame heap is empty. -/
@[inline] def isEmpty (q : PropagationQueue) : IO Bool := do
  let arr ← q.pending.get
  pure arr.isEmpty

/-- Move next-frame pending fires into the current frame.
    Returns true if a new frame was started. -/
def startNextFrame (q : PropagationQueue) : IO Bool := do
  let next ← q.nextFramePending.get
  if next.isEmpty then
    pure false
  else
    q.pending.set next
    q.nextFramePending.set #[]
    pure true

end PropagationQueue

end Reactive
