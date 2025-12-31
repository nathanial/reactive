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
    Higher nodes depend on lower nodes. Processing in height order prevents glitches. -/
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

/-- Propagation state during event firing -/
structure PropagationState where
  currentFrame : Frame
  nextNodeId : Nat
  nextSubscriberId : Nat
  deriving Inhabited

end Reactive
