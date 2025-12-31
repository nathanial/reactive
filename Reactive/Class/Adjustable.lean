/-
  Reactive/Class/Adjustable.lean

  Typeclass for monads supporting incremental/switching computation.
-/
import Reactive.Core
import Reactive.Class.MonadHold

namespace Reactive

/-- Monad supporting adjustable/incremental computation with dynamic switching.

    This is an advanced FRP capability that enables:
    - Switching between different reactive networks dynamically
    - Incremental computation where only affected parts recompute
    - Higher-order FRP patterns -/
class Adjustable (t : Type) (m : Type → Type) extends MonadHold t m where
  /-- Run a computation that produces a value and may be adjusted over time.
      Returns the initial result and an event of replacement computations. -/
  runWithReplace : m a → m (a × Event t (m a))

  /-- Traverse with adjustment capability -/
  traverseWithAdjust : (a → m b) → List a → m (List b × Event t (List b))

export Adjustable (runWithReplace traverseWithAdjust)

end Reactive
