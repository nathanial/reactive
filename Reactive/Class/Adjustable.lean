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
    - Higher-order FRP patterns

    Based on Reflex's Adjustable typeclass design. -/
class Adjustable (t : Type) (m : Type → Type) extends MonadHold t m where
  /-- Run initial computation, switch to replacements when event fires.
      Returns the initial result and an event carrying replacement results.

      When the replacement event fires with a new computation, that computation
      is executed and its result is fired on the returned event. -/
  runWithReplace : m a → Event t (m a) → m (a × Event t a)

  /-- Traverse a list with monadic computation.
      Returns initial results and an event for future updates. -/
  traverseWithAdjust : (a → m b) → List a → m (List b × Event t (List b))

export Adjustable (runWithReplace traverseWithAdjust)

end Reactive
