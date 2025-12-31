/-
  Reactive/Class/MonadSample.lean

  Typeclass for monads that can sample Behaviors.
-/
import Reactive.Core

namespace Reactive

/-- Monad that can sample the current value of Behaviors.

    This is the most basic FRP capability - the ability to read
    the current value of a time-varying Behavior. -/
class MonadSample (t : Type) (m : Type → Type) where
  /-- Sample the current value of a behavior -/
  sample : Behavior t a → m a

export MonadSample (sample)

/-- Behavior is itself a MonadSample (sampling is just extracting the value) -/
instance : MonadSample t (Behavior t) where
  sample b := b

/-- IO can sample behaviors directly -/
instance : MonadSample t IO where
  sample b := b.sample

end Reactive
