/-
  Reactive/Class/MonadHold.lean

  Typeclass for monads that can create Behaviors/Dynamics from Events.
-/
import Reactive.Core
import Reactive.Class.MonadSample

namespace Reactive

/-- Monad that can hold values to create Behaviors and Dynamics from Events.

    This is a key FRP capability - the ability to maintain state by
    "holding" the most recent value from an event stream. -/
class MonadHold (t : Type) (m : Type → Type) extends MonadSample t m where
  /-- Hold the most recent value from an event, starting with an initial value.
      Returns a Behavior that always has the latest value. -/
  hold : a → Event t a → m (Behavior t a)

  /-- Like hold but returns a Dynamic with change events -/
  holdDyn : a → Event t a → m (Dynamic t a)

  /-- Fold over event occurrences to accumulate state -/
  foldDyn : (a → b → b) → b → Event t a → m (Dynamic t b)

  /-- Accumulate with monadic action -/
  foldDynM : [Monad m] → (a → b → m b) → b → Event t a → m (Dynamic t b)

export MonadHold (hold holdDyn foldDyn foldDynM)

/-- Helper: count event occurrences -/
def count [MonadHold t m] [Monad m] (event : Event t a) : m (Dynamic t Nat) :=
  foldDyn (fun _ n => n + 1) 0 event

/-- Helper: collect event values into a list -/
def collect [MonadHold t m] [Monad m] (event : Event t a) : m (Dynamic t (List a)) :=
  foldDyn (fun a xs => a :: xs) [] event

end Reactive
