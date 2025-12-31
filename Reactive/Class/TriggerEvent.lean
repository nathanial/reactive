/-
  Reactive/Class/TriggerEvent.lean

  Typeclass for monads that can create triggerable external events.
-/
import Reactive.Core

namespace Reactive

/-- Monad that can create events that can be triggered from outside the FRP network.

    This is essential for integrating with external event sources like:
    - User input (keyboard, mouse)
    - Network events
    - Timers
    - Other IO operations -/
class TriggerEvent (t : Type) (m : Type → Type) where
  /-- Create a new event with a trigger function.
      The returned function can be called to fire the event. -/
  newTriggerEvent : m (Event t a × (a → IO Unit))

  /-- Create an event and immediately set up a trigger callback -/
  newEventWithTrigger : ((a → IO Unit) → IO Unit) → m (Event t a)

export TriggerEvent (newTriggerEvent newEventWithTrigger)

/-- Create an event triggered by an IO action that returns Option -/
def triggerEventFromOption [TriggerEvent t m] [Monad m] [MonadLiftT IO m]
    (poll : IO (Option a)) : m (Event t a × IO Unit) := do
  let (event, trigger) ← newTriggerEvent
  let pollAction := do
    match ← poll with
    | some a => trigger a
    | none => pure ()
  pure (event, pollAction)

end Reactive
