/-
  Reactive/Host/Spider/Behavior.lean

  Behavior SpiderM combinators for the Spider FRP runtime.
-/
import Reactive.Host.Spider.Core

namespace Reactive.Host

/-! ## Behavior SpiderM Combinators

These provide ergonomic versions of Behavior operations that register
subscriptions with the current scope for automatic cleanup. -/

namespace Behavior

/-- Create a behavior that holds the most recent value from an event.
    Registers subscription with current scope for automatic cleanup.

    This is a SpiderM alternative to `MonadHold.hold` when you only need
    sampling capability without the `Dynamic`'s update event. -/
def holdM (initial : a) (event : Event Spider a) : SpiderM (Behavior Spider a) := ⟨fun env => do
  let valueRef ← IO.mkRef initial
  let unsub ← Reactive.Event.subscribe event fun a => valueRef.set a
  env.currentScope.register unsub
  pure (Behavior.fromSample valueRef.get)⟩

/-- Alias for `holdM`. Common name in other FRP libraries (reactive-banana, sodium). -/
abbrev stepperM := @holdM

/-- Create a behavior by folding over event occurrences.
    Registers subscription with current scope for automatic cleanup. -/
def foldBM (f : a → b → b) (initial : b) (event : Event Spider a) : SpiderM (Behavior Spider b) := ⟨fun env => do
  let valueRef ← IO.mkRef initial
  let unsub ← Reactive.Event.subscribe event fun a => do
    let old ← valueRef.get
    valueRef.set (f a old)
  env.currentScope.register unsub
  pure (Behavior.fromSample valueRef.get)⟩

end Behavior

end Reactive.Host
