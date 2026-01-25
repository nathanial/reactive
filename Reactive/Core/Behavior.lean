/-
  Reactive/Core/Behavior.lean

  Behavior type representing time-varying values that can be sampled at any time.
  Behaviors are pull-based - values are computed on demand.
-/
import Reactive.Core.Types
import Reactive.Core.Event

namespace Reactive

/-- A Behavior represents a time-varying value that can be sampled at any time.
    Behaviors are parameterized by:
    - `t`: The timeline (phantom type for type-safe separation)
    - `a`: The type of values the behavior holds

    Unlike Events which are push-based, Behaviors are pull-based:
    you can sample their current value at any time. -/
structure Behavior (t : Type) (a : Type) where
  private mk ::
  /-- Sample the current value. This is an IO action because the value
      may depend on mutable state (e.g., held event values). -/
  private sampleIO : IO a

namespace Behavior

/-- Create a constant behavior that always returns the same value -/
def constant (x : a) : Behavior t a :=
  ⟨pure x⟩

/-- Sample the current value of a behavior -/
def sample (b : Behavior t a) : IO a :=
  b.sampleIO

/-- Create a behavior from a sampling function -/
def fromSample (f : IO a) : Behavior t a :=
  ⟨f⟩

/-- Map a function over a behavior's values -/
def map (f : a → b) (ba : Behavior t a) : Behavior t b :=
  ⟨f <$> ba.sampleIO⟩

/-- Applicative pure - constant behavior -/
def pure (x : a) : Behavior t a :=
  constant x

/-- Applicative apply - combine behaviors -/
def ap (bf : Behavior t (a → b)) (ba : Behavior t a) : Behavior t b :=
  ⟨do
    let f ← bf.sampleIO
    let a ← ba.sampleIO
    Pure.pure (f a)⟩

/-- Monadic bind - behavior that depends on another behavior's value -/
def bind (ba : Behavior t a) (f : a → Behavior t b) : Behavior t b :=
  ⟨do
    let a ← ba.sampleIO
    (f a).sampleIO⟩

/-- Combine two behaviors with a function -/
def zipWith (f : a → b → c) (ba : Behavior t a) (bb : Behavior t b) : Behavior t c :=
  ⟨do
    let a ← ba.sampleIO
    let b ← bb.sampleIO
    Pure.pure (f a b)⟩

/-- Pair two behaviors -/
def zip (ba : Behavior t a) (bb : Behavior t b) : Behavior t (a × b) :=
  zipWith Prod.mk ba bb

/-- Create a behavior that holds the most recent value from an event.
    Starts with the initial value and updates whenever the event fires.

    This is a pure alternative to `MonadHold.hold` when you only need
    sampling capability without the `Dynamic`'s update event.

    Example:
    ```
    let (clickEvent, fireClick) ← Event.newTrigger nodeId
    let clickCount ← Behavior.hold 0 clickEvent
    -- clickCount.sample returns the most recent value
    ```
-/
def hold [Timeline t] (initial : a) (event : Event t a) : IO (Behavior t a) := do
  let valueRef ← IO.mkRef initial
  let _ ← Reactive.Event.subscribe event fun a => valueRef.set a
  Pure.pure (Behavior.fromSample valueRef.get)

/-- Alias for `hold`. Common name in other FRP libraries (reactive-banana, sodium). -/
abbrev stepper := @hold

/-- Create a behavior by folding over event occurrences.
    Each event value is combined with the current state using the function.

    Example:
    ```
    let (addEvent, fireAdd) ← Event.newTrigger nodeId
    let total ← Behavior.foldB (· + ·) 0 addEvent
    -- Each fire of addEvent adds to the running total
    ```
-/
def foldB [Timeline t] (f : a → b → b) (initial : b) (event : Event t a) : IO (Behavior t b) := do
  let valueRef ← IO.mkRef initial
  let _ ← Reactive.Event.subscribe event fun a => do
    let old ← valueRef.get
    valueRef.set (f a old)
  Pure.pure (Behavior.fromSample valueRef.get)

/-- Sample from a nested behavior (Behavior of Behaviors).
    Allows dynamic behavior switching based on another behavior's value.

    When sampled, first samples the outer behavior to get the inner behavior,
    then samples that inner behavior.

    Example:
    ```
    let mode : Behavior t Mode := ...
    let behaviors : Behavior t (Behavior t Int) := mode.map fun m =>
      match m with
      | .fast => fastBehavior
      | .slow => slowBehavior
    let current ← Behavior.switch behaviors
    -- current samples whichever behavior mode currently selects
    ```
-/
def switch (bb : Behavior t (Behavior t a)) : Behavior t a :=
  ⟨do
    let b ← bb.sampleIO
    b.sampleIO⟩

/-- Alias for switch (monadic join). -/
abbrev join := @switch

end Behavior

instance : Functor (Behavior t) where
  map := Behavior.map

instance : Pure (Behavior t) where
  pure := Behavior.pure

instance : Seq (Behavior t) where
  seq bf ba := Behavior.ap bf (ba ())

instance : Applicative (Behavior t) where

instance : Bind (Behavior t) where
  bind := Behavior.bind

instance : Monad (Behavior t) where

end Reactive
