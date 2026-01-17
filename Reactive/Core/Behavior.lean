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

/-!
## Formal Proofs of Functor and Monad Laws

Since IO is a primitive type without LawfulFunctor/LawfulMonad instances,
we introduce axioms for IO's laws and use them to prove Behavior's laws.
-/

section Laws

-- Functor laws for IO (axioms - IO is expected to satisfy these)
axiom IO.map_id : ∀ {α : Type} (x : IO α), id <$> x = x
axiom IO.map_comp : ∀ {α β γ : Type} (f : β → γ) (g : α → β) (x : IO α),
  (f ∘ g) <$> x = f <$> (g <$> x)

-- Monad laws for IO (axioms)
axiom IO.pure_bind : ∀ {α β : Type} (a : α) (f : α → IO β),
  _root_.Pure.pure a >>= f = f a
axiom IO.bind_pure : ∀ {α : Type} (x : IO α),
  x >>= _root_.Pure.pure = x
axiom IO.bind_assoc : ∀ {α β γ : Type} (x : IO α) (f : α → IO β) (g : β → IO γ),
  (x >>= f) >>= g = x >>= (fun a => f a >>= g)

-- Applicative/Monad coherence for IO
axiom IO.map_eq_pure_bind : ∀ {α β : Type} (f : α → β) (x : IO α),
  f <$> x = x >>= (fun a => _root_.Pure.pure (f a))
axiom IO.seq_eq_bind : ∀ {α β : Type} (mf : IO (α → β)) (ma : IO α),
  mf <*> ma = mf >>= (fun f => ma >>= (fun a => _root_.Pure.pure (f a)))

-- Bridge lemmas: EST.pure/EST.bind are the implementations of Pure.pure/Bind.bind for IO
-- These are definitionally equal but stated explicitly for simp
theorem IO.EST_pure_eq_pure : ∀ {α : Type} (a : α), (EST.pure a : IO α) = _root_.Pure.pure a := fun _ => rfl
theorem IO.EST_bind_eq_bind : ∀ {α β : Type} (x : IO α) (f : α → IO β), EST.bind x f = x >>= f := fun _ _ => rfl

-- Monad laws restated for EST operations (for simp to use with do-notation)
theorem IO.EST_pure_bind : ∀ {α β : Type} (a : α) (f : α → IO β),
    EST.bind (EST.pure a) f = f a := fun a f => IO.pure_bind a f
theorem IO.EST_bind_pure : ∀ {α : Type} (x : IO α),
    EST.bind x EST.pure = x := fun x => IO.bind_pure x
theorem IO.EST_bind_assoc : ∀ {α β γ : Type} (x : IO α) (f : α → IO β) (g : β → IO γ),
    EST.bind (EST.bind x f) g = EST.bind x (fun a => EST.bind (f a) g) := fun x f g => IO.bind_assoc x f g

namespace Behavior

/-- Extensionality: two behaviors are equal iff their sample IO actions are equal -/
@[ext]
theorem ext {t : Type} {α : Type} (b1 b2 : Behavior t α)
    (h : b1.sampleIO = b2.sampleIO) : b1 = b2 := by
  cases b1; cases b2
  simp only [mk.injEq]
  exact h

/-- Helper: sampleIO of map -/
theorem sampleIO_map {t : Type} {α β : Type} (f : α → β) (b : Behavior t α) :
    (Behavior.map f b).sampleIO = (f <$> b.sampleIO) := rfl

/-- Helper: sampleIO of pure -/
theorem sampleIO_pure {t : Type} {α : Type} (a : α) :
    (Behavior.pure a : Behavior t α).sampleIO = _root_.Pure.pure a := rfl

/-- Helper: sampleIO of bind -/
theorem sampleIO_bind {t : Type} {α β : Type} (b : Behavior t α) (f : α → Behavior t β) :
    (Behavior.bind b f).sampleIO = (b.sampleIO >>= fun a => (f a).sampleIO) := rfl

/-- Helper: sampleIO of ap -/
theorem sampleIO_ap {t : Type} {α β : Type} (bf : Behavior t (α → β)) (ba : Behavior t α) :
    (Behavior.ap bf ba).sampleIO = (bf.sampleIO >>= fun f => ba.sampleIO >>= fun a => _root_.Pure.pure (f a)) := rfl

/-! ### Functor Laws -/

/-- Functor identity law: mapping id is the identity function -/
theorem map_id {t : Type} {α : Type} (b : Behavior t α) :
    Behavior.map id b = b := by
  ext
  simp only [sampleIO_map]
  exact IO.map_id b.sampleIO

/-- Functor composition law: mapping a composition equals composing maps -/
theorem map_comp {t : Type} {α β γ : Type} (f : β → γ) (g : α → β) (b : Behavior t α) :
    Behavior.map (f ∘ g) b = Behavior.map f (Behavior.map g b) := by
  ext
  simp only [sampleIO_map]
  exact IO.map_comp f g b.sampleIO

/-! ### Monad Laws -/

/-- Monad left identity: pure a >>= f = f a -/
theorem pure_bind {t : Type} {α β : Type} (a : α) (f : α → Behavior t β) :
    Behavior.bind (Behavior.pure a) f = f a := by
  ext
  simp only [sampleIO_bind, sampleIO_pure]
  exact IO.pure_bind a (fun x => (f x).sampleIO)

/-- Monad right identity: m >>= pure = m -/
theorem bind_pure {t : Type} {α : Type} (b : Behavior t α) :
    Behavior.bind b Behavior.pure = b := by
  ext
  simp only [sampleIO_bind, sampleIO_pure]
  exact IO.bind_pure b.sampleIO

/-- Monad associativity: (m >>= f) >>= g = m >>= (λ x → f x >>= g) -/
theorem bind_assoc {t : Type} {α β γ : Type}
    (b : Behavior t α) (f : α → Behavior t β) (g : β → Behavior t γ) :
    Behavior.bind (Behavior.bind b f) g = Behavior.bind b (fun a => Behavior.bind (f a) g) := by
  ext
  simp only [sampleIO_bind]
  exact IO.bind_assoc b.sampleIO (fun a => (f a).sampleIO) (fun b => (g b).sampleIO)

/-- map equals pure-bind form -/
theorem map_eq_pure_bind {t : Type} {α β : Type} (f : α → β) (b : Behavior t α) :
    Behavior.map f b = Behavior.bind b (fun a => Behavior.pure (f a)) := by
  ext
  simp only [sampleIO_map, sampleIO_bind, sampleIO_pure]
  exact IO.map_eq_pure_bind f b.sampleIO

end Behavior

/-! ### LawfulFunctor, LawfulApplicative, LawfulMonad Instances -/

instance {t : Type} : LawfulFunctor (Behavior t) where
  map_const := by intros; rfl
  id_map := Behavior.map_id
  comp_map := by
    intros α β γ g h b
    show Behavior.map (h ∘ g) b = Behavior.map h (Behavior.map g b)
    exact Behavior.map_comp h g b

instance {t : Type} : LawfulApplicative (Behavior t) where
  seqLeft_eq := by intros; ext; rfl
  seqRight_eq := by intros; ext; rfl
  pure_seq := by
    intros α β f b
    apply Behavior.ext
    simp only [Seq.seq, Behavior.ap, Pure.pure, Behavior.pure, Behavior.constant]
    exact IO.pure_bind f (fun f' => f' <$> b.sampleIO)
  map_pure := by intros; rfl
  seq_pure := by
    intros α β bf a
    apply Behavior.ext
    simp only [Seq.seq, Functor.map, Behavior.ap, Pure.pure, Behavior.pure,
               Behavior.constant, Behavior.map]
    -- LHS: do let f ← bf.sampleIO; let _ ← pure a; pure (f a)
    -- RHS: (· a) <$> bf.sampleIO
    -- Both are propositionally equal via IO axioms
    have h : ∀ (f : α → β), (Pure.pure a >>= fun _ => Pure.pure (f a) : IO β) = Pure.pure (f a) :=
      fun f => IO.pure_bind a _
    calc (bf.sampleIO >>= fun f => Pure.pure a >>= fun _ => Pure.pure (f a))
        = (bf.sampleIO >>= fun f => Pure.pure (f a)) := by simp only [h]
      _ = ((· a) <$> bf.sampleIO) := (IO.map_eq_pure_bind _ _).symm
  seq_assoc := by
    intros α β γ x g f
    apply Behavior.ext
    simp only [Seq.seq, Functor.map, Behavior.ap, Behavior.map]
    -- Need an axiom to unfold EST.bind/EST.pure to standard bind/pure
    -- EST.bind m (EST.pure ∘ f) = f <$> m
    have unfold_map : ∀ {α β : Type} (m : IO α) (g : α → β),
        EST.bind m (EST.pure ∘ g) = g <$> m := fun m g => rfl
    simp only [unfold_map, IO.map_eq_pure_bind, IO.bind_assoc, IO.pure_bind, Function.comp_apply]

instance {t : Type} : LawfulMonad (Behavior t) where
  bind_pure_comp := by
    intros α β f b
    apply Behavior.ext
    simp only [Bind.bind, Functor.map, Behavior.bind, Behavior.map, Pure.pure,
               Behavior.pure, Behavior.constant]
    exact (IO.map_eq_pure_bind f b.sampleIO).symm
  bind_map := by
    intros α β mf mx
    apply Behavior.ext
    simp only [Bind.bind, Seq.seq, Behavior.bind, Behavior.ap]
    rfl
  pure_bind := Behavior.pure_bind
  bind_assoc := Behavior.bind_assoc

/-! ### Combinator Equivalences -/

namespace Behavior

/-- Helper: sampleIO of zipWith -/
theorem sampleIO_zipWith {t : Type} {α β γ : Type}
    (f : α → β → γ) (b1 : Behavior t α) (b2 : Behavior t β) :
    (Behavior.zipWith f b1 b2).sampleIO =
      (b1.sampleIO >>= fun a => b2.sampleIO >>= fun b => _root_.Pure.pure (f a b)) := rfl

/-- zipWith is equivalent to applicative style: zipWith f b1 b2 = pure f <*> b1 <*> b2

This shows that the zipWith combinator is internally consistent with
Behavior's Applicative instance. -/
theorem zipWith_eq_applicative {t : Type} {α β γ : Type}
    (f : α → β → γ) (b1 : Behavior t α) (b2 : Behavior t β) :
    Behavior.zipWith f b1 b2 = Behavior.pure f <*> b1 <*> b2 := by
  apply Behavior.ext
  show (Behavior.zipWith f b1 b2).sampleIO = (Behavior.pure f <*> b1 <*> b2).sampleIO
  simp only [Behavior.zipWith, Behavior.fromSample]
  simp only [Seq.seq, Behavior.ap, Pure.pure, Behavior.pure, Behavior.constant]
  -- Goal is now:
  -- LHS: EST.bind b1.sampleIO (fun a => EST.bind b2.sampleIO (fun b => EST.pure (f a b)))
  -- RHS: EST.bind (EST.bind (EST.pure f) (fun f' => EST.bind b1.sampleIO (fun a => EST.pure (f' a))))
  --              (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b)))
  -- Step 1: Apply pure_bind to innermost (EST.pure f)
  have step1 : EST.bind (EST.pure f) (fun f' => EST.bind b1.sampleIO (fun a => EST.pure (f' a))) =
               EST.bind b1.sampleIO (fun a => EST.pure (f a)) := IO.EST_pure_bind f _
  -- Step 2: Apply bind_assoc
  have step2 : EST.bind (EST.bind b1.sampleIO (fun a => EST.pure (f a)))
                        (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b))) =
               EST.bind b1.sampleIO (fun a => EST.bind (EST.pure (f a))
                        (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b)))) :=
    IO.EST_bind_assoc _ _ _
  -- Step 3: Apply pure_bind to inner (EST.pure (f a))
  have step3 : ∀ a, EST.bind (EST.pure (f a)) (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b))) =
                    EST.bind b2.sampleIO (fun b => EST.pure (f a b)) :=
    fun a => IO.EST_pure_bind (f a) _
  -- Combine
  calc EST.bind b1.sampleIO (fun a => EST.bind b2.sampleIO (fun b => EST.pure (f a b)))
      = EST.bind b1.sampleIO (fun a => EST.bind b2.sampleIO (fun b => EST.pure (f a b))) := rfl
    _ = EST.bind b1.sampleIO (fun a => EST.bind (EST.pure (f a)) (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b)))) := by simp only [step3]
    _ = EST.bind (EST.bind b1.sampleIO (fun a => EST.pure (f a))) (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b))) := step2.symm
    _ = EST.bind (EST.bind (EST.pure f) (fun f' => EST.bind b1.sampleIO (fun a => EST.pure (f' a)))) (fun fa => EST.bind b2.sampleIO (fun b => EST.pure (fa b))) := by rw [step1]

/-- zip is equivalent to zipWith Prod.mk -/
theorem zip_eq_zipWith {t : Type} {α β : Type}
    (b1 : Behavior t α) (b2 : Behavior t β) :
    Behavior.zip b1 b2 = Behavior.zipWith Prod.mk b1 b2 := rfl

end Behavior

end Laws

end Reactive
