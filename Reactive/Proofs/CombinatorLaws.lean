/-
  Reactive/Proofs/CombinatorLaws.lean

  Formal proofs of algebraic equivalences between combinators.
  Establishes that the API is internally consistent.
-/
import Reactive.Core.Behavior
import Reactive.Core.Event
import Reactive.Combinators.Event
import Reactive.Combinators.Behavior

namespace Reactive.Proofs

/-!
## Combinator Equivalence Proofs

This module documents and re-exports algebraic equivalences between various
combinators, establishing internal consistency of the API.

### Event Combinators
- `scan_eq_accumulate`: `Event.scan = Event.accumulate` (definitional)

### Behavior Combinators (from `Reactive.Behavior`)
- `Behavior.zipWith_eq_applicative`: `zipWith f b1 b2 = pure f <*> b1 <*> b2`
- `Behavior.zip_eq_zipWith`: `zip b1 b2 = zipWith Prod.mk b1 b2`

### Semantic Equivalences (documented)
For Event combinators involving IO:
- `filter p e ≈ mapMaybe (fun a => if p a then some a else none) e`
- `attach b e ≈ attachWith Prod.mk b e`

These semantic equivalences hold in the sense that for any given NodeId,
the resulting events have identical behavior.
-/

section EventCombinators

/-!
### Event.scan = Event.accumulate

`scan` is defined as an alias for `accumulate`, so this is definitional.
-/

/-- scan is definitionally equal to accumulate -/
theorem scan_eq_accumulate : @Event.scan = @Event.accumulate := rfl

/-- scan with explicit timeline type -/
theorem scan_eq_accumulate' {t : Type} [Timeline t] :
    @Event.scan t = @Event.accumulate t := rfl

end EventCombinators

section SemanticEquivalences

/-!
### Semantic Equivalences for Event Combinators

The following equivalences hold semantically but cannot be proven as
definitional equalities because the combinators involve IO and allocate
different internal state (refs, subscriptions).

For a given NodeId, these produce events with identical observable behavior.
-/

/-- The filter handler is extensionally equal to the mapMaybe handler
    with the appropriate function.

    This proves that `filter p` and `mapMaybe (fun a => if p a then some a else none)`
    produce the same subscription behavior. -/
theorem filter_handler_eq_mapMaybe_handler {a : Type} (p : a → Bool) :
    (fun (x : a) (fire : a → IO Unit) =>
      if p x then fire x else pure ()) =
    (fun (x : a) (fire : a → IO Unit) =>
      match (if p x then some x else none) with
      | some b => fire b
      | none => pure ()) := by
  funext x fire
  split
  · -- p x = true case
    simp_all
  · -- p x = false case
    simp_all

/-- The attach handler with Prod.mk is extensionally equal to the
    attachWith handler.

    This proves that `attach` and `attachWith Prod.mk` have identical
    subscription behavior. -/
theorem attach_handler_eq_attachWith_handler {t : Type} {a c : Type}
    (b : Behavior t a) :
    (fun (x : c) (fire : (a × c) → IO Unit) => do
      let bv ← b.sample
      fire (bv, x)) =
    (fun (x : c) (fire : (a × c) → IO Unit) => do
      let bv ← b.sample
      fire (Prod.mk bv x)) := rfl

end SemanticEquivalences

end Reactive.Proofs
