/-
  Reactive/Proofs/BehaviorLaws.lean

  Re-exports the formal proofs that Behavior satisfies the Functor and Monad laws.

  The proofs are defined in Reactive.Core.Behavior where access to private
  fields is available. This module provides a convenient namespace for
  accessing them.
-/
import Reactive.Core.Behavior

namespace Reactive.Proofs

/-!
## Behavior Law Proofs

The following proofs are available from `Reactive.Behavior`:

### Functor Laws
- `Behavior.map_id`: `map id b = b`
- `Behavior.map_comp`: `map (f ∘ g) b = map f (map g b)`

### Monad Laws
- `Behavior.pure_bind`: `pure a >>= f = f a`
- `Behavior.bind_pure`: `b >>= pure = b`
- `Behavior.bind_assoc`: `(b >>= f) >>= g = b >>= (fun a => f a >>= g)`
- `Behavior.map_eq_pure_bind`: `map f b = b >>= (fun a => pure (f a))`

### Lawful Typeclass Instances
- `LawfulFunctor (Behavior t)` - satisfies `id_map` and `comp_map`
- `LawfulApplicative (Behavior t)` - satisfies `pure_seq`, `map_pure`, `seq_pure`, `seq_assoc`
- `LawfulMonad (Behavior t)` - satisfies `pure_bind`, `bind_assoc`, `bind_pure_comp`, `bind_map`

### IO Axioms Used
The proofs rely on axioms asserting IO satisfies standard monad laws:
- `IO.map_id`, `IO.map_comp` - Functor laws
- `IO.pure_bind`, `IO.bind_pure`, `IO.bind_assoc` - Monad laws
- `IO.map_eq_pure_bind` - Applicative/Monad coherence

These are reasonable assumptions since IO is implemented to satisfy these laws.
-/

end Reactive.Proofs
