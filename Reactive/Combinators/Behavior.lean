/-
  Reactive/Combinators/Behavior.lean

  Combinators for working with Behaviors.
-/
import Reactive.Core

namespace Reactive

namespace Behavior

/-- Apply a behavior of functions to a behavior of values -/
def apply (bf : Behavior t (a → b)) (ba : Behavior t a) : Behavior t b :=
  ap bf ba

/-- Combine three behaviors -/
def zipWith3 (f : a → b → c → d) (ba : Behavior t a) (bb : Behavior t b) (bc : Behavior t c)
    : Behavior t d :=
  Behavior.fromSample do
    let va ← ba.sample
    let vb ← bb.sample
    let vc ← bc.sample
    Pure.pure (f va vb vc)

/-- Combine four behaviors -/
def zipWith4 (f : a → b → c → d → e) (ba : Behavior t a) (bb : Behavior t b)
    (bc : Behavior t c) (bd : Behavior t d) : Behavior t e :=
  Behavior.fromSample do
    let va ← ba.sample
    let vb ← bb.sample
    let vc ← bc.sample
    let vd ← bd.sample
    Pure.pure (f va vb vc vd)

/-- Behavior that is true when all input behaviors are true -/
def allTrue (bs : List (Behavior t Bool)) : Behavior t Bool :=
  Behavior.fromSample do
    let values ← bs.mapM (·.sample)
    Pure.pure (values.all id)

/-- Behavior that is true when any input behavior is true -/
def anyTrue (bs : List (Behavior t Bool)) : Behavior t Bool :=
  Behavior.fromSample do
    let values ← bs.mapM (·.sample)
    Pure.pure (values.any id)

/-- Negate a boolean behavior -/
def not (b : Behavior t Bool) : Behavior t Bool :=
  Behavior.map Bool.not b

/-- Boolean AND of two behaviors -/
def and (b1 : Behavior t Bool) (b2 : Behavior t Bool) : Behavior t Bool :=
  zipWith (· && ·) b1 b2

/-- Boolean OR of two behaviors -/
def or (b1 : Behavior t Bool) (b2 : Behavior t Bool) : Behavior t Bool :=
  zipWith (· || ·) b1 b2

end Behavior

end Reactive
