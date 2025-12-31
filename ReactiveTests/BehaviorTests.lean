import Crucible
import Reactive

namespace ReactiveTests.BehaviorTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Behavior Tests"

test "Behavior.constant returns constant value" := do
  let b : Behavior Spider Nat := Behavior.constant 42
  let value ← b.sample
  shouldBe value 42

test "Behavior.map transforms values" := do
  let b : Behavior Spider Nat := Behavior.constant 10
  let mapped := Behavior.map (· * 3) b
  let value ← mapped.sample
  shouldBe value 30

test "Behavior.zipWith combines behaviors" := do
  let b1 : Behavior Spider Nat := Behavior.constant 3
  let b2 : Behavior Spider Nat := Behavior.constant 4
  let combined := Behavior.zipWith (· + ·) b1 b2
  let value ← combined.sample
  shouldBe value 7

test "Behavior Applicative works" := do
  let b1 : Behavior Spider Nat := pure 5
  let b2 : Behavior Spider Nat := pure 7
  let combined := (· + ·) <$> b1 <*> b2
  let value ← combined.sample
  shouldBe value 12

test "Behavior Monad works" := do
  let b : Behavior Spider Nat := do
    let x ← Behavior.constant 10
    let y ← Behavior.constant 20
    pure (x + y)
  let value ← b.sample
  shouldBe value 30

#generate_tests

end ReactiveTests.BehaviorTests
