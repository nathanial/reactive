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

-- New tests for full coverage

test "Behavior.hold creates behavior from event" := do
  let result ← runSpider do
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let behavior ← Behavior.hold 0 event
    fire 42
    behavior.sample
  shouldBe result 42

test "Behavior.hold updates on each event fire" := do
  let result ← runSpider do
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let behavior ← Behavior.hold 0 event
    fire 10
    fire 20
    fire 30
    behavior.sample
  shouldBe result 30

test "Behavior.foldB accumulates event values" := do
  let result ← runSpider do
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let behavior ← Behavior.foldB (· + ·) 0 event
    fire 10
    fire 20
    fire 5
    behavior.sample
  shouldBe result 35

test "Behavior.holdM registers with scope" := do
  let result ← runSpider do
    let scope ← SpiderM.getScope
    let countBefore ← scope.subscriptionCount
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← Behavior.holdM 0 event
    let countAfter ← scope.subscriptionCount
    pure (decide (countAfter > countBefore))
  shouldBe result true

test "Behavior.foldBM registers with scope" := do
  let result ← runSpider do
    let scope ← SpiderM.getScope
    let countBefore ← scope.subscriptionCount
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← Behavior.foldBM (· + ·) 0 event
    let countAfter ← scope.subscriptionCount
    pure (decide (countAfter > countBefore))
  shouldBe result true

test "Behavior.zipWith3 combines three behaviors" := do
  let b1 : Behavior Spider Nat := Behavior.constant 2
  let b2 : Behavior Spider Nat := Behavior.constant 3
  let b3 : Behavior Spider Nat := Behavior.constant 4
  let combined := Behavior.zipWith3 (fun a b c => a * b + c) b1 b2 b3
  let value ← combined.sample
  shouldBe value 10  -- 2 * 3 + 4 = 10

test "Behavior.zipWith4 combines four behaviors" := do
  let b1 : Behavior Spider Nat := Behavior.constant 1
  let b2 : Behavior Spider Nat := Behavior.constant 2
  let b3 : Behavior Spider Nat := Behavior.constant 3
  let b4 : Behavior Spider Nat := Behavior.constant 4
  let combined := Behavior.zipWith4 (fun a b c d => a + b + c + d) b1 b2 b3 b4
  let value ← combined.sample
  shouldBe value 10  -- 1 + 2 + 3 + 4 = 10

test "Behavior.allTrue returns true when all true" := do
  let bs : List (Behavior Spider Bool) := [
    Behavior.constant true,
    Behavior.constant true,
    Behavior.constant true
  ]
  let combined := Behavior.allTrue bs
  let value ← combined.sample
  shouldBe value true

test "Behavior.allTrue returns false when any false" := do
  let bs : List (Behavior Spider Bool) := [
    Behavior.constant true,
    Behavior.constant false,
    Behavior.constant true
  ]
  let combined := Behavior.allTrue bs
  let value ← combined.sample
  shouldBe value false

test "Behavior.anyTrue returns true when any true" := do
  let bs : List (Behavior Spider Bool) := [
    Behavior.constant false,
    Behavior.constant true,
    Behavior.constant false
  ]
  let combined := Behavior.anyTrue bs
  let value ← combined.sample
  shouldBe value true

test "Behavior.anyTrue returns false when all false" := do
  let bs : List (Behavior Spider Bool) := [
    Behavior.constant false,
    Behavior.constant false,
    Behavior.constant false
  ]
  let combined := Behavior.anyTrue bs
  let value ← combined.sample
  shouldBe value false

test "Behavior.not negates boolean behavior" := do
  let b : Behavior Spider Bool := Behavior.constant true
  let negated := Behavior.not b
  let value ← negated.sample
  shouldBe value false

test "Behavior.and combines two boolean behaviors" := do
  let b1 : Behavior Spider Bool := Behavior.constant true
  let b2 : Behavior Spider Bool := Behavior.constant false
  let combined := Behavior.and b1 b2
  let value ← combined.sample
  shouldBe value false

test "Behavior.or combines two boolean behaviors" := do
  let b1 : Behavior Spider Bool := Behavior.constant true
  let b2 : Behavior Spider Bool := Behavior.constant false
  let combined := Behavior.or b1 b2
  let value ← combined.sample
  shouldBe value true

-- Direct tests for core combinators

test "Behavior.fromSample creates behavior from IO action" := do
  let counterRef ← IO.mkRef (0 : Nat)
  let b : Behavior Spider Nat := Behavior.fromSample do
    counterRef.modify (· + 1)
    counterRef.get
  -- Each sample should increment and return the new value
  let v1 ← b.sample
  let v2 ← b.sample
  let v3 ← b.sample
  shouldBe (v1, v2, v3) (1, 2, 3)

test "Behavior.fromSample can read external state" := do
  let stateRef ← IO.mkRef "initial"
  let b : Behavior Spider String := Behavior.fromSample stateRef.get
  let v1 ← b.sample
  stateRef.set "updated"
  let v2 ← b.sample
  shouldBe (v1, v2) ("initial", "updated")

test "Behavior.pure creates constant behavior" := do
  let b : Behavior Spider Nat := Behavior.pure 42
  let v1 ← b.sample
  let v2 ← b.sample
  shouldBe (v1, v2) (42, 42)

test "Behavior.ap applies function behavior to value behavior" := do
  let bf : Behavior Spider (Nat → Nat) := Behavior.constant (· * 2)
  let ba : Behavior Spider Nat := Behavior.constant 21
  let result := Behavior.ap bf ba
  let value ← result.sample
  shouldBe value 42

test "Behavior.ap with changing function behavior" := do
  let funcRef ← IO.mkRef (fun (n : Nat) => n + 1)
  let bf : Behavior Spider (Nat → Nat) := Behavior.fromSample funcRef.get
  let ba : Behavior Spider Nat := Behavior.constant 10
  let result := Behavior.ap bf ba
  let v1 ← result.sample
  funcRef.set (· * 3)
  let v2 ← result.sample
  shouldBe (v1, v2) (11, 30)

test "Behavior.apply is alias for ap" := do
  let bf : Behavior Spider (Nat → Nat) := Behavior.constant (· + 5)
  let ba : Behavior Spider Nat := Behavior.constant 10
  let result := Behavior.apply bf ba
  let value ← result.sample
  shouldBe value 15

test "Behavior.bind chains dependent behaviors" := do
  let stateRef ← IO.mkRef (0 : Nat)
  let outer : Behavior Spider Nat := Behavior.fromSample stateRef.get
  let result := Behavior.bind outer fun n =>
    Behavior.constant (n * 10)
  stateRef.set 5
  let v1 ← result.sample
  stateRef.set 7
  let v2 ← result.sample
  shouldBe (v1, v2) (50, 70)

test "Behavior.bind with nested behaviors" := do
  let b1 : Behavior Spider Nat := Behavior.constant 10
  let b2 : Behavior Spider Nat := Behavior.constant 20
  let result := Behavior.bind b1 fun x =>
    Behavior.bind b2 fun y =>
      Behavior.constant (x + y)
  let value ← result.sample
  shouldBe value 30

test "Behavior.zip pairs two behaviors" := do
  let b1 : Behavior Spider Nat := Behavior.constant 1
  let b2 : Behavior Spider String := Behavior.constant "hello"
  let zipped := Behavior.zip b1 b2
  let value ← zipped.sample
  shouldBe value (1, "hello")

test "Behavior.zip with dynamic values" := do
  let ref1 ← IO.mkRef (10 : Nat)
  let ref2 ← IO.mkRef (20 : Nat)
  let b1 : Behavior Spider Nat := Behavior.fromSample ref1.get
  let b2 : Behavior Spider Nat := Behavior.fromSample ref2.get
  let zipped := Behavior.zip b1 b2
  let v1 ← zipped.sample
  ref1.set 100
  ref2.set 200
  let v2 ← zipped.sample
  shouldBe (v1, v2) ((10, 20), (100, 200))

test "Behavior allTrue with empty list returns true" := do
  let result := Behavior.allTrue ([] : List (Behavior Spider Bool))
  let value ← result.sample
  shouldBe value true

test "Behavior anyTrue with empty list returns false" := do
  let result := Behavior.anyTrue ([] : List (Behavior Spider Bool))
  let value ← result.sample
  shouldBe value false

test "Behavior.switch samples nested behavior" := do
  let inner1 : Behavior Spider Nat := Behavior.constant 10
  let inner2 : Behavior Spider Nat := Behavior.constant 20
  let selectorRef ← IO.mkRef inner1
  let outer : Behavior Spider (Behavior Spider Nat) := Behavior.fromSample selectorRef.get
  let switched := Behavior.switch outer
  let v1 ← switched.sample
  selectorRef.set inner2
  let v2 ← switched.sample
  shouldBe (v1, v2) (10, 20)

test "Behavior.switch with dynamic inner behaviors" := do
  let valueRef ← IO.mkRef (100 : Nat)
  let dynamicInner : Behavior Spider Nat := Behavior.fromSample valueRef.get
  let outer : Behavior Spider (Behavior Spider Nat) := Behavior.constant dynamicInner
  let switched := Behavior.switch outer
  let v1 ← switched.sample
  valueRef.set 200
  let v2 ← switched.sample
  shouldBe (v1, v2) (100, 200)

test "Behavior.join is alias for switch" := do
  let inner : Behavior Spider String := Behavior.constant "hello"
  let outer : Behavior Spider (Behavior Spider String) := Behavior.constant inner
  let joined := Behavior.join outer
  let value ← joined.sample
  shouldBe value "hello"


end ReactiveTests.BehaviorTests
