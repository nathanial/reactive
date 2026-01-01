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
    let behavior ← SpiderM.liftIO <| Behavior.hold 0 event
    SpiderM.liftIO <| fire 42
    behavior.sample
  shouldBe result 42

test "Behavior.hold updates on each event fire" := do
  let result ← runSpider do
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let behavior ← SpiderM.liftIO <| Behavior.hold 0 event
    SpiderM.liftIO <| fire 10
    SpiderM.liftIO <| fire 20
    SpiderM.liftIO <| fire 30
    behavior.sample
  shouldBe result 30

test "Behavior.foldB accumulates event values" := do
  let result ← runSpider do
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let behavior ← SpiderM.liftIO <| Behavior.foldB (· + ·) 0 event
    SpiderM.liftIO <| fire 10
    SpiderM.liftIO <| fire 20
    SpiderM.liftIO <| fire 5
    behavior.sample
  shouldBe result 35

test "Behavior.holdM registers with scope" := do
  let result ← runSpider do
    let scope ← SpiderM.getScope
    let countBefore ← SpiderM.liftIO <| scope.subscriptionCount
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← Behavior.holdM 0 event
    let countAfter ← SpiderM.liftIO <| scope.subscriptionCount
    pure (decide (countAfter > countBefore))
  shouldBe result true

test "Behavior.foldBM registers with scope" := do
  let result ← runSpider do
    let scope ← SpiderM.getScope
    let countBefore ← SpiderM.liftIO <| scope.subscriptionCount
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← Behavior.foldBM (· + ·) 0 event
    let countAfter ← SpiderM.liftIO <| scope.subscriptionCount
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

#generate_tests

end ReactiveTests.BehaviorTests
