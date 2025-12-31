import Crucible
import Reactive

namespace ReactiveTests.DynamicTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Dynamic Tests"

test "Dynamic.hold maintains latest value" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn 0 event

    -- Initial value
    let v0 ← liftM (m := IO) <| dyn.sample
    ensure (v0 == 0) "Initial value should be 0"

    -- Fire some events
    liftM (m := IO) <| trigger 5
    let v1 ← liftM (m := IO) <| dyn.sample
    ensure (v1 == 5) "After trigger 5, value should be 5"

    liftM (m := IO) <| trigger 10
    let v2 ← liftM (m := IO) <| dyn.sample
    ensure (v2 == 10) "After trigger 10, value should be 10"

    pure v2

  shouldBe result 10

test "Dynamic.foldDyn accumulates values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← foldDyn (· + ·) 0 event

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 3

    liftM (m := IO) <| dyn.sample

  shouldBe result 6  -- 0 + 1 + 2 + 3

test "Dynamic.updated fires on changes" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn 0 event

    let changesRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| dyn.updated.subscribe fun n =>
      changesRef.modify (· ++ [n])

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 3

    liftM (m := IO) changesRef.get

  shouldBe result [1, 2, 3]

test "Dynamic.current returns a Behavior" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := String)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn "initial" event

    let behavior := dyn.current

    liftM (m := IO) <| trigger "updated"

    sample behavior

  shouldBe result "updated"

#generate_tests

end ReactiveTests.DynamicTests
