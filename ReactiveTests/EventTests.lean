import Crucible
import Reactive

namespace ReactiveTests.EventTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Event Tests"

test "Event.newTrigger creates event that can be fired" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let receivedRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)

    let _ ← liftM (m := IO) <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 3

    liftM (m := IO) receivedRef.get

  shouldBe result [1, 2, 3]

test "Event.map transforms values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let nodeId ← SpiderM.freshNodeId
    let mappedEvent ← liftM (m := IO) <| Event.map (· * 2) event nodeId

    let receivedRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| mappedEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 5

    liftM (m := IO) receivedRef.get

  shouldBe result [2, 4, 10]

test "Event.filter removes non-matching values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let nodeId ← SpiderM.freshNodeId
    let filteredEvent ← liftM (m := IO) <| Event.filter (· > 2) event nodeId

    let receivedRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| filteredEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 3
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 5

    liftM (m := IO) receivedRef.get

  shouldBe result [3, 5]

test "Event.merge combines events" := do
  let result ← runSpider do
    let pair1 ← newTriggerEvent (t := Spider) (a := Nat)
    let pair2 ← newTriggerEvent (t := Spider) (a := Nat)
    let event1 := pair1.1
    let trigger1 := pair1.2
    let event2 := pair2.1
    let trigger2 := pair2.2
    let nodeId ← SpiderM.freshNodeId
    let mergedEvent ← liftM (m := IO) <| Event.merge event1 event2 nodeId

    let receivedRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| mergedEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    liftM (m := IO) <| trigger1 1
    liftM (m := IO) <| trigger2 2
    liftM (m := IO) <| trigger1 3

    liftM (m := IO) receivedRef.get

  shouldBe result [1, 2, 3]

#generate_tests

end ReactiveTests.EventTests
