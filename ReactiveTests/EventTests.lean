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
    let mappedEvent ← liftM (m := IO) <| Event.mapWithId (· * 2) event nodeId

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
    let filteredEvent ← liftM (m := IO) <| Event.filterWithId (· > 2) event nodeId

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
    let mergedEvent ← liftM (m := IO) <| Event.mergeWithId event1 event2 nodeId

    let receivedRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| mergedEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    liftM (m := IO) <| trigger1 1
    liftM (m := IO) <| trigger2 2
    liftM (m := IO) <| trigger1 3

    liftM (m := IO) receivedRef.get

  shouldBe result [1, 2, 3]

-- SpiderM Combinator Tests

test "Event.mapM transforms values with auto NodeId" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| mapped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4, 6]

test "Event.filterM filters values with auto NodeId" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let filtered ← Event.filterM (· % 2 == 0) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO <| trigger 4
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4]

test "Event.mergeM combines events with auto NodeId" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let merged ← Event.mergeM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| t2 2
    SpiderM.liftIO <| t1 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.scanM accumulates values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let scanned ← Event.scanM (· + ·) 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| scanned.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3, 6]

test "Event.takeNM takes first n occurrences" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let taken ← Event.takeNM 3 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| taken.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO <| trigger 4
    SpiderM.liftIO <| trigger 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.dropNM drops first n occurrences" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dropped ← Event.dropNM 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| dropped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO <| trigger 4
    SpiderM.liftIO receivedRef.get
  shouldBe result [3, 4]

test "Event.gateM filters by boolean behavior" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (gateEvent, gateToggle) ← newTriggerEvent (t := Spider) (a := Bool)
    let gateBehavior ← holdDyn true gateEvent
    let gated ← Event.gateM gateBehavior.current event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1        -- gate open, passes
    SpiderM.liftIO <| gateToggle false -- close gate
    SpiderM.liftIO <| trigger 2        -- gate closed, blocked
    SpiderM.liftIO <| gateToggle true  -- open gate
    SpiderM.liftIO <| trigger 3        -- gate open, passes
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.leftmostM takes first from list" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← Event.leftmostM [e1, e2, e3]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t2 1
    SpiderM.liftIO <| t1 2
    SpiderM.liftIO <| t3 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

#generate_tests

end ReactiveTests.EventTests
