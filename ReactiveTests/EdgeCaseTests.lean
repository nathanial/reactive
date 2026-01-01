import Crucible
import Reactive

namespace ReactiveTests.EdgeCaseTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Edge Case Tests"

test "event with zero subscribers can still fire" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- No subscribers, but firing should not crash
    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    pure "ok"
  shouldBe result "ok"

test "firing same value twice notifies twice" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 42
    SpiderM.liftIO <| trigger 42
    SpiderM.liftIO <| trigger 42
    SpiderM.liftIO receivedRef.get
  shouldBe result [42, 42, 42]

test "sample during network construction returns initial value" := do
  let result ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 100 event
    -- Sample immediately during construction
    sample dyn.current
  shouldBe result 100

test "very deep event chain propagates correctly" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a chain of 50 mapped events
    let mut current := source
    for _ in [0:50] do
      current ← Event.mapM (· + 1) current

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| current.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 0
    SpiderM.liftIO receivedRef.get
  -- 0 + 50 increments = 50
  shouldBe result [50]

test "many simultaneous subscribers all receive values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Add 100 subscribers
    for _ in [0:100] do
      let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
        countRef.modify (· + 1)

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO countRef.get
  shouldBe result 100

test "dynamic with no updates samples initial value" := do
  let result ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 999 event
    -- Never fire, just sample
    sample dyn.current
  shouldBe result 999

test "unsubscribe prevents future notifications" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let unsubscribe ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO unsubscribe  -- Unsubscribe
    SpiderM.liftIO <| trigger 3  -- Should not be received
    SpiderM.liftIO <| trigger 4  -- Should not be received
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "multiple unsubscribe calls are safe" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let unsubscribe ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO unsubscribe
    SpiderM.liftIO unsubscribe  -- Second unsubscribe should be safe
    SpiderM.liftIO unsubscribe  -- Third too
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [1]

test "behavior samples return current value immediately" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event

    let v0 ← sample dyn.current
    SpiderM.liftIO <| trigger 10
    let v1 ← sample dyn.current
    SpiderM.liftIO <| trigger 20
    let v2 ← sample dyn.current
    pure (v0, v1, v2)
  shouldBe result (0, 10, 20)

test "filter with always-false predicate creates silent event" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let filtered ← Event.filterM (fun _ => false) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result []

#generate_tests

end ReactiveTests.EdgeCaseTests
