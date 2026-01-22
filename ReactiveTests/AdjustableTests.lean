import Crucible
import Reactive

namespace ReactiveTests.AdjustableTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Adjustable Tests"

test "runWithReplaceM returns initial result" := do
  let result ← runSpider do
    let (replaceEvent, _) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)
    let (initial, _) ← SpiderM.runWithReplaceM (pure 42 : SpiderM Nat) replaceEvent
    pure initial
  shouldBe result 42

test "runWithReplaceM fires result event on replacement" := do
  let result ← runSpider do
    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)
    let (initial, resultEvent) ← SpiderM.runWithReplaceM (pure 1 : SpiderM Nat) replaceEvent

    let resultsRef ← SpiderM.liftIO <| IO.mkRef [initial]
    let _ ← SpiderM.liftIO <| resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    triggerReplace (pure 2)
    triggerReplace (pure 3)

    SpiderM.liftIO resultsRef.get
  shouldBe result [1, 2, 3]

-- Note: Detailed frame semantics tests are in FrameSemanticsTests.lean

test "runWithReplaceRequester basic" := do
  let result ← runSpider do
    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- Computation that produces its own replacement event
    let computation : SpiderM (Nat × Event Spider (SpiderM Nat)) :=
      pure (42, replaceEvent)

    let (initial, resultEvent) ← runWithReplaceRequester computation

    let resultsRef ← SpiderM.liftIO <| IO.mkRef [initial]
    let _ ← SpiderM.liftIO <| resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    triggerReplace (pure 100)
    SpiderM.liftIO resultsRef.get
  shouldBe result [42, 100]

test "traverseDynList updates when list changes" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2] listEvent

    let f : Nat → SpiderM Nat := fun n => pure (n * 10)
    let resultDyn ← traverseDynList f listDyn

    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← resultDyn.updated.subscribe fun vals =>
      valuesRef.modify (· ++ [vals])

    -- Get initial value
    let initial ← sample resultDyn.current

    -- Update the list
    fireList [3, 4, 5]

    let updates ← SpiderM.liftIO valuesRef.get
    pure (initial, updates)

  -- Initial: [1, 2] → [10, 20]
  -- Update: [3, 4, 5] → [30, 40, 50]
  shouldBe result ([10, 20], [[30, 40, 50]])


end ReactiveTests.AdjustableTests
