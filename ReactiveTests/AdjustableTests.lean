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

    SpiderM.liftIO <| triggerReplace (pure 2)
    SpiderM.liftIO <| triggerReplace (pure 3)

    SpiderM.liftIO resultsRef.get
  shouldBe result [1, 2, 3]

test "runWithReplaceM replacement can use FRP combinators" := do
  let result ← runSpider do
    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- A computation that creates reactive state and returns a derived value
    let computeWithState : Nat → SpiderM Nat := fun multiplier => do
      let (evt, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
      let dyn ← foldDyn (fun x acc => acc + x) 0 evt
      -- Fire some values - these propagate within their own frame
      SpiderM.liftIO <| trigger (multiplier * 1)
      SpiderM.liftIO <| trigger (multiplier * 2)
      -- The multiplier itself is returned (not the async result)
      pure multiplier

    let (initial, resultEvent) ← SpiderM.runWithReplaceM (computeWithState 5) replaceEvent

    let resultsRef ← SpiderM.liftIO <| IO.mkRef [initial]
    let _ ← SpiderM.liftIO <| resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    -- Replace with a new computation using multiplier 10
    SpiderM.liftIO <| triggerReplace (computeWithState 10)
    SpiderM.liftIO resultsRef.get

  -- Initial: returns 5
  -- Replacement: returns 10
  shouldBe result [5, 10]

test "traverseWithAdjustM maps over list" := do
  let result ← runSpider do
    let f : Nat → SpiderM Nat := fun n => pure (n * 2)
    let (results, _) ← SpiderM.traverseWithAdjustM f [1, 2, 3]
    pure results
  shouldBe result [2, 4, 6]

test "traverseWithAdjustM with empty list" := do
  let result ← runSpider do
    let f : Nat → SpiderM Nat := fun n => pure (n * 2)
    let (results, _) ← SpiderM.traverseWithAdjustM f ([] : List Nat)
    pure results
  shouldBe result []

test "traverseWithAdjustM with effectful computation" := do
  let result ← runSpider do
    let counterRef ← SpiderM.liftIO <| IO.mkRef 0
    let f : Nat → SpiderM Nat := fun n => do
      let count ← SpiderM.liftIO <| counterRef.modifyGet fun c => (c, c + 1)
      pure (n + count)
    let (results, _) ← SpiderM.traverseWithAdjustM f [10, 20, 30]
    pure results
  -- 10 + 0, 20 + 1, 30 + 2
  shouldBe result [10, 21, 32]

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

    SpiderM.liftIO <| triggerReplace (pure 100)
    SpiderM.liftIO resultsRef.get
  shouldBe result [42, 100]

test "traverseDynList updates when list changes" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2] listEvent

    let f : Nat → SpiderM Nat := fun n => pure (n * 10)
    let resultDyn ← traverseDynList f listDyn

    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← SpiderM.liftIO <| resultDyn.updated.subscribe fun vals =>
      valuesRef.modify (· ++ [vals])

    -- Get initial value
    let initial ← sample resultDyn.current

    -- Update the list
    SpiderM.liftIO <| fireList [3, 4, 5]

    let updates ← SpiderM.liftIO valuesRef.get
    pure (initial, updates)

  -- Initial: [1, 2] → [10, 20]
  -- Update: [3, 4, 5] → [30, 40, 50]
  shouldBe result ([10, 20], [[30, 40, 50]])

#generate_tests

end ReactiveTests.AdjustableTests
