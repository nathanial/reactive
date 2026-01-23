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
    let _ ← resultEvent.subscribe fun n =>
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
    let _ ← resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    triggerReplace (pure 100)
    SpiderM.liftIO resultsRef.get
  shouldBe result [42, 100]

test "traverseDynList updates when list changes" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2] listEvent

    let f : Nat → SpiderM Nat := fun n => pure (n * 10)
    -- Use `id` as key function since items are Nat
    let resultDyn ← traverseDynList id f listDyn

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

test "traverseDynList incremental - only computes new items" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2, 3] listEvent

    -- Track which items f is called for (in order)
    let fCalledForRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let f : Nat → SpiderM Nat := fun n => do
      SpiderM.liftIO <| fCalledForRef.modify (· ++ [n])
      pure (n * 10)

    let resultDyn ← traverseDynList id f listDyn

    -- Initial list [1, 2, 3]: f should be called for all items
    let initialResults ← sample resultDyn.current
    let fCalledAfterInit ← SpiderM.liftIO fCalledForRef.get

    -- Update to [2, 3, 4, 5]:
    --   - Item 1 is removed
    --   - Items 2, 3 are KEPT (should reuse cached results, f NOT called)
    --   - Items 4, 5 are NEW (f should be called)
    fireList [2, 3, 4, 5]

    let fCalledAfterUpdate ← SpiderM.liftIO fCalledForRef.get
    let finalResults ← sample resultDyn.current

    pure (initialResults, fCalledAfterInit, finalResults, fCalledAfterUpdate)

  let (initialResults, fCalledAfterInit, finalResults, fCalledAfterUpdate) := result

  -- After init: f was called for [1, 2, 3]
  shouldBe fCalledAfterInit [1, 2, 3]
  shouldBe initialResults [10, 20, 30]

  -- After update: f was ONLY called for NEW items [4, 5]
  -- If f were called again for 2, 3, we'd see [1, 2, 3, 2, 3, 4, 5]
  -- Instead we see [1, 2, 3, 4, 5] proving 2, 3 were reused from cache
  shouldBe fCalledAfterUpdate [1, 2, 3, 4, 5]
  shouldBe finalResults [20, 30, 40, 50]

test "traverseDynList incremental - disposes removed items" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2, 3] listEvent

    -- Track disposal via subscription cleanup
    let disposedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let f : Nat → SpiderM Nat := fun n => do
      -- Create a subscription that logs when disposed
      let (evt, _) ← newTriggerEvent (t := Spider) (a := Unit)
      let _ ← evt.subscribe fun _ => pure ()
      -- Register cleanup action to track disposal
      let scope ← SpiderM.getScope
      SpiderM.liftIO <| scope.register do
        disposedRef.modify (· ++ [n])
      pure (n * 10)

    let resultDyn ← traverseDynList id f listDyn
    let _ ← sample resultDyn.current

    -- Remove items 1 and 2 (keep 3)
    fireList [3]

    let final ← sample resultDyn.current
    let disposed ← SpiderM.liftIO disposedRef.get

    pure (final, disposed)

  -- Final list should be [30] (only item 3 remains)
  shouldBe result.1 [30]
  -- Items 1 and 2 should have been disposed
  shouldBe (result.2.contains 1) true
  shouldBe (result.2.contains 2) true

test "traverseDynList incremental - reordering preserves results" := do
  let result ← runSpider do
    let (listEvent, fireList) ← newTriggerEvent (t := Spider) (a := List Nat)
    let listDyn ← holdDyn [1, 2, 3] listEvent

    -- Use a counter to verify f is called once per unique key
    let counterRef ← SpiderM.liftIO <| IO.mkRef 0

    let f : Nat → SpiderM Nat := fun n => do
      let count ← SpiderM.liftIO <| counterRef.modifyGet fun c => (c, c + 1)
      -- Result includes both the value and a unique call ID
      pure (n * 100 + count)

    let resultDyn ← traverseDynList id f listDyn

    let initial ← sample resultDyn.current
    let countAfterInit ← SpiderM.liftIO counterRef.get

    -- Reorder: [3, 1, 2] - no new items, just reordered
    fireList [3, 1, 2]

    let reordered ← sample resultDyn.current
    let countAfterReorder ← SpiderM.liftIO counterRef.get

    pure (initial, countAfterInit, reordered, countAfterReorder)

  -- Initial: [100+0, 200+1, 300+2] = [100, 201, 302]
  shouldBe result.1 [100, 201, 302]
  shouldBe result.2.1 3  -- f called 3 times

  -- After reorder: same results, just reordered to [302, 100, 201]
  shouldBe result.2.2.1 [302, 100, 201]
  shouldBe result.2.2.2 3  -- f NOT called again (count unchanged)


end ReactiveTests.AdjustableTests
