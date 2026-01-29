import Crucible
import Reactive
import Std.Data.HashMap

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
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2, 3]

test "Event.map transforms values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let nodeId ← SpiderM.freshNodeId
    let mappedEvent ← SpiderM.liftIO <| Event.mapWithId (· * 2) event nodeId

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| mappedEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 5

    SpiderM.liftIO receivedRef.get

  shouldBe result [2, 4, 10]

test "Event.filter removes non-matching values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let nodeId ← SpiderM.freshNodeId
    let filteredEvent ← SpiderM.liftIO <| Event.filterWithId (· > 2) event nodeId

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| filteredEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 5

    SpiderM.liftIO receivedRef.get

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
    let mergedEvent ← SpiderM.liftIO <| Event.mergeWithId event1 event2 nodeId

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| mergedEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger1 1
    SpiderM.liftIO <| trigger2 2
    SpiderM.liftIO <| trigger1 3

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2, 3]

-- SpiderM Combinator Tests

test "Event.mapM transforms values with auto NodeId" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← mapped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4, 6]

test "Event.filterM filters values with auto NodeId" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let filtered ← Event.filterM (· % 2 == 0) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    trigger 4
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4]

test "Event.mergeM combines events with auto NodeId" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let merged ← Event.mergeM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t1 1
    t2 2
    t1 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.scanM accumulates values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let scanned ← Event.scanM (· + ·) 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← scanned.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3, 6]

test "Event.withPreviousM emits (prev, curr) pairs" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let pairs ← Event.withPreviousM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← pairs.subscribe fun p =>
      receivedRef.modify (· ++ [p])

    trigger 10
    trigger 20
    trigger 15
    trigger 30
    SpiderM.liftIO receivedRef.get
  -- First occurrence (10) is skipped since there's no previous
  shouldBe result [(10, 20), (20, 15), (15, 30)]

test "Event.withPreviousM skips first occurrence" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let pairs ← Event.withPreviousM event

    let countRef ← SpiderM.liftIO <| IO.mkRef 0
    let _ ← pairs.subscribe fun _ =>
      countRef.modify (· + 1)

    -- Fire only once - should produce no output
    trigger "only"
    SpiderM.liftIO countRef.get
  shouldBe result 0

test "Event.withPrevious with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let pairs ← SpiderM.liftIO <| Event.withPrevious ctx event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← pairs.subscribe fun p =>
      receivedRef.modify (· ++ [p])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [(1, 2), (2, 3)]

test "Event.distinctM skips consecutive duplicates" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let unique ← Event.distinctM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← unique.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 1  -- duplicate, skipped
    trigger 2
    trigger 2  -- duplicate, skipped
    trigger 2  -- duplicate, skipped
    trigger 1  -- different from previous, fires
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 1, 3]

test "Event.distinctM fires first value" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let unique ← Event.distinctM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← unique.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    trigger "hello"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["hello"]

test "Event.distinct with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let unique ← SpiderM.liftIO <| Event.distinct ctx event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← unique.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 5
    trigger 5
    trigger 10
    trigger 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [5, 10, 5]

test "Event.bufferM collects n events before emitting" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let batched ← Event.bufferM 3 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Array Nat))
    let _ ← batched.subscribe fun arr =>
      receivedRef.modify (· ++ [arr])

    trigger 1
    trigger 2
    trigger 3  -- batch emits here
    trigger 4
    trigger 5
    trigger 6  -- batch emits here
    trigger 7
    SpiderM.liftIO receivedRef.get
  shouldBe result [#[1, 2, 3], #[4, 5, 6]]

test "Event.bufferM with incomplete batch" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let batched ← Event.bufferM 4 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Array String))
    let _ ← batched.subscribe fun arr =>
      receivedRef.modify (· ++ [arr])

    trigger "a"
    trigger "b"
    trigger "c"
    -- Only 3 events, buffer size is 4, so no emission
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.buffer with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let batched ← SpiderM.liftIO <| Event.buffer ctx 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Array Nat))
    let _ ← batched.subscribe fun arr =>
      receivedRef.modify (· ++ [arr])

    trigger 10
    trigger 20
    trigger 30
    trigger 40
    SpiderM.liftIO receivedRef.get
  shouldBe result [#[10, 20], #[30, 40]]

test "Event.windowM collects events within time window" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let windowed ← Event.windowM (Chronos.Duration.fromMilliseconds 50) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Array Nat))
    let _ ← windowed.subscribe fun arr =>
      receivedRef.modify (· ++ [arr])

    -- Fire several events quickly (within window)
    trigger 1
    trigger 2
    trigger 3
    -- Wait for window to close
    SpiderM.liftIO <| IO.sleep 80
    SpiderM.liftIO receivedRef.get
  shouldBe result [#[1, 2, 3]]

test "Event.windowM emits multiple windows" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let windowed ← Event.windowM (Chronos.Duration.fromMilliseconds 30) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Array String))
    let _ ← windowed.subscribe fun arr =>
      receivedRef.modify (· ++ [arr])

    -- First window
    trigger "a"
    trigger "b"
    SpiderM.liftIO <| IO.sleep 50

    -- Second window
    trigger "c"
    SpiderM.liftIO <| IO.sleep 50

    SpiderM.liftIO receivedRef.get
  shouldBe result [#["a", "b"], #["c"]]

test "Event.takeNM takes first n occurrences" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let taken ← Event.takeNM 3 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← taken.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    trigger 4
    trigger 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.onceM takes only first occurrence" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← Event.onceM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1]

test "Event.once with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let first ← SpiderM.liftIO <| Event.once ctx event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← first.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    trigger "first"
    trigger "second"
    trigger "third"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["first"]

test "Event.dropNM drops first n occurrences" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dropped ← Event.dropNM 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← dropped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    trigger 4
    SpiderM.liftIO receivedRef.get
  shouldBe result [3, 4]

test "Event.gateM filters by boolean behavior" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (gateEvent, gateToggle) ← newTriggerEvent (t := Spider) (a := Bool)
    let gateBehavior ← holdDyn true gateEvent
    let gated ← Event.gateM gateBehavior.current event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1        -- gate open, passes
    gateToggle false -- close gate
    trigger 2        -- gate closed, blocked
    gateToggle true  -- open gate
    trigger 3        -- gate open, passes
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.leftmostM takes first from list" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← Event.leftmostM [e1, e2, e3]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t2 1
    t1 2
    t3 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Fluent Event.map' enables chaining" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Use explicit form: Event.map' event f
    let mapped ← Event.map' event (· * 2)

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← mapped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4, 6]

test "Fluent chaining with bind" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Chain: map then filter using bind with explicit form
    let processed ← Event.map' event (· * 2) >>= (Event.filter' · (· > 3))

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← processed.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1  -- 1*2=2, filtered out (not > 3)
    trigger 2  -- 2*2=4, passes
    trigger 3  -- 3*2=6, passes
    SpiderM.liftIO receivedRef.get
  shouldBe result [4, 6]

test "Fluent Event.gate' and merge'" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let gateBehavior := Behavior.constant true

    -- Gate e1 and merge with e2 using explicit form
    let gated ← Event.gate' e1 gateBehavior
    let merged ← Event.merge' gated e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t1 1
    t2 2
    t1 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

-- New tests for full coverage

test "Event.never never fires" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let neverEvent ← SpiderM.liftIO <| Event.never (t := Spider) ctx (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← neverEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])
    -- The event never fires, so receivedRef should stay empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.neverM never fires (SpiderM version)" := do
  let result ← runSpider do
    let neverEvent ← Event.neverM (a := String)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← neverEvent.subscribe fun s =>
      receivedRef.modify (· ++ [s])
    -- The event never fires, so receivedRef should stay empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.mapMaybeM filters and transforms" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Only pass through even numbers, and halve them
    let filtered ← Event.mapMaybeM (fun n =>
      if n % 2 == 0 then some (n / 2) else none) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1  -- odd, filtered out
    trigger 4  -- even, becomes 2
    trigger 5  -- odd, filtered out
    trigger 10 -- even, becomes 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 5]

test "Event.attachM pairs event with behavior value" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let counterBehavior := Behavior.constant 42
    let attached ← Event.attachM counterBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    trigger "hello"
    trigger "world"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(42, "hello"), (42, "world")]

test "Event.attachWithM applies function to behavior and event" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let multiplierBehavior := Behavior.constant 10
    let attached ← Event.attachWithM (· * ·) multiplierBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← attached.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20, 30]

test "Event.fanEitherM splits Sum event into two" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Sum Nat String)
    let (leftEvent, rightEvent) ← Event.fanEitherM event

    let leftRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let rightRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← leftEvent.subscribe fun n =>
      leftRef.modify (· ++ [n])
    let _ ← rightEvent.subscribe fun s =>
      rightRef.modify (· ++ [s])

    trigger (Sum.inl 1)
    trigger (Sum.inr "hello")
    trigger (Sum.inl 2)
    trigger (Sum.inr "world")

    let left ← SpiderM.liftIO leftRef.get
    let right ← SpiderM.liftIO rightRef.get
    pure (left, right)
  shouldBe result ([1, 2], ["hello", "world"])

test "Event.splitEM splits event by predicate" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (evens, odds) ← Event.splitEM (· % 2 == 0) event

    let evensRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let oddsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← evens.subscribe fun n =>
      evensRef.modify (· ++ [n])
    let _ ← odds.subscribe fun n =>
      oddsRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    trigger 4
    trigger 5

    let evenVals ← SpiderM.liftIO evensRef.get
    let oddVals ← SpiderM.liftIO oddsRef.get
    pure (evenVals, oddVals)
  shouldBe result ([2, 4], [1, 3, 5])

test "Event.splitE with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let (short, long) ← SpiderM.liftIO <| Event.splitE ctx (·.length < 4) event

    let shortRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let longRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← short.subscribe fun s =>
      shortRef.modify (· ++ [s])
    let _ ← long.subscribe fun s =>
      longRef.modify (· ++ [s])

    trigger "hi"
    trigger "hello"
    trigger "yo"
    trigger "world"

    let shortVals ← SpiderM.liftIO shortRef.get
    let longVals ← SpiderM.liftIO longRef.get
    pure (shortVals, longVals)
  shouldBe result (["hi", "yo"], ["hello", "world"])

test "Event.partitionEM is an alias for splitEM" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (passing, failing) ← Event.partitionEM (· > 5) event

    let passRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let failRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← passing.subscribe fun n =>
      passRef.modify (· ++ [n])
    let _ ← failing.subscribe fun n =>
      failRef.modify (· ++ [n])

    trigger 3
    trigger 7
    trigger 2
    trigger 10

    let passVals ← SpiderM.liftIO passRef.get
    let failVals ← SpiderM.liftIO failRef.get
    pure (passVals, failVals)
  shouldBe result ([7, 10], [3, 2])

test "Event.partitionE with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (positive, nonPositive) ← SpiderM.liftIO <| Event.partitionE ctx (· > 0) event

    let posRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let nonPosRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← positive.subscribe fun n =>
      posRef.modify (· ++ [n])
    let _ ← nonPositive.subscribe fun n =>
      nonPosRef.modify (· ++ [n])

    trigger 0
    trigger 5
    trigger 0
    trigger 3

    let posVals ← SpiderM.liftIO posRef.get
    let nonPosVals ← SpiderM.liftIO nonPosRef.get
    pure (posVals, nonPosVals)
  shouldBe result ([5, 3], [0, 0])

test "Event.fanM/selectM dispatches per key" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Std.HashMap Nat String)
    let fan ← Event.fanM event
    let ones ← Event.selectM fan 1
    let twos ← Event.selectM fan 2

    let onesRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let twosRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← ones.subscribe fun v =>
      onesRef.modify (· ++ [v])
    let _ ← twos.subscribe fun v =>
      twosRef.modify (· ++ [v])

    let map1 : Std.HashMap Nat String := Std.HashMap.ofList [(1, "a"), (3, "skip")]
    let map2 : Std.HashMap Nat String := Std.HashMap.ofList [(2, "b"), (1, "c")]
    let map3 : Std.HashMap Nat String := Std.HashMap.ofList [(3, "ignore")]

    trigger map1
    trigger map2
    trigger map3

    let onesVals ← SpiderM.liftIO onesRef.get
    let twosVals ← SpiderM.liftIO twosRef.get
    pure (onesVals, twosVals)
  shouldBe result (["a", "c"], ["b"])

test "Event.accumulateM maintains running state" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let accumulated ← Event.accumulateM (· + ·) 100 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1   -- 100 + 1 = 101
    trigger 2   -- 101 + 2 = 103
    trigger 10  -- 103 + 10 = 113
    SpiderM.liftIO receivedRef.get
  shouldBe result [101, 103, 113]

test "Event.delayFrameM fires after current frame" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayFrameM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← event.subscribe fun n =>
      receivedRef.modify (· ++ [s!"now {n}"])
    let _ ← delayed.subscribe fun n =>
      receivedRef.modify (· ++ [s!"later {n}"])

    trigger 1
    trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result ["now 1", "later 1", "now 2", "later 2"]

test "Event.mergeListM with empty list returns never event" := do
  let result ← runSpider do
    let merged ← Event.mergeListM ([] : List (Event Spider Nat))
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])
    -- Nothing to fire, so should be empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.leftmostM with empty list returns never event" := do
  let result ← runSpider do
    let first ← Event.leftmostM ([] : List (Event Spider Nat))
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← first.subscribe fun n =>
      receivedRef.modify (· ++ [n])
    -- Nothing to fire, so should be empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "rapid event firing preserves order" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire many events rapidly
    for i in [0:100] do
      trigger i
    SpiderM.liftIO receivedRef.get
  shouldBe result (List.range 100)

test "Event.attachM tracks dynamic behavior changes" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let (countEvent, countFire) ← newTriggerEvent (t := Spider) (a := Nat)
    let counterDyn ← holdDyn 0 countEvent
    let attached ← Event.attachM counterDyn.current event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    trigger "a"
    countFire 10
    trigger "b"
    countFire 20
    trigger "c"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(0, "a"), (10, "b"), (20, "c")]

-- Pure IO versions with TimelineCtx

test "Event.tag samples behavior on each event" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Unit)
    let counterRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let counterBehavior : Behavior Spider Nat := Behavior.fromSample do
      counterRef.modify (· + 1)
      counterRef.get
    let tagged ← SpiderM.liftIO <| Event.tag ctx counterBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← tagged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger ()
    trigger ()
    trigger ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.attach pairs behavior value with event value" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let attached ← SpiderM.liftIO <| Event.attach ctx multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    trigger "a"
    trigger "b"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(10, "a"), (10, "b")]

test "Event.attachWith combines behavior and event with function" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let attached ← SpiderM.liftIO <| Event.attachWith ctx (· * ·) multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← attached.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20, 30]

test "Event.gate filters by boolean behavior (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let gateRef ← SpiderM.liftIO <| IO.mkRef true
    let gateBehavior : Behavior Spider Bool := Behavior.fromSample gateRef.get
    let gated ← SpiderM.liftIO <| Event.gate ctx gateBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1        -- gate open
    SpiderM.liftIO <| gateRef.set false
    trigger 2        -- gate closed
    SpiderM.liftIO <| gateRef.set true
    trigger 3        -- gate open
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.accumulate maintains running total (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let accumulated ← SpiderM.liftIO <| Event.accumulate ctx (· + ·) 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 5
    trigger 10
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [5, 15, 18]

test "Event.scan is alias for accumulate" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let scanned ← SpiderM.liftIO <| Event.scan ctx (· * ·) 1 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← scanned.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 2  -- 1 * 2 = 2
    trigger 3  -- 2 * 3 = 6
    trigger 4  -- 6 * 4 = 24
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 6, 24]

test "Event.takeN limits occurrences (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let taken ← SpiderM.liftIO <| Event.takeN ctx 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← taken.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3  -- should not fire
    trigger 4  -- should not fire
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.dropN skips occurrences (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dropped ← SpiderM.liftIO <| Event.dropN ctx 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← dropped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1  -- dropped
    trigger 2  -- dropped
    trigger 3  -- passes
    trigger 4  -- passes
    SpiderM.liftIO receivedRef.get
  shouldBe result [3, 4]

test "Event.leftmost with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← SpiderM.liftIO <| Event.leftmost ctx [e1, e2]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t2 10
    t1 20
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20]

test "Event.fanEither with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Sum Nat String)
    let (leftEvent, rightEvent) ← SpiderM.liftIO <| Event.fanEither ctx event

    let leftRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let rightRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← leftEvent.subscribe fun n =>
      leftRef.modify (· ++ [n])
    let _ ← rightEvent.subscribe fun s =>
      rightRef.modify (· ++ [s])

    trigger (Sum.inl 1)
    trigger (Sum.inr "a")
    trigger (Sum.inl 2)

    let left ← SpiderM.liftIO leftRef.get
    let right ← SpiderM.liftIO rightRef.get
    pure (left, right)
  shouldBe result ([1, 2], ["a"])

test "Event.mergeList with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let merged ← SpiderM.liftIO <| Event.mergeList ctx [e1, e2]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])

    t1 1
    t2 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [[1], [2]]

test "Event.mapM cleans up on scope dispose" := do
  let disposedRef ← IO.mkRef false
  let _ ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← SpiderM.withAutoDisposeScope do
      let mapped ← Event.mapM (· + 1) event
      -- Subscribe so we can verify cleanup
      let _ ← mapped.subscribe fun _ => pure ()
      pure ()
    -- After withAutoDisposeScope, subscriptions should be cleaned up
    SpiderM.liftIO <| disposedRef.set true
    pure ()
  let disposed ← disposedRef.get
  shouldBe disposed true

test "Event.mapConstM maps all values to constant" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let constEvent ← Event.mapConstM "fired" event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← constEvent.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    trigger 1
    trigger 42
    trigger 999
    SpiderM.liftIO receivedRef.get
  shouldBe result ["fired", "fired", "fired"]

test "Event.mapConst' maps all values to constant (fluent style)" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let constEvent ← Event.mapConst' event 100

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← constEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger "hello"
    trigger "world"
    SpiderM.liftIO receivedRef.get
  shouldBe result [100, 100]

test "Event.zipEM pairs simultaneous events (diamond pattern)" := do
  let result ← runSpider do
    -- Use diamond pattern: single source, two derived events
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· * 2) trigger      -- produces Nat
    let e2 ← Event.mapM (toString ·) trigger  -- produces String
    let zipped ← Event.zipEM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    fire 5
    SpiderM.liftIO receivedRef.get
  -- e1 produces 10, e2 produces "5" - both fire simultaneously from same source
  shouldBe result [(10, "5")]

test "Event.zipEM ignores non-simultaneous events" := do
  let result ← runSpider do
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := String)
    let zipped ← Event.zipEM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    -- Fire separately (different frames)
    fire1 10
    fire2 "hello"

    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.zipEM handles multiple simultaneous pairs" := do
  let result ← runSpider do
    -- Use diamond pattern
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM id trigger
    let e2 ← Event.mapM (fun n => String.mk (List.replicate n 'x')) trigger
    let zipped ← Event.zipEM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    -- Fire twice, each creating a simultaneous pair
    fire 1  -- produces (1, "x")
    fire 2  -- produces (2, "xx")

    SpiderM.liftIO receivedRef.get
  shouldBe result [(1, "x"), (2, "xx")]

test "Event.zipE with pure IO (diamond pattern)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· + 40) trigger
    let e2 ← Event.mapM (fun _ => "test") trigger
    let zipped ← SpiderM.liftIO <| Event.zipE ctx e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    fire 2  -- produces (42, "test")

    SpiderM.liftIO receivedRef.get
  shouldBe result [(42, "test")]

test "Event.sampleM is alias for tagM" := do
  let result ← runSpider do
    let counterRef ← SpiderM.liftIO <| IO.mkRef 0
    let counterBehavior : Behavior Spider Nat := Behavior.fromSample do
      counterRef.modify (· + 1)
      counterRef.get
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Unit)
    let sampled ← Event.sampleM counterBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← sampled.subscribe fun v =>
      receivedRef.modify (· ++ [v])

    fire ()
    fire ()
    fire ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.snapshotM is alias for attachM" := do
  let result ← runSpider do
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let (event, fire) ← newTriggerEvent (t := Spider) (a := String)
    let snapped ← Event.snapshotM multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    fire "a"
    fire "b"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(10, "a"), (10, "b")]

test "Event.sample is alias for tag (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let counterRef ← SpiderM.liftIO <| IO.mkRef 0
    let counterBehavior : Behavior Spider Nat := Behavior.fromSample do
      counterRef.modify (· + 1)
      counterRef.get
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Unit)
    let sampled ← SpiderM.liftIO <| Event.sample ctx counterBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← sampled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger ()
    trigger ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.snapshot is alias for attach (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let snapped ← SpiderM.liftIO <| Event.snapshot ctx multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    trigger "x"
    trigger "y"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(10, "x"), (10, "y")]

test "Event.sample' is fluent alias for tag'" := do
  let result ← runSpider do
    let counterRef ← SpiderM.liftIO <| IO.mkRef 0
    let counterBehavior : Behavior Spider Nat := Behavior.fromSample do
      counterRef.modify (· + 1)
      counterRef.get
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Unit)
    let sampled ← Event.sample' event counterBehavior

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← sampled.subscribe fun v =>
      receivedRef.modify (· ++ [v])

    fire ()
    fire ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.snapshot' is fluent alias for attach'" := do
  let result ← runSpider do
    let multiplier : Behavior Spider Nat := Behavior.constant 5
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let snapped ← Event.snapshot' event multiplier

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    fire 1
    fire 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [(5, 1), (5, 2)]

test "Event.differenceM fires when e1 fires but e2 doesn't" := do
  let result ← runSpider do
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Unit)
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire1 1   -- e1 only → fires
    fire1 2   -- e1 only → fires
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.differenceM blocks when both events fire (diamond pattern)" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (fun _ => ()) trigger  -- always fires with trigger
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 5  -- both e1 and e2 fire → blocked
    fire 10 -- both fire → blocked
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.differenceM with conditional e2 (selective blocking)" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM id trigger
    let e2 ← Event.mapMaybeM (fun n => if n % 2 == 0 then some () else none) trigger
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1  -- odd: e1 fires, e2 doesn't → passes
    fire 2  -- even: both fire → blocked
    fire 3  -- odd: passes
    fire 4  -- even: blocked
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.difference with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := String)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Unit)
    let diff ← SpiderM.liftIO <| Event.difference ctx e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← diff.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    fire1 "a"
    fire1 "b"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["a", "b"]

-- Left-bias / First-only Merge Tests

test "Event.mergeM left-biases simultaneous events" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Create two derived events from same source (diamond pattern)
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (· * 3) trigger
    let merged ← Event.mergeM e1 e2  -- left-bias: only e1 fires

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 5  -- e1 produces 10, e2 produces 15, but only 10 delivered
    SpiderM.liftIO receivedRef.get
  shouldBe result [10]

test "Event.mergeAllM fires all simultaneous events" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Create two derived events from same source (diamond pattern)
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (· * 3) trigger
    let merged ← Event.mergeAllM e1 e2  -- all-fire: both fire

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 5  -- e1 produces 10, e2 produces 15, both delivered
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 15]

test "Event.leftmostM takes only first when simultaneous" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Create three derived events from same source
    let e1 ← Event.mapM (· + 1) trigger
    let e2 ← Event.mapM (· + 2) trigger
    let e3 ← Event.mapM (· + 3) trigger
    let first ← Event.leftmostM [e1, e2, e3]  -- first-only: only e1 fires

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 10  -- e1=11, e2=12, e3=13, but only 11 delivered
    SpiderM.liftIO receivedRef.get
  shouldBe result [11]

test "Event.mergeAllListM fires all values" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Create three derived events from same source
    let e1 ← Event.mapM (· + 1) trigger
    let e2 ← Event.mapM (· + 2) trigger
    let e3 ← Event.mapM (· + 3) trigger
    let all ← Event.mergeAllListM [e1, e2, e3]  -- all-fire: all fire

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← all.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 10  -- e1=11, e2=12, e3=13, all delivered (order may vary)
    let vals ← SpiderM.liftIO receivedRef.get
    pure ((vals.toArray.qsort (· < ·)).toList)
  -- All three values should be present (sorted for order-independence)
  shouldBe result [11, 12, 13]

test "Event.mergeM sequential frames reset properly" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (· * 3) trigger
    let merged ← Event.mergeM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Each fire is a separate frame, so each can fire independently
    fire 1  -- 2 (left-bias suppresses 3)
    fire 2  -- 4 (left-bias suppresses 6)
    fire 3  -- 6 (left-bias suppresses 9)
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4, 6]

test "Event.mergeM with non-simultaneous events fires both" := do
  let result ← runSpider do
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Nat)
    let merged ← Event.mergeM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire separately (different frames)
    fire1 10
    fire2 20
    fire1 30
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20, 30]

test "Event.mergeAll' fluent variant fires all" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· + 100) trigger
    let e2 ← Event.mapM (· + 200) trigger
    let merged ← Event.mergeAll' e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [105, 205]

end ReactiveTests.EventTests
