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

test "Event.onceM takes only first occurrence" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← Event.onceM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1]

test "Event.once with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let first ← SpiderM.liftIO <| Event.once ctx event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| first.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    SpiderM.liftIO <| trigger "first"
    SpiderM.liftIO <| trigger "second"
    SpiderM.liftIO <| trigger "third"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["first"]

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

test "Fluent Event.map' enables chaining" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Use explicit form: Event.map' event f
    let mapped ← Event.map' event (· * 2)

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| mapped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 4, 6]

test "Fluent chaining with bind" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Chain: map then filter using bind with explicit form
    let processed ← Event.map' event (· * 2) >>= (Event.filter' · (· > 3))

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| processed.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1  -- 1*2=2, filtered out (not > 3)
    SpiderM.liftIO <| trigger 2  -- 2*2=4, passes
    SpiderM.liftIO <| trigger 3  -- 3*2=6, passes
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
    let _ ← SpiderM.liftIO <| merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| t2 2
    SpiderM.liftIO <| t1 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

-- New tests for full coverage

test "Event.never never fires" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let neverEvent ← SpiderM.liftIO <| Event.never (t := Spider) ctx (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| neverEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])
    -- The event never fires, so receivedRef should stay empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.neverM never fires (SpiderM version)" := do
  let result ← runSpider do
    let neverEvent ← Event.neverM (a := String)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| neverEvent.subscribe fun s =>
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
    let _ ← SpiderM.liftIO <| filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1  -- odd, filtered out
    SpiderM.liftIO <| trigger 4  -- even, becomes 2
    SpiderM.liftIO <| trigger 5  -- odd, filtered out
    SpiderM.liftIO <| trigger 10 -- even, becomes 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 5]

test "Event.attachM pairs event with behavior value" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let counterBehavior := Behavior.constant 42
    let attached ← Event.attachM counterBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| trigger "hello"
    SpiderM.liftIO <| trigger "world"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(42, "hello"), (42, "world")]

test "Event.attachWithM applies function to behavior and event" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let multiplierBehavior := Behavior.constant 10
    let attached ← Event.attachWithM (· * ·) multiplierBehavior event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| attached.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20, 30]

test "Event.fanEitherM splits Sum event into two" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Sum Nat String)
    let (leftEvent, rightEvent) ← Event.fanEitherM event

    let leftRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let rightRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| leftEvent.subscribe fun n =>
      leftRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| rightEvent.subscribe fun s =>
      rightRef.modify (· ++ [s])

    SpiderM.liftIO <| trigger (Sum.inl 1)
    SpiderM.liftIO <| trigger (Sum.inr "hello")
    SpiderM.liftIO <| trigger (Sum.inl 2)
    SpiderM.liftIO <| trigger (Sum.inr "world")

    let left ← SpiderM.liftIO leftRef.get
    let right ← SpiderM.liftIO rightRef.get
    pure (left, right)
  shouldBe result ([1, 2], ["hello", "world"])

test "Event.fanM/selectM dispatches per key" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Std.HashMap Nat String)
    let fan ← Event.fanM event
    let ones ← Event.selectM fan 1
    let twos ← Event.selectM fan 2

    let onesRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let twosRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| ones.subscribe fun v =>
      onesRef.modify (· ++ [v])
    let _ ← SpiderM.liftIO <| twos.subscribe fun v =>
      twosRef.modify (· ++ [v])

    let map1 : Std.HashMap Nat String := Std.HashMap.ofList [(1, "a"), (3, "skip")]
    let map2 : Std.HashMap Nat String := Std.HashMap.ofList [(2, "b"), (1, "c")]
    let map3 : Std.HashMap Nat String := Std.HashMap.ofList [(3, "ignore")]

    SpiderM.liftIO <| trigger map1
    SpiderM.liftIO <| trigger map2
    SpiderM.liftIO <| trigger map3

    let onesVals ← SpiderM.liftIO onesRef.get
    let twosVals ← SpiderM.liftIO twosRef.get
    pure (onesVals, twosVals)
  shouldBe result (["a", "c"], ["b"])

test "Event.accumulateM maintains running state" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let accumulated ← Event.accumulateM (· + ·) 100 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1   -- 100 + 1 = 101
    SpiderM.liftIO <| trigger 2   -- 101 + 2 = 103
    SpiderM.liftIO <| trigger 10  -- 103 + 10 = 113
    SpiderM.liftIO receivedRef.get
  shouldBe result [101, 103, 113]

test "Event.delayFrameM fires after current frame" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayFrameM event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [s!"now {n}"])
    let _ ← SpiderM.liftIO <| delayed.subscribe fun n =>
      receivedRef.modify (· ++ [s!"later {n}"])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result ["now 1", "later 1", "now 2", "later 2"]

test "Event.mergeListM with empty list returns never event" := do
  let result ← runSpider do
    let merged ← Event.mergeListM ([] : List (Event Spider Nat))
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← SpiderM.liftIO <| merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])
    -- Nothing to fire, so should be empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.leftmostM with empty list returns never event" := do
  let result ← runSpider do
    let first ← Event.leftmostM ([] : List (Event Spider Nat))
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| first.subscribe fun n =>
      receivedRef.modify (· ++ [n])
    -- Nothing to fire, so should be empty
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "rapid event firing preserves order" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire many events rapidly
    for i in [0:100] do
      SpiderM.liftIO <| trigger i
    SpiderM.liftIO receivedRef.get
  shouldBe result (List.range 100)

test "Event.attachM tracks dynamic behavior changes" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let (countEvent, countFire) ← newTriggerEvent (t := Spider) (a := Nat)
    let counterDyn ← holdDyn 0 countEvent
    let attached ← Event.attachM counterDyn.current event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| trigger "a"
    SpiderM.liftIO <| countFire 10
    SpiderM.liftIO <| trigger "b"
    SpiderM.liftIO <| countFire 20
    SpiderM.liftIO <| trigger "c"
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
    let _ ← SpiderM.liftIO <| tagged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.attach pairs behavior value with event value" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let attached ← SpiderM.liftIO <| Event.attach ctx multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| attached.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| trigger "a"
    SpiderM.liftIO <| trigger "b"
    SpiderM.liftIO receivedRef.get
  shouldBe result [(10, "a"), (10, "b")]

test "Event.attachWith combines behavior and event with function" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let attached ← SpiderM.liftIO <| Event.attachWith ctx (· * ·) multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| attached.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
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
    let _ ← SpiderM.liftIO <| gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1        -- gate open
    SpiderM.liftIO <| gateRef.set false
    SpiderM.liftIO <| trigger 2        -- gate closed
    SpiderM.liftIO <| gateRef.set true
    SpiderM.liftIO <| trigger 3        -- gate open
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.accumulate maintains running total (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let accumulated ← SpiderM.liftIO <| Event.accumulate ctx (· + ·) 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 5
    SpiderM.liftIO <| trigger 10
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [5, 15, 18]

test "Event.scan is alias for accumulate" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let scanned ← SpiderM.liftIO <| Event.scan ctx (· * ·) 1 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| scanned.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 2  -- 1 * 2 = 2
    SpiderM.liftIO <| trigger 3  -- 2 * 3 = 6
    SpiderM.liftIO <| trigger 4  -- 6 * 4 = 24
    SpiderM.liftIO receivedRef.get
  shouldBe result [2, 6, 24]

test "Event.takeN limits occurrences (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let taken ← SpiderM.liftIO <| Event.takeN ctx 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| taken.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3  -- should not fire
    SpiderM.liftIO <| trigger 4  -- should not fire
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.dropN skips occurrences (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dropped ← SpiderM.liftIO <| Event.dropN ctx 2 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| dropped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1  -- dropped
    SpiderM.liftIO <| trigger 2  -- dropped
    SpiderM.liftIO <| trigger 3  -- passes
    SpiderM.liftIO <| trigger 4  -- passes
    SpiderM.liftIO receivedRef.get
  shouldBe result [3, 4]

test "Event.leftmost with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let first ← SpiderM.liftIO <| Event.leftmost ctx [e1, e2]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| first.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t2 10
    SpiderM.liftIO <| t1 20
    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20]

test "Event.fanEither with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Sum Nat String)
    let (leftEvent, rightEvent) ← SpiderM.liftIO <| Event.fanEither ctx event

    let leftRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let rightRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| leftEvent.subscribe fun n =>
      leftRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| rightEvent.subscribe fun s =>
      rightRef.modify (· ++ [s])

    SpiderM.liftIO <| trigger (Sum.inl 1)
    SpiderM.liftIO <| trigger (Sum.inr "a")
    SpiderM.liftIO <| trigger (Sum.inl 2)

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
    let _ ← SpiderM.liftIO <| merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])

    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| t2 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [[1], [2]]

test "Event.mapM cleans up on scope dispose" := do
  let disposedRef ← IO.mkRef false
  let _ ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let _ ← SpiderM.withAutoDisposeScope do
      let mapped ← Event.mapM (· + 1) event
      -- Subscribe so we can verify cleanup
      let _ ← SpiderM.liftIO <| mapped.subscribe fun _ => pure ()
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
    let _ ← SpiderM.liftIO <| constEvent.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 42
    SpiderM.liftIO <| trigger 999
    SpiderM.liftIO receivedRef.get
  shouldBe result ["fired", "fired", "fired"]

test "Event.mapConst' maps all values to constant (fluent style)" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let constEvent ← Event.mapConst' event 100

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| constEvent.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger "hello"
    SpiderM.liftIO <| trigger "world"
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
    let _ ← SpiderM.liftIO <| zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| fire 5
    SpiderM.liftIO receivedRef.get
  -- e1 produces 10, e2 produces "5" - both fire simultaneously from same source
  shouldBe result [(10, "5")]

test "Event.zipEM ignores non-simultaneous events" := do
  let result ← runSpider do
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := String)
    let zipped ← Event.zipEM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    -- Fire separately (different frames)
    SpiderM.liftIO <| fire1 10
    SpiderM.liftIO <| fire2 "hello"

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
    let _ ← SpiderM.liftIO <| zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    -- Fire twice, each creating a simultaneous pair
    SpiderM.liftIO <| fire 1  -- produces (1, "x")
    SpiderM.liftIO <| fire 2  -- produces (2, "xx")

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
    let _ ← SpiderM.liftIO <| zipped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| fire 2  -- produces (42, "test")

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
    let _ ← SpiderM.liftIO <| sampled.subscribe fun v =>
      receivedRef.modify (· ++ [v])

    SpiderM.liftIO <| fire ()
    SpiderM.liftIO <| fire ()
    SpiderM.liftIO <| fire ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Event.snapshotM is alias for attachM" := do
  let result ← runSpider do
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let (event, fire) ← newTriggerEvent (t := Spider) (a := String)
    let snapped ← Event.snapshotM multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| fire "a"
    SpiderM.liftIO <| fire "b"
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
    let _ ← SpiderM.liftIO <| sampled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.snapshot is alias for attach (pure IO)" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let multiplier : Behavior Spider Nat := Behavior.constant 10
    let snapped ← SpiderM.liftIO <| Event.snapshot ctx multiplier event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← SpiderM.liftIO <| snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| trigger "x"
    SpiderM.liftIO <| trigger "y"
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
    let _ ← SpiderM.liftIO <| sampled.subscribe fun v =>
      receivedRef.modify (· ++ [v])

    SpiderM.liftIO <| fire ()
    SpiderM.liftIO <| fire ()
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.snapshot' is fluent alias for attach'" := do
  let result ← runSpider do
    let multiplier : Behavior Spider Nat := Behavior.constant 5
    let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let snapped ← Event.snapshot' event multiplier

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← SpiderM.liftIO <| snapped.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| fire 1
    SpiderM.liftIO <| fire 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [(5, 1), (5, 2)]

test "Event.differenceM fires when e1 fires but e2 doesn't" := do
  let result ← runSpider do
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Unit)
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| fire1 1   -- e1 only → fires
    SpiderM.liftIO <| fire1 2   -- e1 only → fires
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "Event.differenceM blocks when both events fire (diamond pattern)" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (fun _ => ()) trigger  -- always fires with trigger
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| fire 5  -- both e1 and e2 fire → blocked
    SpiderM.liftIO <| fire 10 -- both fire → blocked
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "Event.differenceM with conditional e2 (selective blocking)" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let e1 ← Event.mapM id trigger
    let e2 ← Event.mapMaybeM (fun n => if n % 2 == 0 then some () else none) trigger
    let diff ← Event.differenceM e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| diff.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| fire 1  -- odd: e1 fires, e2 doesn't → passes
    SpiderM.liftIO <| fire 2  -- even: both fire → blocked
    SpiderM.liftIO <| fire 3  -- odd: passes
    SpiderM.liftIO <| fire 4  -- even: blocked
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3]

test "Event.difference with pure IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (e1, fire1) ← newTriggerEvent (t := Spider) (a := String)
    let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Unit)
    let diff ← SpiderM.liftIO <| Event.difference ctx e1 e2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| diff.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    SpiderM.liftIO <| fire1 "a"
    SpiderM.liftIO <| fire1 "b"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["a", "b"]

#generate_tests

end ReactiveTests.EventTests
