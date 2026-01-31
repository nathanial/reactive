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
    let v0 ← dyn.sample
    ensure (v0 == 0) "Initial value should be 0"

    -- Fire some events
    trigger 5
    let v1 ← dyn.sample
    ensure (v1 == 5) "After trigger 5, value should be 5"

    trigger 10
    let v2 ← dyn.sample
    ensure (v2 == 10) "After trigger 10, value should be 10"

    pure v2

  shouldBe result 10

test "Dynamic.foldDyn accumulates values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← foldDyn (· + ·) 0 event

    trigger 1
    trigger 2
    trigger 3

    dyn.sample

  shouldBe result 6  -- 0 + 1 + 2 + 3

test "Dynamic.updated fires on changes" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn 0 event

    let changesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← dyn.updated.subscribe fun n =>
      changesRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3

    SpiderM.liftIO changesRef.get

  shouldBe result [1, 2, 3]

test "Dynamic.holdUniqDynM filters duplicate updates" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let uniq ← Dynamic.holdUniqDynM dyn

    let changesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← uniq.updated.subscribe fun n =>
      changesRef.modify (· ++ [n])

    trigger 0  -- same as initial, should not fire
    trigger 1
    trigger 1
    trigger 2
    trigger 2

    SpiderM.liftIO changesRef.get
  shouldBe result [1, 2]

test "Dynamic.current returns a Behavior" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := String)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn "initial" event

    let behavior := dyn.current

    trigger "updated"

    sample behavior

  shouldBe result "updated"

test "Dynamic.mapM transforms values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 10 event
    let mapped ← Dynamic.mapM (· * 2) dyn

    let v0 ← mapped.sample
    trigger 5
    let v1 ← mapped.sample
    pure (v0, v1)
  shouldBe result (20, 10)

test "Dynamic.zipWithM combines dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 10 e1
    let d2 ← holdDyn 20 e2
    let combined ← Dynamic.zipWithM (· + ·) d1 d2

    let v0 ← combined.sample
    t1 5
    let v1 ← combined.sample
    t2 3
    let v2 ← combined.sample
    pure (v0, v1, v2)
  shouldBe result (30, 25, 8)

test "Dynamic.pureM creates constant dynamic" := do
  let result ← runSpider do
    let dyn ← Dynamic.pureM 42
    dyn.sample
  shouldBe result 42

test "Dynamic.apM applies function dynamic" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat → Nat)
    let (e2, _t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let df ← holdDyn (· + 1) e1
    let da ← holdDyn 10 e2
    let applied ← Dynamic.apM df da

    let v0 ← applied.sample
    t1 (· * 2)
    let v1 ← applied.sample
    pure (v0, v1)
  shouldBe result (11, 20)

test "Dynamic.Builder supports Functor and Applicative" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 2 e1
    let d2 ← holdDyn 5 e2
    let ctx ← SpiderM.getTimelineCtx
    let built ←
      Dynamic.Builder.run ctx do
        (fun a b => a * 2 + b) <$> Dynamic.Builder.of d1 <*> Dynamic.Builder.of d2

    let v0 ← built.sample
    t1 4
    let v1 ← built.sample
    t2 10
    let v2 ← built.sample
    pure (v0, v1, v2)
  shouldBe result (9, 13, 18)

test "Dynamic.zipWith3M combines three dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 1 e1
    let d2 ← holdDyn 2 e2
    let d3 ← holdDyn 3 e3
    let combined ← Dynamic.zipWith3M (fun a b c => a + b + c) d1 d2 d3

    let v0 ← combined.sample
    t1 10
    let v1 ← combined.sample
    t2 20
    let v2 ← combined.sample
    t3 30
    let v3 ← combined.sample
    pure (v0, v1, v2, v3)
  shouldBe result (6, 15, 33, 60)

-- New tests for full coverage

test "Dynamic.value is alias for sample" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 100 event
    let v0 ← Dynamic.value dyn
    trigger 200
    let v1 ← Dynamic.value dyn
    pure (v0, v1)
  shouldBe result (100, 200)

test "Dynamic.toBehavior returns current behavior" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let dyn ← holdDyn "initial" event
    let behavior := Dynamic.toBehavior dyn
    let v0 ← behavior.sample
    trigger "changed"
    let v1 ← behavior.sample
    pure (v0, v1)
  shouldBe result ("initial", "changed")

test "Dynamic.pure' creates constant dynamic via IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let dyn ← Dynamic.pure' ctx 99
    dyn.sample
  shouldBe result 99

test "Dynamic.tagUpdated tags update event with constant value" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let tagged ← Dynamic.tagUpdated ctx "fired" dyn

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← tagged.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result ["fired", "fired", "fired"]

test "Dynamic.changes provides old and new values" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let changesEvent ← Dynamic.changes ctx dyn

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← changesEvent.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    trigger 5   -- old=0, new=5
    trigger 10  -- old=5, new=10
    trigger 3   -- old=10, new=3
    SpiderM.liftIO receivedRef.get
  shouldBe result [(0, 5), (5, 10), (10, 3)]

test "Dynamic.zip pairs two dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := String)
    let d1 ← holdDyn 10 e1
    let d2 ← holdDyn "hello" e2
    let zipped ← Dynamic.zip' d1 d2

    let v0 ← zipped.sample
    t1 20
    let v1 ← zipped.sample
    t2 "world"
    let v2 ← zipped.sample
    pure (v0, v1, v2)
  shouldBe result ((10, "hello"), (20, "hello"), (20, "world"))

test "Multiple Dynamic.mapM from same source" := do
  -- Test case: Two derived dynamics from the same source Dynamic
  -- This simulates the pattern used in TextInput components sharing focusedInput
  let result ← runSpider do
    -- Create a source dynamic using proper FRP pattern (event + holdDyn)
    let (sourceEvent, fireSource) ← newTriggerEvent (t := Spider) (a := Option String)
    let source ← holdDyn none sourceEvent

    -- Create TWO derived dynamics from the same source (like two TextInputs)
    -- Each checks if the source equals their own name
    let derived1 ← Dynamic.mapM (· == some "input1") source
    let derived2 ← Dynamic.mapM (· == some "input2") source

    -- Initial state: both should be false
    let v1_0 ← derived1.sample
    let v2_0 ← derived2.sample

    -- Set focus to input1
    fireSource (some "input1")
    let v1_1 ← derived1.sample
    let v2_1 ← derived2.sample

    -- Set focus to input2
    fireSource (some "input2")
    let v1_2 ← derived1.sample
    let v2_2 ← derived2.sample

    -- Clear focus
    fireSource none
    let v1_3 ← derived1.sample
    let v2_3 ← derived2.sample

    pure ((v1_0, v2_0), (v1_1, v2_1), (v1_2, v2_2), (v1_3, v2_3))

  -- Expected: derived1 is true only when source == some "input1"
  --           derived2 is true only when source == some "input2"
  shouldBe result (
    (false, false),  -- initial: none
    (true, false),   -- focus input1
    (false, true),   -- focus input2
    (false, false)   -- cleared
  )

test "Multiple Dynamic.mapM with subscriptions" := do
  -- More complex test: multiple derived dynamics with subscriptions
  -- This more closely matches the TextInput crash scenario
  let result ← runSpider do
    -- Create a source dynamic using proper FRP pattern (event + holdDyn)
    let (sourceEvent, fireSource) ← newTriggerEvent (t := Spider) (a := Option String)
    let source ← holdDyn none sourceEvent

    -- Create derived dynamics
    let derived1 ← Dynamic.mapM (· == some "input1") source
    let derived2 ← Dynamic.mapM (· == some "input2") source

    -- Subscribe to updates on both derived dynamics
    let updates1 ← SpiderM.liftIO <| IO.mkRef ([] : List Bool)
    let updates2 ← SpiderM.liftIO <| IO.mkRef ([] : List Bool)

    let _ ← derived1.updated.subscribe fun b =>
      updates1.modify (· ++ [b])
    let _ ← derived2.updated.subscribe fun b =>
      updates2.modify (· ++ [b])

    -- Trigger updates
    fireSource (some "input1")
    fireSource (some "input2")
    fireSource none

    let u1 ← SpiderM.liftIO <| updates1.get
    let u2 ← SpiderM.liftIO <| updates2.get
    pure (u1, u2)

  -- mapM doesn't deduplicate, so all updates fire:
  -- derived1: true (input1), false (input2), false (none)
  -- derived2: false (input1), true (input2), false (none)
  -- Use mapUniqM instead if deduplication is desired.
  shouldBe result (
    [true, false, false],   -- derived1: all 3 updates fire
    [false, true, false]    -- derived2: all 3 updates fire
  )

-- Tests for Option-handling Dynamic combinators

-- Helper structure for bindOptionM tests
structure TestItem where
  id : Nat
  valueDyn : Dynamic Spider Nat

test "Dynamic.bindOptionM with initial none shows default" := do
  let result ← runSpider do
    let (event, _trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn none event
    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 999
    bound.sample
  shouldBe result 999

test "Dynamic.bindOptionM with initial some tracks inner dynamic" := do
  let result ← runSpider do
    -- Create an inner dynamic with value 10
    let (innerEvent, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 10 innerEvent
    let item : TestItem := { id := 1, valueDyn := innerDyn }

    let (event, _trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn (some item) event
    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 999
    bound.sample
  shouldBe result 10

test "Dynamic.bindOptionM switching from some to none shows default" := do
  let result ← runSpider do
    -- Create an inner dynamic with value 10
    let (innerEvent, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 10 innerEvent
    let item : TestItem := { id := 1, valueDyn := innerDyn }

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn (some item) event
    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 999

    let v0 ← bound.sample
    trigger none
    let v1 ← bound.sample
    pure (v0, v1)
  shouldBe result (10, 999)

test "Dynamic.bindOptionM switching from none to some tracks new inner" := do
  let result ← runSpider do
    -- Create an inner dynamic with value 42
    let (innerEvent, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 42 innerEvent
    let item : TestItem := { id := 1, valueDyn := innerDyn }

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn none event
    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 999

    let v0 ← bound.sample
    trigger (some item)
    let v1 ← bound.sample
    pure (v0, v1)
  shouldBe result (999, 42)

test "Dynamic.bindOptionM switches inner subscription between some values" := do
  let result ← runSpider do
    -- Create two inner dynamics with different values
    let (innerEvent1, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn1 ← holdDyn 100 innerEvent1
    let item1 : TestItem := { id := 1, valueDyn := innerDyn1 }

    let (innerEvent2, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn2 ← holdDyn 200 innerEvent2
    let item2 : TestItem := { id := 2, valueDyn := innerDyn2 }

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn (some item1) event
    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 0

    let v0 ← bound.sample  -- item1's value = 100
    trigger (some item2)
    let v1 ← bound.sample  -- item2's value = 200
    trigger (some item1)
    let v2 ← bound.sample  -- back to item1's value = 100
    pure (v0, v1, v2)
  shouldBe result (100, 200, 100)

test "Dynamic.bindOptionM tracks inner dynamic updates" := do
  let result ← runSpider do
    -- Create an inner dynamic that we can update
    let (innerEvent, innerTrigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 10 innerEvent
    let item : TestItem := { id := 1, valueDyn := innerDyn }

    let (optEvent, triggerOpt) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn none optEvent

    let bound ← Dynamic.bindOptionM optDyn (·.valueDyn) 0

    -- Initially none → default
    let v0 ← bound.sample

    -- Switch to some item
    triggerOpt (some item)
    let v1 ← bound.sample

    -- Update the inner dynamic
    innerTrigger 20
    let v2 ← bound.sample

    pure (v0, v1, v2)
  shouldBe result (0, 10, 20)

test "Dynamic.switchOptionM with none shows default" := do
  let result ← runSpider do
    let (event, _trigger) ← newTriggerEvent (t := Spider) (a := Option (Dynamic Spider Nat))
    let optDynOfDyn ← holdDyn none event
    let switched ← Dynamic.switchOptionM optDynOfDyn 42
    switched.sample
  shouldBe result 42

test "Dynamic.switchOptionM with some tracks inner dynamic" := do
  let result ← runSpider do
    let (innerEvent, innerTrigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 100 innerEvent
    let (outerEvent, _outerTrigger) ← newTriggerEvent (t := Spider) (a := Option (Dynamic Spider Nat))
    let optDynOfDyn ← holdDyn (some innerDyn) outerEvent
    let switched ← Dynamic.switchOptionM optDynOfDyn 0

    let v0 ← switched.sample
    innerTrigger 200
    let v1 ← switched.sample
    pure (v0, v1)
  shouldBe result (100, 200)

test "Dynamic.bindOption' fluent style works correctly" := do
  let result ← runSpider do
    -- Create an inner dynamic
    let (innerEvent, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let innerDyn ← holdDyn 105 innerEvent
    let item : TestItem := { id := 1, valueDyn := innerDyn }

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Option TestItem)
    let optDyn ← holdDyn none event
    let bound ← Dynamic.bindOption' optDyn 999 (·.valueDyn)

    let v0 ← bound.sample
    trigger (some item)
    let v1 ← bound.sample
    pure (v0, v1)
  shouldBe result (999, 105)

-- Tests for Dynamic.memoizeM

test "Dynamic.memoizeM skips computation when input unchanged" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let source ← holdDyn 10 event

    -- Create a memoized dynamic with pure function
    let memoized ← Dynamic.memoizeM (· * 2) source

    -- Initial value
    let v0 ← memoized.sample

    -- Fire same value - computation should be skipped, no event fires
    trigger 10
    let v1 ← memoized.sample

    -- Fire different value - computation should run
    trigger 20
    let v2 ← memoized.sample

    -- Fire same value again - computation should be skipped
    trigger 20
    let v3 ← memoized.sample

    pure (v0, v1, v2, v3)

  -- Values should be correct
  shouldBe result (20, 20, 40, 40)

test "Dynamic.memoizeM fires event only when input changes" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let source ← holdDyn 5 event
    let memoized ← Dynamic.memoizeM (· * 3) source

    let eventsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← memoized.updated.subscribe fun n =>
      eventsRef.modify (· ++ [n])

    trigger 5   -- same as initial, should NOT fire
    trigger 10  -- different, should fire
    trigger 10  -- same as previous, should NOT fire
    trigger 15  -- different, should fire
    trigger 5   -- different (back to original), should fire

    SpiderM.liftIO eventsRef.get

  -- Only 3 events: 10*3=30, 15*3=45, 5*3=15
  shouldBe result [30, 45, 15]

test "Dynamic.memoize' fluent style works correctly" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let source ← holdDyn "hello" event

    -- Use fluent syntax via explicit namespace
    let memoized ← Dynamic.memoize' source (·.length)

    let v0 ← memoized.sample
    trigger "hello"  -- same, skip
    trigger "world"  -- different string (same length but different input!)
    let v1 ← memoized.sample
    trigger "hi"     -- different
    let v2 ← memoized.sample

    pure (v0, v1, v2)

  -- Values should be string lengths
  -- "hello" -> 5, "world" -> 5, "hi" -> 2
  shouldBe result (5, 5, 2)

test "Dynamic.memoizeM vs mapUniqM: input-based vs output-based dedup" := do
  -- This test demonstrates the difference between memoizeM (input-based)
  -- and mapUniqM (output-based) deduplication

  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let source ← holdDyn 1 event

    -- memoizeM: skips computation AND event if INPUT unchanged
    let memoized ← Dynamic.memoizeM (· % 3) source
    -- Maps: 1->1, 4->1, 7->1, 2->2, etc.

    -- mapUniqM: always computes, but skips event if OUTPUT unchanged
    let mapUniq ← Dynamic.mapUniqM (· % 3) source

    let memoEventsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let mapUniqEventsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← memoized.updated.subscribe fun n =>
      memoEventsRef.modify (· ++ [n])
    let _ ← mapUniq.updated.subscribe fun n =>
      mapUniqEventsRef.modify (· ++ [n])

    -- Fire sequence: 1 -> 4 -> 4 -> 7
    -- 1 % 3 = 1 (initial)
    -- 4 % 3 = 1 (same output, different input)
    -- 4 (same input)
    -- 7 % 3 = 1 (same output, different input)

    trigger 4
    trigger 4
    trigger 7

    let memoEvents ← SpiderM.liftIO memoEventsRef.get
    let mapUniqEvents ← SpiderM.liftIO mapUniqEventsRef.get

    pure (memoEvents, mapUniqEvents)

  -- memoizeM fires for each new INPUT: 4 and 7 (1 is initial, skips duplicate 4)
  -- Output is always 1, so events are [1, 1]
  shouldBe result.1 [1, 1]

  -- mapUniqM fires only when OUTPUT changes: none (all outputs are 1)
  shouldBe result.2 []


end ReactiveTests.DynamicTests
