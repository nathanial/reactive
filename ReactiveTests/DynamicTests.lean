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


end ReactiveTests.DynamicTests
