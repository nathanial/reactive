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
    let v0 ← liftM (m := IO) <| dyn.sample
    ensure (v0 == 0) "Initial value should be 0"

    -- Fire some events
    liftM (m := IO) <| trigger 5
    let v1 ← liftM (m := IO) <| dyn.sample
    ensure (v1 == 5) "After trigger 5, value should be 5"

    liftM (m := IO) <| trigger 10
    let v2 ← liftM (m := IO) <| dyn.sample
    ensure (v2 == 10) "After trigger 10, value should be 10"

    pure v2

  shouldBe result 10

test "Dynamic.foldDyn accumulates values" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← foldDyn (· + ·) 0 event

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 3

    liftM (m := IO) <| dyn.sample

  shouldBe result 6  -- 0 + 1 + 2 + 3

test "Dynamic.updated fires on changes" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := Nat)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn 0 event

    let changesRef ← liftM (m := IO) <| IO.mkRef ([] : List Nat)
    let _ ← liftM (m := IO) <| dyn.updated.subscribe fun n =>
      changesRef.modify (· ++ [n])

    liftM (m := IO) <| trigger 1
    liftM (m := IO) <| trigger 2
    liftM (m := IO) <| trigger 3

    liftM (m := IO) changesRef.get

  shouldBe result [1, 2, 3]

test "Dynamic.current returns a Behavior" := do
  let result ← runSpider do
    let pair ← newTriggerEvent (t := Spider) (a := String)
    let event := pair.1
    let trigger := pair.2
    let dyn ← holdDyn "initial" event

    let behavior := dyn.current

    liftM (m := IO) <| trigger "updated"

    sample behavior

  shouldBe result "updated"

test "Dynamic.mapM transforms values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 10 event
    let mapped ← Dynamic.mapM (· * 2) dyn

    let v0 ← SpiderM.liftIO <| mapped.sample
    SpiderM.liftIO <| trigger 5
    let v1 ← SpiderM.liftIO <| mapped.sample
    pure (v0, v1)
  shouldBe result (20, 10)

test "Dynamic.zipWithM combines dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 10 e1
    let d2 ← holdDyn 20 e2
    let combined ← Dynamic.zipWithM (· + ·) d1 d2

    let v0 ← SpiderM.liftIO <| combined.sample
    SpiderM.liftIO <| t1 5
    let v1 ← SpiderM.liftIO <| combined.sample
    SpiderM.liftIO <| t2 3
    let v2 ← SpiderM.liftIO <| combined.sample
    pure (v0, v1, v2)
  shouldBe result (30, 25, 8)

test "Dynamic.pureM creates constant dynamic" := do
  let result ← runSpider do
    let dyn ← Dynamic.pureM 42
    SpiderM.liftIO <| dyn.sample
  shouldBe result 42

test "Dynamic.apM applies function dynamic" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat → Nat)
    let (e2, _t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let df ← holdDyn (· + 1) e1
    let da ← holdDyn 10 e2
    let applied ← Dynamic.apM df da

    let v0 ← SpiderM.liftIO <| applied.sample
    SpiderM.liftIO <| t1 (· * 2)
    let v1 ← SpiderM.liftIO <| applied.sample
    pure (v0, v1)
  shouldBe result (11, 20)

test "Dynamic.zipWith3M combines three dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 1 e1
    let d2 ← holdDyn 2 e2
    let d3 ← holdDyn 3 e3
    let combined ← Dynamic.zipWith3M (fun a b c => a + b + c) d1 d2 d3

    let v0 ← SpiderM.liftIO <| combined.sample
    SpiderM.liftIO <| t1 10
    let v1 ← SpiderM.liftIO <| combined.sample
    SpiderM.liftIO <| t2 20
    let v2 ← SpiderM.liftIO <| combined.sample
    SpiderM.liftIO <| t3 30
    let v3 ← SpiderM.liftIO <| combined.sample
    pure (v0, v1, v2, v3)
  shouldBe result (6, 15, 33, 60)

-- New tests for full coverage

test "Dynamic.value is alias for sample" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 100 event
    let v0 ← SpiderM.liftIO <| Dynamic.value dyn
    SpiderM.liftIO <| trigger 200
    let v1 ← SpiderM.liftIO <| Dynamic.value dyn
    pure (v0, v1)
  shouldBe result (100, 200)

test "Dynamic.toBehavior returns current behavior" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    let dyn ← holdDyn "initial" event
    let behavior := Dynamic.toBehavior dyn
    let v0 ← behavior.sample
    SpiderM.liftIO <| trigger "changed"
    let v1 ← behavior.sample
    pure (v0, v1)
  shouldBe result ("initial", "changed")

test "Dynamic.pure' creates constant dynamic via IO" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let dyn ← SpiderM.liftIO <| Dynamic.pure' ctx 99
    SpiderM.liftIO <| dyn.sample
  shouldBe result 99

test "Dynamic.tagUpdated tags update event with constant value" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let tagged ← SpiderM.liftIO <| Dynamic.tagUpdated ctx "fired" dyn

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| tagged.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result ["fired", "fired", "fired"]

test "Dynamic.changes provides old and new values" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let changesEvent ← SpiderM.liftIO <| Dynamic.changes ctx dyn

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← SpiderM.liftIO <| changesEvent.subscribe fun pair =>
      receivedRef.modify (· ++ [pair])

    SpiderM.liftIO <| trigger 5   -- old=0, new=5
    SpiderM.liftIO <| trigger 10  -- old=5, new=10
    SpiderM.liftIO <| trigger 3   -- old=10, new=3
    SpiderM.liftIO receivedRef.get
  shouldBe result [(0, 5), (5, 10), (10, 3)]

test "Dynamic.new allows manual value updates" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (dyn, update) ← SpiderM.liftIO <| Dynamic.new ctx 0
    let v0 ← SpiderM.liftIO <| dyn.sample
    SpiderM.liftIO <| update 42
    let v1 ← SpiderM.liftIO <| dyn.sample
    SpiderM.liftIO <| update 100
    let v2 ← SpiderM.liftIO <| dyn.sample
    pure (v0, v1, v2)
  shouldBe result (0, 42, 100)

test "Dynamic.new update fires updated event" := do
  let result ← runSpider do
    let ctx ← SpiderM.getTimelineCtx
    let (dyn, update) ← SpiderM.liftIO <| Dynamic.new ctx 0

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| dyn.updated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| update 1
    SpiderM.liftIO <| update 2
    SpiderM.liftIO <| update 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3]

test "Dynamic.zip pairs two dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := String)
    let d1 ← holdDyn 10 e1
    let d2 ← holdDyn "hello" e2
    let zipped ← Dynamic.zip' d1 d2

    let v0 ← SpiderM.liftIO <| zipped.sample
    SpiderM.liftIO <| t1 20
    let v1 ← SpiderM.liftIO <| zipped.sample
    SpiderM.liftIO <| t2 "world"
    let v2 ← SpiderM.liftIO <| zipped.sample
    pure (v0, v1, v2)
  shouldBe result ((10, "hello"), (20, "hello"), (20, "world"))

#generate_tests

end ReactiveTests.DynamicTests
