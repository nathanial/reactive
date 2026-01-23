import Crucible
import Reactive

namespace ReactiveTests.IntegrationHelperTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Integration Helper Tests"

test "fromRef creates event from ref updates" := do
  let result ← runSpider do
    let (event, update, ref) ← fromRef (0 : Nat)

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    update 10
    update 20
    update 30

    SpiderM.liftIO receivedRef.get
  shouldBe result [10, 20, 30]

test "fromRef returns correct ref for reading" := do
  let result ← runSpider do
    let (_, update, ref) ← fromRef (0 : Nat)

    let v0 ← SpiderM.liftIO ref.get
    update 42
    let v1 ← SpiderM.liftIO ref.get
    update 100
    let v2 ← SpiderM.liftIO ref.get

    pure (v0, v1, v2)
  shouldBe result (0, 42, 100)

test "fromRefWithBehavior provides both event and behavior" := do
  let result ← runSpider do
    let (event, behavior, update) ← fromRefWithBehavior "initial"

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← event.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    let b0 ← behavior.sample
    update "updated"
    let b1 ← behavior.sample

    let events ← SpiderM.liftIO receivedRef.get
    pure (b0, b1, events)
  shouldBe result ("initial", "updated", ["updated"])

test "toCallback exports event as callback function" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let callbackRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    -- Export event as callback
    toCallback event fun n => callbackRef.modify (· ++ [n])

    -- Fire the event
    trigger 1
    trigger 2
    trigger 3

    SpiderM.liftIO callbackRef.get
  shouldBe result [1, 2, 3]

test "fromRef event fires on each update" := do
  let result ← runSpider do
    let (event, update, _) ← fromRef (0 : Nat)

    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← event.subscribe fun _ =>
      countRef.modify (· + 1)

    update 1
    update 1  -- Same value still fires
    update 1
    update 2

    SpiderM.liftIO countRef.get
  shouldBe result 4

test "fromRefWithBehavior behavior always returns current value" := do
  let result ← runSpider do
    let (_, behavior, update) ← fromRefWithBehavior (100 : Nat)

    let samples ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let v0 ← behavior.sample
    SpiderM.liftIO <| (samples.modify (· ++ [v0]))

    update 200
    let v1 ← behavior.sample
    SpiderM.liftIO <| (samples.modify (· ++ [v1]))

    update 300
    let v2 ← behavior.sample
    SpiderM.liftIO <| (samples.modify (· ++ [v2]))

    SpiderM.liftIO samples.get
  shouldBe result [100, 200, 300]


end ReactiveTests.IntegrationHelperTests
