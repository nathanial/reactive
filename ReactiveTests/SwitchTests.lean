import Crucible
import Reactive

namespace ReactiveTests.SwitchTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Switch Tests"

test "switchDyn switches to new event when dynamic changes" := do
  let result ← runSpider do
    -- Create two events
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create a dynamic that starts with e1
    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Event Spider Nat)
    let dynEvent ← holdDyn e1 switchEvent

    -- Create switched event
    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynWithId dynEvent nodeId

    -- Collect fired values
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| switched.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire e1
    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| t1 2

    -- Switch to e2
    SpiderM.liftIO <| switchTrigger e2

    -- Fire e1 (should be ignored now)
    SpiderM.liftIO <| t1 999

    -- Fire e2 (should be received)
    SpiderM.liftIO <| t2 3
    SpiderM.liftIO <| t2 4

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2, 3, 4]

test "switchDynamic propagates inner dynamic changes" := do
  let result ← runSpider do
    -- Create two inner dynamics
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let inner1 ← holdDyn 10 e1
    let inner2 ← holdDyn 20 e2

    -- Create outer dynamic that starts with inner1
    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Dynamic Spider Nat)
    let outer ← holdDyn inner1 switchEvent

    -- Create switched dynamic
    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynamicWithId outer nodeId

    -- Check initial value
    let v0 ← SpiderM.liftIO <| switched.sample

    -- Update inner1
    SpiderM.liftIO <| t1 15
    let v1 ← SpiderM.liftIO <| switched.sample

    -- Switch to inner2
    SpiderM.liftIO <| switchTrigger inner2
    let v2 ← SpiderM.liftIO <| switched.sample

    -- Update inner2
    SpiderM.liftIO <| t2 25
    let v3 ← SpiderM.liftIO <| switched.sample

    -- Update inner1 (should be ignored now)
    SpiderM.liftIO <| t1 999
    let v4 ← SpiderM.liftIO <| switched.sample

    pure (v0, v1, v2, v3, v4)

  shouldBe result (10, 15, 20, 25, 25)

test "switchDynamic fires update events" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let inner1 ← holdDyn 10 e1
    let inner2 ← holdDyn 20 e2

    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Dynamic Spider Nat)
    let outer ← holdDyn inner1 switchEvent

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynamicWithId outer nodeId

    -- Track update events
    let updatesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| switched.updated.subscribe fun n =>
      updatesRef.modify (· ++ [n])

    -- Trigger changes
    SpiderM.liftIO <| t1 15           -- inner1 change
    SpiderM.liftIO <| switchTrigger inner2  -- switch to inner2
    SpiderM.liftIO <| t2 25           -- inner2 change

    SpiderM.liftIO updatesRef.get

  shouldBe result [15, 20, 25]

test "switchHold switches on event occurrence" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Event Spider Nat)

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchHoldWithId e1 switchEvent nodeId

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| switched.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| switchTrigger e2
    SpiderM.liftIO <| t1 999  -- ignored
    SpiderM.liftIO <| t2 2

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2]

test "switchBehavior samples inner behavior" := do
  let result ← runSpider do
    let b1 : Behavior Spider Nat := Behavior.constant 10
    let b2 : Behavior Spider Nat := Behavior.constant 20

    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Behavior Spider Nat)
    let outer ← holdDyn b1 switchEvent

    let switched := switchBehavior outer.current

    let v0 ← sample switched
    SpiderM.liftIO <| switchTrigger b2
    let v1 ← sample switched

    pure (v0, v1)

  shouldBe result (10, 20)

test "Dynamic.switchM propagates inner dynamic changes with scope" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let inner1 ← holdDyn 10 e1
    let inner2 ← holdDyn 20 e2

    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Dynamic Spider Nat)
    let outer ← holdDyn inner1 switchEvent

    -- Use new SpiderM wrapper
    let switched ← Dynamic.switchM outer

    let v0 ← SpiderM.liftIO <| switched.sample
    SpiderM.liftIO <| t1 15
    let v1 ← SpiderM.liftIO <| switched.sample
    SpiderM.liftIO <| switchTrigger inner2
    let v2 ← SpiderM.liftIO <| switched.sample
    SpiderM.liftIO <| t2 25
    let v3 ← SpiderM.liftIO <| switched.sample

    pure (v0, v1, v2, v3)

  shouldBe result (10, 15, 20, 25)

test "Event.switchDynM switches events with scope" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)

    let (switchEvent, switchTrigger) ← newTriggerEvent (t := Spider) (a := Event Spider Nat)
    let dynEvent ← holdDyn e1 switchEvent

    -- Use new SpiderM wrapper
    let switched ← Event.switchDynM dynEvent

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← Event.subscribeM switched fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| t1 1
    SpiderM.liftIO <| t1 2
    SpiderM.liftIO <| switchTrigger e2
    SpiderM.liftIO <| t1 999  -- ignored
    SpiderM.liftIO <| t2 3

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2, 3]


end ReactiveTests.SwitchTests
