import Crucible
import Reactive
import Chronos

namespace ReactiveTests.TemporalTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Temporal Tests"

/-! ## Frame-Based Delay Tests -/

test "delayFrame delays to next frame" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayFrameM trigger

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← trigger.subscribe fun n =>
      orderRef.modify (· ++ [s!"immediate:{n}"])
    let _ ← delayed.subscribe fun n =>
      orderRef.modify (· ++ [s!"delayed:{n}"])

    fire 1
    SpiderM.liftIO orderRef.get

  -- Immediate fires in frame 1, delayed fires in frame 2
  shouldBe result ["immediate:1", "delayed:1"]

test "delayFrame preserves value order" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayFrameM trigger

    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← delayed.subscribe fun n =>
      valuesRef.modify (· ++ [n])

    fire 1
    fire 2
    fire 3
    SpiderM.liftIO valuesRef.get

  shouldBe result [1, 2, 3]

test "delayFrame breaks immediate feedback" := do
  -- This tests that delayFrame prevents immediate recursion
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayFrameM trigger

    let countRef ← SpiderM.liftIO <| IO.mkRef 0
    let _ ← delayed.subscribe fun n => do
      let count ← countRef.get
      if count < 3 then
        countRef.set (count + 1)
        -- This would cause infinite loop without delay
        fire (n + 1)

    fire 1
    SpiderM.liftIO countRef.get

  shouldBe result 3

/-! ## Time-Based Delay Tests -/

test "delayDuration fires after duration" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayDurationM (Chronos.Duration.fromMilliseconds 30) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← delayed.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 42
    -- Wait for delay to complete
    SpiderM.liftIO <| IO.sleep 100
    SpiderM.liftIO receivedRef.get

  shouldBe result [42]

test "delayDuration delays independently" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let delayed ← Event.delayDurationM (Chronos.Duration.fromMilliseconds 30) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← delayed.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1
    SpiderM.liftIO <| IO.sleep 5  -- Small delay to ensure first task is scheduled
    fire 2
    SpiderM.liftIO <| IO.sleep 100
    SpiderM.liftIO receivedRef.get

  -- Both should arrive (order may vary due to async)
  let vals := result.toArray.qsort (· < ·) |>.toList
  shouldBe vals [1, 2]

/-! ## Debounce Tests -/

test "debounce only fires after quiet period" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let debounced ← Event.debounceM (Chronos.Duration.fromMilliseconds 50) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← debounced.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Rapid fire sequence
    fire 1
    SpiderM.liftIO <| IO.sleep 20
    fire 2
    SpiderM.liftIO <| IO.sleep 20
    fire 3
    -- Wait for quiet period + buffer
    SpiderM.liftIO <| IO.sleep 100

    SpiderM.liftIO receivedRef.get

  -- Only the last value should fire after quiet period
  shouldBe result [3]

test "debounce fires for separated bursts" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let debounced ← Event.debounceM (Chronos.Duration.fromMilliseconds 30) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← debounced.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- First burst
    fire 1
    fire 2
    SpiderM.liftIO <| IO.sleep 80  -- Wait for first debounce

    -- Second burst
    fire 10
    fire 20
    SpiderM.liftIO <| IO.sleep 80  -- Wait for second debounce

    SpiderM.liftIO receivedRef.get

  -- Should have two debounced fires (last from each burst)
  shouldBe result [2, 20]

/-! ## Throttle Tests -/

test "throttle limits fire rate" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let throttled ← Event.throttleM (Chronos.Duration.fromMilliseconds 100) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← throttled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1  -- Leading fire (immediate)
    fire 2  -- In cooldown, stored for trailing
    fire 3  -- In cooldown, replaces pending trailing
    SpiderM.liftIO <| IO.sleep 150  -- Wait for trailing

    SpiderM.liftIO receivedRef.get

  -- Leading fire + trailing fire with last value
  shouldBe result [1, 3]

test "throttle leading only" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let throttled ← Event.throttleM (Chronos.Duration.fromMilliseconds 100) trigger
        (leading := true) (trailing := false)

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← throttled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1  -- Leading fire
    fire 2  -- Ignored
    fire 3  -- Ignored
    SpiderM.liftIO <| IO.sleep 150

    SpiderM.liftIO receivedRef.get

  -- Only leading fire
  shouldBe result [1]

test "throttle trailing only" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let throttled ← Event.throttleM (Chronos.Duration.fromMilliseconds 100) trigger
        (leading := false) (trailing := true)

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← throttled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1  -- No leading, stored for trailing
    fire 2  -- Replaces pending
    fire 3  -- Replaces pending
    SpiderM.liftIO <| IO.sleep 150

    SpiderM.liftIO receivedRef.get

  -- Only trailing fire with last value
  shouldBe result [3]

test "throttle fires after interval" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let throttled ← Event.throttleM (Chronos.Duration.fromMilliseconds 50) trigger

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← throttled.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    fire 1  -- Leading
    SpiderM.liftIO <| IO.sleep 100  -- Wait for interval
    fire 2  -- Should fire (interval elapsed)
    SpiderM.liftIO <| IO.sleep 100

    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2]


end ReactiveTests.TemporalTests
