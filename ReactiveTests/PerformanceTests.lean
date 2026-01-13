/-
  Performance / Stress Tests for Reactive FRP Library

  Tests large and deep FRP networks to ensure the implementation
  handles extreme scale without crashing or hanging.
-/

import Crucible
import Reactive
import Chronos

namespace ReactiveTests.PerformanceTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Performance Tests"

/-! ## Helper Functions -/

/-- Build a chain of N mapped events from a source.
    Each map adds 1 to the value, so final value = initial + depth. -/
def buildDeepChain (source : Event Spider Nat) (depth : Nat) : SpiderM (Event Spider Nat) := do
  let mut current := source
  for _ in [:depth] do
    current ← Event.mapM (· + 1) current
  pure current

/-- Add N subscribers to an event, each incrementing a counter on fire. -/
def addCountingSubscribers (event : Event Spider Nat) (n : Nat)
    (countRef : IO.Ref Nat) : SpiderM Unit := do
  for _ in [:n] do
    let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
      countRef.modify (· + 1)

/-- Fire a trigger N times with consecutive values 0..N-1. -/
def fireNTimes (trigger : Nat → IO Unit) (n : Nat) : IO Unit := do
  for i in [:n] do
    trigger i

/-! ## Wide Fan-Out Tests (Many Subscribers) -/

test "perf: wide fan-out with 1000 subscribers, 100 fires" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Add 1000 subscribers
    addCountingSubscribers event 1000 countRef

    -- Time 100 fires
    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 100
    let elapsed ← SpiderM.liftIO start.elapsed

    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- Verify correctness: 1000 subscribers * 100 fires = 100,000
  shouldBe result.1 100000
  IO.println s!"  [1000 subscribers x 100 fires: {result.2}]"

test "perf: wide fan-out with 5000 subscribers, 10 fires" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Add 5000 subscribers
    addCountingSubscribers event 5000 countRef

    -- Time 10 fires
    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 10
    let elapsed ← SpiderM.liftIO start.elapsed

    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- Verify correctness: 5000 subscribers * 10 fires = 50,000
  shouldBe result.1 50000
  IO.println s!"  [5000 subscribers x 10 fires: {result.2}]"

/-! ## Deep Chain Tests (Long Dependency Chains) -/

test "perf: deep chain with 1000 map operations, 100 fires" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a chain of 1000 mapped events
    let deepEvent ← buildDeepChain source 1000

    let receivedRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| deepEvent.subscribe fun n => do
      receivedRef.set n
      countRef.modify (· + 1)

    -- Time 100 fires through the deep chain
    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 100
    let elapsed ← SpiderM.liftIO start.elapsed

    let finalValue ← SpiderM.liftIO receivedRef.get
    let fireCount ← SpiderM.liftIO countRef.get
    pure (finalValue, fireCount, elapsed)

  -- Final value should be 99 (last fire) + 1000 (chain depth) = 1099
  shouldBe result.1 1099
  shouldBe result.2.1 100
  IO.println s!"  [1000-deep chain x 100 fires: {result.2.2}]"

test "perf: deep chain with 2000 map operations, 50 fires" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a chain of 2000 mapped events
    let deepEvent ← buildDeepChain source 2000

    let receivedRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| deepEvent.subscribe fun n => do
      receivedRef.set n
      countRef.modify (· + 1)

    -- Time 50 fires
    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 50
    let elapsed ← SpiderM.liftIO start.elapsed

    let finalValue ← SpiderM.liftIO receivedRef.get
    let fireCount ← SpiderM.liftIO countRef.get
    pure (finalValue, fireCount, elapsed)

  -- Final value should be 49 + 2000 = 2049
  shouldBe result.1 2049
  shouldBe result.2.1 50
  IO.println s!"  [2000-deep chain x 50 fires: {result.2.2}]"

/-! ## High-Frequency Firing Tests -/

test "perf: rapid firing 10000 events" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
      countRef.modify (· + 1)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 10000
    let elapsed ← SpiderM.liftIO start.elapsed

    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  shouldBe result.1 10000
  IO.println s!"  [10000 rapid fires: {result.2}]"

/-! ## Diamond Pattern Tests (Fan-out + Fan-in) -/

test "perf: 100-branch diamond pattern, 100 fires" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Fan out to 100 branches
    let mut branches : Array (Event Spider Nat) := #[]
    for i in [:100] do
      let branch ← Event.mapM (· + i) source
      branches := branches.push branch

    -- Fan in with mergeList to batch simultaneous values
    let merged ← Event.mergeListM branches.toList

    let batchCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let totalValuesRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| merged.subscribe fun batch => do
      batchCountRef.modify (· + 1)
      totalValuesRef.modify (· + batch.length)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 100
    let elapsed ← SpiderM.liftIO start.elapsed

    let batches ← SpiderM.liftIO batchCountRef.get
    let totalValues ← SpiderM.liftIO totalValuesRef.get
    pure (batches, totalValues, elapsed)

  -- 100 fires, each producing 1 batch of 100 values
  shouldBe result.1 100
  shouldBe result.2.1 10000
  IO.println s!"  [100-branch diamond x 100 fires: {result.2.2}]"

test "perf: 200-branch diamond pattern, 50 fires" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Fan out to 200 branches
    let mut branches : Array (Event Spider Nat) := #[]
    for i in [:200] do
      let branch ← Event.mapM (· + i) source
      branches := branches.push branch

    -- Fan in with mergeList
    let merged ← Event.mergeListM branches.toList

    let totalValuesRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| merged.subscribe fun batch =>
      totalValuesRef.modify (· + batch.length)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 50
    let elapsed ← SpiderM.liftIO start.elapsed

    let totalValues ← SpiderM.liftIO totalValuesRef.get
    pure (totalValues, elapsed)

  -- 50 fires * 200 branches = 10000 total values
  shouldBe result.1 10000
  IO.println s!"  [200-branch diamond x 50 fires: {result.2}]"

/-! ## Mixed Wide + Deep Tests -/

test "perf: mixed wide and deep network (50 chains x 100 depth x 20 subscribers)" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create 50 deep chains, each 100 levels deep
    let mut endpoints : Array (Event Spider Nat) := #[]
    for i in [:50] do
      let chain ← buildDeepChain source 100
      -- Add a unique offset per chain so we can verify independence
      let tagged ← Event.mapM (· + i * 1000) chain
      endpoints := endpoints.push tagged

    -- Add 20 subscribers to each of the 50 endpoints
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    for endpoint in endpoints do
      for _ in [:20] do
        let _ ← SpiderM.liftIO <| endpoint.subscribe fun _ =>
          countRef.modify (· + 1)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    SpiderM.liftIO <| fireNTimes trigger 100
    let elapsed ← SpiderM.liftIO start.elapsed

    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- 50 endpoints * 20 subscribers * 100 fires = 100,000
  shouldBe result.1 100000
  IO.println s!"  [50 chains x 100 depth x 20 subs x 100 fires: {result.2}]"

/-! ## Switch Combinator Under Load -/

test "perf: switch combinator with 500 switch cycles" := do
  let result ← runSpider do
    -- Create the first event separately to use as initial value
    let (firstEvent, firstFire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create a pool of 10 source events stored as arrays (including first)
    let mut events : Array (Event Spider Nat) := #[firstEvent]
    let mut fires : Array (Nat → IO Unit) := #[firstFire]
    for _ in [:9] do
      let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
      events := events.push event
      fires := fires.push fire

    -- Create the switching infrastructure
    let (switchEvent, switchTrigger) ←
      newTriggerEvent (t := Spider) (a := Event Spider Nat)

    let dynEvent ← holdDyn firstEvent switchEvent

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynWithId dynEvent nodeId

    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← Event.subscribeM switched fun _ =>
      countRef.modify (· + 1)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- 500 cycles of: switch to new event, then fire that event
    for i in [:500] do
      let eventIdx := i % 10
      match events[eventIdx]?, fires[eventIdx]? with
      | some event, some fire => do
        SpiderM.liftIO <| switchTrigger event
        SpiderM.liftIO <| fire i
      | _, _ => pure ()

    let elapsed ← SpiderM.liftIO start.elapsed
    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- Each switch + fire cycle should result in 1 notification
  shouldBe result.1 500
  IO.println s!"  [500 switch + fire cycles: {result.2}]"

/-! ## Large Propagation Queue Tests -/

test "perf: large propagation queue (100 simultaneous sources)" := do
  let result ← runSpider do
    -- Create 100 independent trigger events
    let mut triggers : Array (Nat → IO Unit) := #[]
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    for _ in [:100] do
      let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
      let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
        countRef.modify (· + 1)
      triggers := triggers.push trigger

    -- Create a master event that triggers all 100 sources
    let (master, fireMaster) ← newTriggerEvent (t := Spider) (a := Unit)
    let triggersCapture := triggers
    let _ ← SpiderM.liftIO <| master.subscribe fun _ => do
      for i in [:triggersCapture.size] do
        match triggersCapture[i]? with
        | some t => t i
        | none => pure ()

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now
    -- Fire master 100 times, each cascading to 100 sub-fires
    for _ in [:100] do
      SpiderM.liftIO <| fireMaster ()
    let elapsed ← SpiderM.liftIO start.elapsed

    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- 100 master fires * 100 sub-triggers = 10,000 subscriber notifications
  shouldBe result.1 10000
  IO.println s!"  [100 sources x 100 simultaneous fires: {result.2}]"

/-! ## Nested Dynamic Tests (Dynamic of Dynamic) -/

test "perf: switchDynamic with 100 inner dynamics, 500 switches" := do
  let result ← runSpider do
    -- Create 100 inner dynamics, each with its own trigger
    let mut innerDyns : Array (Dynamic Spider Nat) := #[]
    let mut innerFires : Array (Nat → IO Unit) := #[]
    for i in [:100] do
      let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
      let dyn ← holdDyn (i * 1000) event  -- Initial values 0, 1000, 2000, ...
      innerDyns := innerDyns.push dyn
      innerFires := innerFires.push fire

    -- Create outer dynamic that switches between inner dynamics
    let (switchEvent, switchTrigger) ←
      newTriggerEvent (t := Spider) (a := Dynamic Spider Nat)

    let firstInner := innerDyns[0]?.getD (← Dynamic.pureM 0)
    let outer ← holdDyn firstInner switchEvent

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynamicWithId outer nodeId

    -- Track update count
    let updateCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| switched.updated.subscribe fun _ =>
      updateCountRef.modify (· + 1)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- 500 cycles: switch to different inner dynamic, then update that inner
    for i in [:500] do
      let idx := i % 100
      match innerDyns[idx]?, innerFires[idx]? with
      | some dyn, some fire => do
        SpiderM.liftIO <| switchTrigger dyn
        SpiderM.liftIO <| fire (i * 10)
      | _, _ => pure ()

    let elapsed ← SpiderM.liftIO start.elapsed
    let updateCount ← SpiderM.liftIO updateCountRef.get
    pure (updateCount, elapsed)

  -- Each switch fires an update (new inner value), each inner fire also fires
  -- 500 switches + 500 inner updates = 1000 updates
  shouldBe result.1 1000
  IO.println s!"  [100 inner dynamics, 500 switch+update cycles: {result.2}]"

test "perf: nested dynamics with rapid inner updates (1000 updates)" := do
  let result ← runSpider do
    -- Create an inner dynamic
    let (innerEvent, innerTrigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let inner ← holdDyn 0 innerEvent

    -- Wrap in outer dynamic (doesn't switch, just propagates inner changes)
    let outer ← Dynamic.pureM inner

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynamicWithId outer nodeId

    let updateCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let lastValueRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| switched.updated.subscribe fun n => do
      updateCountRef.modify (· + 1)
      lastValueRef.set n

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- Rapidly update the inner dynamic 1000 times
    for i in [:1000] do
      SpiderM.liftIO <| innerTrigger i

    let elapsed ← SpiderM.liftIO start.elapsed
    let updateCount ← SpiderM.liftIO updateCountRef.get
    let lastValue ← SpiderM.liftIO lastValueRef.get
    pure (updateCount, lastValue, elapsed)

  shouldBe result.1 1000
  shouldBe result.2.1 999  -- Last value is 999 (0-indexed)
  IO.println s!"  [nested dynamic, 1000 rapid inner updates: {result.2.2}]"

test "perf: deeply nested dynamics (10 levels)" := do
  let result ← runSpider do
    -- Create the innermost dynamic
    let (coreEvent, coreTrigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let core ← holdDyn 0 coreEvent

    -- Wrap in 10 levels of switchDynamic
    let mut current : Dynamic Spider Nat := core
    for _ in [:10] do
      let wrapped ← Dynamic.pureM current
      let nodeId ← SpiderM.freshNodeId
      current ← SpiderM.liftIO <| switchDynamicWithId wrapped nodeId

    let updateCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| current.updated.subscribe fun _ =>
      updateCountRef.modify (· + 1)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- Fire 100 updates through all 10 levels
    for i in [:100] do
      SpiderM.liftIO <| coreTrigger i

    let elapsed ← SpiderM.liftIO start.elapsed
    let updateCount ← SpiderM.liftIO updateCountRef.get
    let finalValue ← SpiderM.liftIO current.sample
    pure (updateCount, finalValue, elapsed)

  shouldBe result.1 100
  shouldBe result.2.1 99  -- Final value is 99
  IO.println s!"  [10-level nested dynamics, 100 updates: {result.2.2}]"

test "perf: switchDynamic with frequent outer and inner changes" := do
  let result ← runSpider do
    -- Create 20 inner dynamics
    let mut innerDyns : Array (Dynamic Spider Nat) := #[]
    let mut innerFires : Array (Nat → IO Unit) := #[]
    for i in [:20] do
      let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)
      let dyn ← holdDyn (i * 100) event
      innerDyns := innerDyns.push dyn
      innerFires := innerFires.push fire

    -- Outer switching dynamic
    let (switchEvent, switchTrigger) ←
      newTriggerEvent (t := Spider) (a := Dynamic Spider Nat)

    let firstInner := innerDyns[0]?.getD (← Dynamic.pureM 0)
    let outer ← holdDyn firstInner switchEvent

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynamicWithId outer nodeId

    let updateCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← SpiderM.liftIO <| switched.updated.subscribe fun _ =>
      updateCountRef.modify (· + 1)

    -- Track which inner is currently selected
    let currentIdxRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- 200 iterations: switch then update the CURRENT inner
    for i in [:200] do
      let switchIdx := i % 20
      -- Switch every other iteration
      if i % 2 == 0 then
        match innerDyns[switchIdx]? with
        | some dyn => do
          SpiderM.liftIO <| switchTrigger dyn
          SpiderM.liftIO <| currentIdxRef.set switchIdx
        | none => pure ()
      -- Update the currently selected inner (so it propagates)
      let currentIdx ← SpiderM.liftIO currentIdxRef.get
      match innerFires[currentIdx]? with
      | some fire => SpiderM.liftIO <| fire i
      | none => pure ()

    let elapsed ← SpiderM.liftIO start.elapsed
    let updateCount ← SpiderM.liftIO updateCountRef.get
    pure (updateCount, elapsed)

  -- 100 switches (each fires with new value) + 200 inner updates = 300 updates
  shouldBe result.1 300
  IO.println s!"  [20 inner dynamics, 200 mixed switch/update ops: {result.2}]"

/-! ## Subscribe/Unsubscribe Churn Tests -/

test "perf: rapid subscribe/unsubscribe cycles (1000 cycles)" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- 1000 cycles of: subscribe, fire, unsubscribe
    for i in [:1000] do
      let unsub ← SpiderM.liftIO <| event.subscribe fun _ =>
        countRef.modify (· + 1)
      SpiderM.liftIO <| trigger i
      SpiderM.liftIO unsub

    let elapsed ← SpiderM.liftIO start.elapsed
    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- Each cycle: subscribe, fire (count++), unsubscribe = 1000 fires
  shouldBe result.1 1000
  IO.println s!"  [1000 subscribe/fire/unsubscribe cycles: {result.2}]"

test "perf: many subscribers with interleaved unsubscribe (500 subs, 250 unsubs)" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Subscribe 500, keeping unsubscribe actions
    let mut unsubs : Array (IO Unit) := #[]
    for _ in [:500] do
      let unsub ← SpiderM.liftIO <| event.subscribe fun _ =>
        countRef.modify (· + 1)
      unsubs := unsubs.push unsub

    let start ← SpiderM.liftIO Chronos.MonotonicTime.now

    -- Unsubscribe every other one (250 unsubscribes)
    for i in [:250] do
      match unsubs[i * 2]? with
      | some unsub => SpiderM.liftIO unsub
      | none => pure ()

    -- Fire 100 times with remaining 250 subscribers
    SpiderM.liftIO <| fireNTimes trigger 100

    let elapsed ← SpiderM.liftIO start.elapsed
    let count ← SpiderM.liftIO countRef.get
    pure (count, elapsed)

  -- 250 remaining subscribers * 100 fires = 25000
  shouldBe result.1 25000
  IO.println s!"  [500 subs, 250 unsubs, 100 fires: {result.2}]"

#generate_tests

end ReactiveTests.PerformanceTests
