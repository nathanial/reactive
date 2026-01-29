import Crucible
import Reactive

namespace ReactiveTests.TopologyTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Topology Tests"

-- Fan-out tests

test "fan-out: one source to multiple derived events" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create multiple derived events from the same source
    let doubled ← Event.mapM (· * 2) source
    let tripled ← Event.mapM (· * 3) source
    let squared ← Event.mapM (fun x => x * x) source

    let doubledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let tripledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let squaredRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← doubled.subscribe fun n => doubledRef.modify (· ++ [n])
    let _ ← tripled.subscribe fun n => tripledRef.modify (· ++ [n])
    let _ ← squared.subscribe fun n => squaredRef.modify (· ++ [n])

    trigger 5
    let d ← SpiderM.liftIO doubledRef.get
    let t ← SpiderM.liftIO tripledRef.get
    let s ← SpiderM.liftIO squaredRef.get
    pure (d, t, s)
  shouldBe result ([10], [15], [25])

test "fan-out with filtering branches" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let evens ← Event.filterM (· % 2 == 0) source
    let odds ← Event.filterM (· % 2 == 1) source
    let divisibleBy3 ← Event.filterM (· % 3 == 0) source

    let evensRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let oddsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let div3Ref ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← evens.subscribe fun n => evensRef.modify (· ++ [n])
    let _ ← odds.subscribe fun n => oddsRef.modify (· ++ [n])
    let _ ← divisibleBy3.subscribe fun n => div3Ref.modify (· ++ [n])

    for i in [1:10] do
      trigger i

    let e ← SpiderM.liftIO evensRef.get
    let o ← SpiderM.liftIO oddsRef.get
    let d ← SpiderM.liftIO div3Ref.get
    pure (e, o, d)
  shouldBe result ([2, 4, 6, 8], [1, 3, 5, 7, 9], [3, 6, 9])

-- Fan-in tests

test "fan-in: multiple sources merged into one" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e4, t4) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e5, t5) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.leftmostM [e1, e2, e3, e4, e5]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t3 30
    t1 10
    t5 50
    t2 20
    t4 40
    SpiderM.liftIO receivedRef.get
  shouldBe result [30, 10, 50, 20, 40]

test "fan-in with accumulation" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.leftmostM [e1, e2, e3]
    let accumulated ← Event.accumulateM (· + ·) 0 merged

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    t1 5
    t2 10
    t3 3
    SpiderM.liftIO receivedRef.get
  shouldBe result [5, 15, 18]

-- Diamond pattern tests

test "diamond pattern: split and rejoin with first-only" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Split
    let branch1 ← Event.mapM (· + 10) source
    let branch2 ← Event.mapM (· + 20) source

    -- Rejoin (leftmostM uses first-only semantics)
    let joined ← Event.leftmostM [branch1, branch2]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← joined.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 5
    SpiderM.liftIO receivedRef.get
  -- Both branches fire simultaneously, but leftmostM takes only the first
  shouldBe result [15]

test "diamond pattern: split and rejoin with mergeAllListM" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Split
    let branch1 ← Event.mapM (· + 10) source
    let branch2 ← Event.mapM (· + 20) source

    -- Rejoin (mergeAllListM fires all)
    let joined ← Event.mergeAllListM [branch1, branch2]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← joined.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 5
    SpiderM.liftIO receivedRef.get
  -- Both branches fire, mergeAllListM delivers all values
  shouldBe result [15, 25]

test "diamond pattern with mergeList batching" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Split into two branches
    let left ← Event.mapM (· * 2) source
    let right ← Event.mapM (· * 3) source

    -- Rejoin with mergeList to get batched values
    let merged ← Event.mergeListM [left, right]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])

    trigger 10
    SpiderM.liftIO receivedRef.get
  -- Should batch [20, 30] from both branches
  shouldBe result [[20, 30]]

test "mergeList separates delayed branch into next frame" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let immediate ← Event.mapM (· + 1) source
    let delayed ← Event.delayFrameM source
    let delayedMapped ← Event.mapM (· + 100) delayed

    let merged ← Event.mergeListM [immediate, delayedMapped]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])

    trigger 1
    SpiderM.liftIO receivedRef.get
  shouldBe result [[2], [101]]

test "multiple diamond patterns" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- First diamond
    let d1_left ← Event.mapM (· + 1) source
    let d1_right ← Event.mapM (· + 2) source
    let d1_merged ← Event.mergeListM [d1_left, d1_right]

    -- Second diamond from first merge
    let d2_left ← Event.mapM (fun ns => ns.map (· * 10)) d1_merged
    let d2_right ← Event.mapM (fun ns => ns.map (· * 100)) d1_merged
    let d2_merged ← Event.mergeListM [d2_left, d2_right]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List (List Nat)))
    let _ ← d2_merged.subscribe fun nss =>
      receivedRef.modify (· ++ [nss])

    trigger 5
    SpiderM.liftIO receivedRef.get
  -- First diamond: [6, 7], second diamond transforms and merges
  shouldBe result [[[60, 70], [600, 700]]]

-- Complex topology tests

test "tree topology: one source, multiple levels of derived events" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Level 1
    let l1a ← Event.mapM (· * 2) source
    let l1b ← Event.mapM (· * 3) source

    -- Level 2 (derived from level 1)
    let l2a ← Event.mapM (· + 1) l1a
    let l2b ← Event.mapM (· + 2) l1a
    let l2c ← Event.mapM (· + 3) l1b
    let l2d ← Event.mapM (· + 4) l1b

    -- Collect all level 2 events
    let merged ← Event.mergeListM [l2a, l2b, l2c, l2d]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun ns =>
      receivedRef.modify (· ++ [ns])

    trigger 10
    SpiderM.liftIO receivedRef.get
  -- source=10 -> l1a=20, l1b=30
  -- l1a=20 -> l2a=21, l2b=22
  -- l1b=30 -> l2c=33, l2d=34
  shouldBe result [[21, 22, 33, 34]]

test "cyclic-like pattern with gate" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create a counter that we'll use as a gate
    let counter ← foldDyn (fun _ n => n + 1) 0 source

    -- Gate the source by counter < 3
    let gateBehavior := counter.current.map (fun n => decide (n < 3))
    let gated ← Event.gateM gateBehavior source

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire 5 times
    for i in [1:6] do
      trigger i

    SpiderM.liftIO receivedRef.get
  -- Counter updates before gate checks (same frame, height ordering)
  -- Fire 1: counter becomes 1, gate checks 1<3=true, passes
  -- Fire 2: counter becomes 2, gate checks 2<3=true, passes
  -- Fire 3: counter becomes 3, gate checks 3<3=false, blocked
  shouldBe result [1, 2]

test "parallel processing pipelines" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Pipeline 1: filter evens, then double
    let p1_filtered ← Event.filterM (· % 2 == 0) source
    let p1_result ← Event.mapM (· * 2) p1_filtered

    -- Pipeline 2: filter odds, then triple
    let p2_filtered ← Event.filterM (· % 2 == 1) source
    let p2_result ← Event.mapM (· * 3) p2_filtered

    -- Combine results
    let combined ← Event.leftmostM [p1_result, p2_result]
    let accumulated ← Event.accumulateM (· + ·) 0 combined

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← accumulated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire 1-5
    for i in [1:6] do
      trigger i

    SpiderM.liftIO receivedRef.get
  -- 1 (odd) -> 3, 2 (even) -> 4, 3 (odd) -> 9, 4 (even) -> 8, 5 (odd) -> 15
  -- accumulated: [3, 7, 16, 24, 39]
  shouldBe result [3, 7, 16, 24, 39]

-- Combinator interaction tests

test "switchDyn with filtered events" := do
  let result ← runSpider do
    let (selector, selectTrigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let (values, valueTrigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create different filtered views
    let evens ← Event.filterM (· % 2 == 0) values
    let odds ← Event.filterM (· % 2 == 1) values
    let all := values

    -- Map selector to pick which event source to use
    let selectorMapped ← Event.mapM (fun n =>
      if n == 0 then all else if n == 1 then evens else odds) selector

    -- Dynamic that switches between event sources
    let eventSelector ← holdDyn all selectorMapped

    let nodeId ← SpiderM.freshNodeId
    let switched ← SpiderM.liftIO <| switchDynWithId eventSelector nodeId

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← switched.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Start with all
    valueTrigger 1
    valueTrigger 2

    -- Switch to evens only
    selectTrigger 1
    valueTrigger 3  -- odd, filtered
    valueTrigger 4  -- even, passes

    -- Switch to odds only
    selectTrigger 2
    valueTrigger 5  -- odd, passes
    valueTrigger 6  -- even, filtered

    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 4, 5]

test "gate with accumulated threshold" := do
  let result ← runSpider do
    let (values, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Accumulate a running sum
    let sumDyn ← foldDyn (· + ·) 0 values

    -- Gate values when sum < 20
    let gateBehavior := sumDyn.current.map (fun n => decide (n < 20))
    let gated ← Event.gateM gateBehavior values

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← gated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire values that will accumulate
    trigger 5   -- sum becomes 5, gate checks 5<20=true, passes
    trigger 10  -- sum becomes 15, gate checks 15<20=true, passes
    trigger 7   -- sum becomes 22, gate checks 22<20=false, blocked
    trigger 3   -- sum becomes 25, gate checks 25<20=false, blocked

    SpiderM.liftIO receivedRef.get
  -- Gate samples behavior after foldDyn updates (same frame, height ordering)
  shouldBe result [5, 10]

test "mapMaybe with attach" := do
  let result ← runSpider do
    let (events, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let multiplierRef ← SpiderM.liftIO <| IO.mkRef (2 : Nat)
    let multiplierB : Behavior Spider Nat := Behavior.fromSample multiplierRef.get

    -- Attach multiplier to events
    let attached ← Event.attachM multiplierB events

    -- Filter out results where product > 50
    let filtered ← Event.mapMaybeM (fun (m, v) =>
      let product := m * v
      if product <= 50 then some product else none) attached

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 10  -- 2*10=20 <= 50, passes
    trigger 20  -- 2*20=40 <= 50, passes
    SpiderM.liftIO <| multiplierRef.set 5
    trigger 10  -- 5*10=50 <= 50, passes
    trigger 15  -- 5*15=75 > 50, filtered

    SpiderM.liftIO receivedRef.get
  shouldBe result [20, 40, 50]

test "dynamic zipWith with multiple dynamics" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)

    let d1 ← holdDyn 1 e1
    let d2 ← holdDyn 2 e2
    let d3 ← holdDyn 3 e3

    -- Combine all three dynamics
    let combined ← Dynamic.zipWith3M (fun a b c => a + b + c) d1 d2 d3

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← combined.updated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Initial: 1+2+3=6

    t1 10  -- 10+2+3=15
    t2 20  -- 10+20+3=33
    t3 30  -- 10+20+30=60

    SpiderM.liftIO receivedRef.get
  shouldBe result [15, 33, 60]


end ReactiveTests.TopologyTests
