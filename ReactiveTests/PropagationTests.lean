import Crucible
import Reactive

namespace ReactiveTests.PropagationTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Propagation Tests"

test "height ordering is respected" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Unit)

    -- Chain of increasing height
    let e1 ← Event.mapM (fun _ => 1) trigger  -- height 1
    let e2 ← Event.mapM (fun _ => 2) e1       -- height 2
    let e3 ← Event.mapM (fun _ => 3) e2       -- height 3

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    -- Subscribe to all in reverse order of height
    let _ ← e3.subscribe fun n => orderRef.modify (· ++ [n])
    let _ ← e1.subscribe fun n => orderRef.modify (· ++ [n])
    let _ ← e2.subscribe fun n => orderRef.modify (· ++ [n])

    fire ()
    SpiderM.liftIO orderRef.get

  -- Should fire in height order: e1 (h1), e2 (h2), e3 (h3)
  -- regardless of subscription order
  shouldBe result [1, 2, 3]

test "diamond dependency fires with left-bias" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create diamond: trigger → e1, e2 → merged
    let e1 ← Event.mapM (· + 1) trigger   -- height 1
    let e2 ← Event.mapM (· + 10) trigger  -- height 1
    let merged ← Event.mergeM e1 e2       -- height 2 (left-bias)

    -- Track all values seen by merged
    let seenRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      seenRef.modify (· ++ [n])

    -- Fire trigger with value 5
    fire 5

    SpiderM.liftIO seenRef.get

  -- Both e1 and e2 fire simultaneously, but mergeM uses left-bias
  -- Only e1's value (6) is delivered; e2's value (15) is suppressed
  shouldBe result [6]

test "diamond dependency with mergeAllM fires both" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create diamond: trigger → e1, e2 → merged
    let e1 ← Event.mapM (· + 1) trigger   -- height 1
    let e2 ← Event.mapM (· + 10) trigger  -- height 1
    let merged ← Event.mergeAllM e1 e2    -- height 2 (all-fire)

    -- Track all values seen by merged
    let seenRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      seenRef.modify (· ++ [n])

    -- Fire trigger with value 5
    fire 5

    SpiderM.liftIO seenRef.get

  -- Both e1 and e2 fire simultaneously, mergeAllM delivers both
  -- Within same height, ordered by nodeId (e1 created before e2)
  shouldBe result [6, 15]

test "multiple triggers in sequence create separate frames" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) trigger

    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← mapped.subscribe fun n =>
      valuesRef.modify (· ++ [n])

    -- Fire twice - each should be a complete frame
    fire 1
    fire 2
    fire 3

    SpiderM.liftIO valuesRef.get

  shouldBe result [2, 4, 6]

test "nested triggers within a frame are processed in order" := do
  let result ← runSpider do
    let (outerTrigger, fireOuter) ← newTriggerEvent (t := Spider) (a := Nat)
    let (innerTrigger, fireInner) ← newTriggerEvent (t := Spider) (a := Nat)

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)

    -- When outer fires, also fire inner
    let _ ← outerTrigger.subscribe fun n => do
      orderRef.modify (· ++ [s!"outer:{n}"])
      fireInner (n * 10)

    let _ ← innerTrigger.subscribe fun n =>
      orderRef.modify (· ++ [s!"inner:{n}"])

    fireOuter 5
    SpiderM.liftIO orderRef.get

  -- Outer fires first (it started the frame), then inner
  shouldBe result ["outer:5", "inner:50"]

test "complex graph maintains height ordering" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a more complex graph:
    --           trigger (h0)
    --          /   |   \
    --        a     b     c  (all h1)
    --         \   / \   /
    --          ab     bc    (h2)
    --            \   /
    --             abc       (h3)

    let a ← Event.mapM (fun n => ("a", n)) trigger
    let b ← Event.mapM (fun n => ("b", n)) trigger
    let c ← Event.mapM (fun n => ("c", n)) trigger

    let a_ab ← Event.mapM (fun (s, _) => s!"ab-{s}") a
    let b_ab ← Event.mapM (fun (s, _) => s!"ab-{s}") b
    let ab ← Event.mergeAllM a_ab b_ab  -- use mergeAllM to see all values
    let b_bc ← Event.mapM (fun (s, _) => s!"bc-{s}") b
    let c_bc ← Event.mapM (fun (s, _) => s!"bc-{s}") c
    let bc ← Event.mergeAllM b_bc c_bc  -- use mergeAllM to see all values

    let abc ← Event.mergeAllM ab bc     -- use mergeAllM to see all values

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← abc.subscribe fun s =>
      orderRef.modify (· ++ [s])

    fire 1
    SpiderM.liftIO orderRef.get

  -- All height-1 events (a, b, c) fire first
  -- Then height-2 (ab, bc) - using mergeAllM, both paths propagate
  -- Then height-3 (abc) receives from ab and bc
  -- The exact order within same height depends on nodeId
  ensure (result.length == 4) s!"Expected 4 values, got {result.length}: {result}"

/-! ## mergeList Batching Tests -/

test "mergeList batches simultaneous events" := do
  let result ← runSpider do
    let (t1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (t2, fire2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (t3, fire3) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.mergeListM [t1, t2, t3]

    let batchesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun batch =>
      batchesRef.modify (· ++ [batch])

    -- Fire all three in the same frame (same trigger call triggers all)
    -- We need a single event that triggers all three
    let (combo, fireCombo) ← newTriggerEvent (t := Spider) (a := Unit)
    let _ ← combo.subscribe fun _ => do
      fire1 1
      fire2 2
      fire3 3

    fireCombo ()
    SpiderM.liftIO batchesRef.get

  -- Should receive one batch with all three values
  shouldBe result [[1, 2, 3]]

test "mergeList batches from diamond pattern" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create two derived events from same source
    let e1 ← Event.mapM (· * 2) trigger
    let e2 ← Event.mapM (· * 3) trigger

    let merged ← Event.mergeListM [e1, e2]

    let batchesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun batch =>
      batchesRef.modify (· ++ [batch])

    fire 5
    SpiderM.liftIO batchesRef.get

  -- Both e1 and e2 fire in same frame, should be batched
  -- Order depends on nodeId (e1 created first, so 10 before 15)
  shouldBe result [[10, 15]]

test "mergeList separate frames produce separate batches" := do
  let result ← runSpider do
    let (t1, fire1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (t2, fire2) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.mergeListM [t1, t2]

    let batchesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun batch =>
      batchesRef.modify (· ++ [batch])

    -- Fire in separate frames
    fire1 1
    fire2 2

    SpiderM.liftIO batchesRef.get

  -- Each fire is a separate frame, so separate batches
  shouldBe result [[1], [2]]

test "mergeList with single event fires single-element list" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.mergeListM [trigger]

    let batchesRef ← SpiderM.liftIO <| IO.mkRef ([] : List (List Nat))
    let _ ← merged.subscribe fun batch =>
      batchesRef.modify (· ++ [batch])

    fire 42
    SpiderM.liftIO batchesRef.get

  shouldBe result [[42]]


end ReactiveTests.PropagationTests
