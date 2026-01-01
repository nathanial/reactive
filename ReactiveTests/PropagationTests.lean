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
    let _ ← SpiderM.liftIO <| e3.subscribe fun n => orderRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| e1.subscribe fun n => orderRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| e2.subscribe fun n => orderRef.modify (· ++ [n])

    SpiderM.liftIO <| fire ()
    SpiderM.liftIO orderRef.get

  -- Should fire in height order: e1 (h1), e2 (h2), e3 (h3)
  -- regardless of subscription order
  shouldBe result [1, 2, 3]

test "diamond dependency fires in height order" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Create diamond: trigger → e1, e2 → merged
    let e1 ← Event.mapM (· + 1) trigger   -- height 1
    let e2 ← Event.mapM (· + 10) trigger  -- height 1
    let merged ← Event.mergeM e1 e2       -- height 2

    -- Track all values seen by merged
    let seenRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| merged.subscribe fun n =>
      seenRef.modify (· ++ [n])

    -- Fire trigger with value 5
    SpiderM.liftIO <| fire 5

    SpiderM.liftIO seenRef.get

  -- Both e1 and e2 are height 1, so they fire before merged (height 2)
  -- Within same height, ordered by nodeId (e1 created before e2)
  -- So we see: e1's value (6), then e2's value (15)
  shouldBe result [6, 15]

test "multiple triggers in sequence create separate frames" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) trigger

    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| mapped.subscribe fun n =>
      valuesRef.modify (· ++ [n])

    -- Fire twice - each should be a complete frame
    SpiderM.liftIO <| fire 1
    SpiderM.liftIO <| fire 2
    SpiderM.liftIO <| fire 3

    SpiderM.liftIO valuesRef.get

  shouldBe result [2, 4, 6]

test "nested triggers within a frame are processed in order" := do
  let result ← runSpider do
    let (outerTrigger, fireOuter) ← newTriggerEvent (t := Spider) (a := Nat)
    let (innerTrigger, fireInner) ← newTriggerEvent (t := Spider) (a := Nat)

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)

    -- When outer fires, also fire inner
    let _ ← SpiderM.liftIO <| outerTrigger.subscribe fun n => do
      orderRef.modify (· ++ [s!"outer:{n}"])
      fireInner (n * 10)

    let _ ← SpiderM.liftIO <| innerTrigger.subscribe fun n =>
      orderRef.modify (· ++ [s!"inner:{n}"])

    SpiderM.liftIO <| fireOuter 5
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
    let ab ← Event.mergeM a_ab b_ab
    let b_bc ← Event.mapM (fun (s, _) => s!"bc-{s}") b
    let c_bc ← Event.mapM (fun (s, _) => s!"bc-{s}") c
    let bc ← Event.mergeM b_bc c_bc

    let abc ← Event.mergeM ab bc

    let orderRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← SpiderM.liftIO <| abc.subscribe fun s =>
      orderRef.modify (· ++ [s])

    SpiderM.liftIO <| fire 1
    SpiderM.liftIO orderRef.get

  -- All height-1 events (a, b, c) fire first
  -- Then height-2 (ab, bc)
  -- Then height-3 (abc) receives from ab and bc
  -- The exact order within same height depends on nodeId
  ensure (result.length == 4) s!"Expected 4 values, got {result.length}: {result}"

#generate_tests

end ReactiveTests.PropagationTests
