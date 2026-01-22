import Crucible
import Reactive

namespace ReactiveTests.FluentApiTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Fluent API Tests"

test "Event fluent map' equals regular mapM" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let regularRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let fluentRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let regular ← Event.mapM (· * 2) event
    let fluent ← Event.map' event (· * 2)

    let _ ← SpiderM.liftIO <| regular.subscribe fun n =>
      regularRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| fluent.subscribe fun n =>
      fluentRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3

    let r ← SpiderM.liftIO regularRef.get
    let f ← SpiderM.liftIO fluentRef.get
    pure (r == f)
  shouldBe result true

test "Event fluent filter' equals regular filterM" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let regularRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let fluentRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let regular ← Event.filterM (· > 2) event
    let fluent ← Event.filter' event (· > 2)

    let _ ← SpiderM.liftIO <| regular.subscribe fun n =>
      regularRef.modify (· ++ [n])
    let _ ← SpiderM.liftIO <| fluent.subscribe fun n =>
      fluentRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 3
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 5

    let r ← SpiderM.liftIO regularRef.get
    let f ← SpiderM.liftIO fluentRef.get
    pure (r == f)
  shouldBe result true

test "Dynamic fluent map' equals regular mapM" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 10 event

    let regular ← Dynamic.mapM (· * 3) dyn
    let fluent ← Dynamic.map' dyn (· * 3)

    let r0 ← SpiderM.liftIO <| regular.sample
    let f0 ← SpiderM.liftIO <| fluent.sample

    SpiderM.liftIO <| trigger 5
    let r1 ← SpiderM.liftIO <| regular.sample
    let f1 ← SpiderM.liftIO <| fluent.sample

    pure ((r0 == f0) && (r1 == f1))
  shouldBe result true

test "Dynamic fluent zipWith' equals regular zipWithM" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let d1 ← holdDyn 10 e1
    let d2 ← holdDyn 20 e2

    let regular ← Dynamic.zipWithM (· + ·) d1 d2
    let fluent ← Dynamic.zipWith' d1 (· + ·) d2

    let r0 ← SpiderM.liftIO <| regular.sample
    let f0 ← SpiderM.liftIO <| fluent.sample

    SpiderM.liftIO <| t1 5
    let r1 ← SpiderM.liftIO <| regular.sample
    let f1 ← SpiderM.liftIO <| fluent.sample

    pure ((r0 == f0) && (r1 == f1))
  shouldBe result true

test "fluent API chains work with complex pipelines" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Chain: map -> filter -> map
    let processed ← Event.map' event (· * 2) >>= (Event.filter' · (· > 5)) >>= (Event.map' · (· + 100))

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| processed.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1  -- 1*2=2, filtered (not > 5)
    SpiderM.liftIO <| trigger 3  -- 3*2=6, passes, +100=106
    SpiderM.liftIO <| trigger 5  -- 5*2=10, passes, +100=110

    SpiderM.liftIO receivedRef.get
  shouldBe result [106, 110]

test "fluent scan' accumulates correctly" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    let scanned ← Event.scan' event (· + ·) 0

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← SpiderM.liftIO <| scanned.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO <| trigger 3

    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 3, 6]


end ReactiveTests.FluentApiTests
