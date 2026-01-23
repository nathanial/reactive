import Crucible
import Reactive

namespace ReactiveTests.EdgeCaseTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Edge Case Tests"

test "event with zero subscribers can still fire" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    -- No subscribers, but firing should not crash
    trigger 1
    trigger 2
    trigger 3
    pure "ok"
  shouldBe result "ok"

test "firing same value twice notifies twice" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 42
    trigger 42
    trigger 42
    SpiderM.liftIO receivedRef.get
  shouldBe result [42, 42, 42]

test "sample during network construction returns initial value" := do
  let result ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 100 event
    -- Sample immediately during construction
    sample dyn.current
  shouldBe result 100

test "very deep event chain propagates correctly" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a chain of 50 mapped events
    let mut current := source
    for _ in [0:50] do
      current ← Event.mapM (· + 1) current

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← current.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 0
    SpiderM.liftIO receivedRef.get
  -- 0 + 50 increments = 50
  shouldBe result [50]

test "many simultaneous subscribers all receive values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Add 100 subscribers
    for _ in [0:100] do
      let _ ← event.subscribe fun _ =>
        countRef.modify (· + 1)

    trigger 1
    SpiderM.liftIO countRef.get
  shouldBe result 100

test "dynamic with no updates samples initial value" := do
  let result ← runSpider do
    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 999 event
    -- Never fire, just sample
    sample dyn.current
  shouldBe result 999

test "unsubscribe prevents future notifications" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let unsubscribe ← event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    unsubscribe  -- Unsubscribe
    trigger 3  -- Should not be received
    trigger 4  -- Should not be received
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "multiple unsubscribe calls are safe" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let unsubscribe ← event.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    unsubscribe
    unsubscribe  -- Second unsubscribe should be safe
    unsubscribe  -- Third too
    trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [1]

test "behavior samples return current value immediately" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event

    let v0 ← sample dyn.current
    trigger 10
    let v1 ← sample dyn.current
    trigger 20
    let v2 ← sample dyn.current
    pure (v0, v1, v2)
  shouldBe result (0, 10, 20)

test "filter with always-false predicate creates silent event" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let filtered ← Event.filterM (fun _ => false) event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← filtered.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    trigger 3
    SpiderM.liftIO receivedRef.get
  shouldBe result []

-- Stress tests

test "large fan-out: 500 subscribers all receive values" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Add 500 subscribers
    for _ in [0:500] do
      let _ ← event.subscribe fun _ =>
        countRef.modify (· + 1)

    trigger 1
    SpiderM.liftIO countRef.get
  shouldBe result 500

test "rapid subscribe/unsubscribe cycles" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let countRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Rapidly subscribe and unsubscribe 100 times
    for _ in [0:100] do
      let unsub ← event.subscribe fun _ =>
        countRef.modify (· + 1)
      unsub

    -- Now add a permanent subscriber
    let _ ← event.subscribe fun _ =>
      countRef.modify (· + 1)

    trigger 1
    SpiderM.liftIO countRef.get
  -- Only the permanent subscriber should receive
  shouldBe result 1

test "subscribe during event firing" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let subscribed ← SpiderM.liftIO <| IO.mkRef false

    -- First subscriber adds a second subscriber when fired
    let _ ← event.subscribe fun n => do
      receivedRef.modify (· ++ [n])
      let alreadySubscribed ← subscribed.get
      if !alreadySubscribed then
        subscribed.set true
        let _ ← event.subscribe fun m =>
          receivedRef.modify (· ++ [m + 1000])
        pure ()

    trigger 1
    trigger 2
    SpiderM.liftIO receivedRef.get
  -- First fire: [1], second fire: [2, 1002] (new subscriber sees it)
  shouldBe result [1, 2, 1002]

test "unsubscribe during event firing" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let unsubRef ← SpiderM.liftIO <| IO.mkRef (pure () : IO Unit)

    -- Subscriber that unsubscribes itself after first fire
    let unsub ← event.subscribe fun n => do
      receivedRef.modify (· ++ [n])
      let doUnsub ← unsubRef.get
      doUnsub

    SpiderM.liftIO <| unsubRef.set unsub

    trigger 1  -- received, then unsubscribes
    trigger 2  -- not received
    trigger 3  -- not received
    SpiderM.liftIO receivedRef.get
  shouldBe result [1]

test "map chain with 100 transformations" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Build a chain of 100 map operations
    let mut current := source
    for _ in [0:100] do
      current ← Event.mapM (· + 1) current

    let receivedRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← current.subscribe fun n =>
      receivedRef.set n

    trigger 0
    SpiderM.liftIO receivedRef.get
  shouldBe result 100

test "filter chain preserves values through multiple filters" := do
  let result ← runSpider do
    let (source, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Chain multiple filters: keep only multiples of 2, 3, and 5
    let f1 ← Event.filterM (fun n => n % 2 == 0) source
    let f2 ← Event.filterM (fun n => n % 3 == 0) f1
    let f3 ← Event.filterM (fun n => n % 5 == 0) f2

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← f3.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire various numbers
    for i in [1:61] do
      trigger i

    SpiderM.liftIO receivedRef.get
  -- Only multiples of 30 should pass (30, 60)
  shouldBe result [30, 60]

test "takeN with zero takes nothing" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let taken ← Event.takeNM 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← taken.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result []

test "dropN with zero drops nothing" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dropped ← Event.dropNM 0 event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← dropped.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    trigger 1
    trigger 2
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2]

test "accumulate with non-commutative operation preserves order" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := String)
    -- Prepend each new value
    let accumulated ← Event.accumulateM (fun new acc => new ++ acc) "" event

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List String)
    let _ ← accumulated.subscribe fun s =>
      receivedRef.modify (· ++ [s])

    trigger "a"
    trigger "b"
    trigger "c"
    SpiderM.liftIO receivedRef.get
  shouldBe result ["a", "ba", "cba"]

test "multiple events firing in same frame maintain order" := do
  let result ← runSpider do
    let (e1, t1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e2, t2) ← newTriggerEvent (t := Spider) (a := Nat)
    let (e3, t3) ← newTriggerEvent (t := Spider) (a := Nat)

    let merged ← Event.leftmostM [e1, e2, e3]

    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← merged.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire in specific order
    t1 1
    t2 2
    t3 3
    t2 4
    t1 5
    SpiderM.liftIO receivedRef.get
  shouldBe result [1, 2, 3, 4, 5]


end ReactiveTests.EdgeCaseTests
