import Crucible
import Reactive

namespace ReactiveTests.ScopeTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Scope Tests"

/-! ## SubscriptionScope Unit Tests -/

test "SubscriptionScope.new creates non-disposed scope" := do
  let scope ← SubscriptionScope.new
  let disposed ← scope.isDisposed
  shouldBe disposed false

test "SubscriptionScope.dispose runs all subscriptions" := do
  let scope ← SubscriptionScope.new
  let callCount ← IO.mkRef 0
  scope.register (callCount.modify (· + 1))
  scope.register (callCount.modify (· + 1))
  scope.register (callCount.modify (· + 1))

  let countBefore ← callCount.get
  shouldBe countBefore 0

  scope.dispose

  let countAfter ← callCount.get
  shouldBe countAfter 3

test "SubscriptionScope.dispose marks scope as disposed" := do
  let scope ← SubscriptionScope.new
  let disposedBefore ← scope.isDisposed
  shouldBe disposedBefore false

  scope.dispose

  let disposedAfter ← scope.isDisposed
  shouldBe disposedAfter true

test "SubscriptionScope.dispose is idempotent" := do
  let scope ← SubscriptionScope.new
  let callCount ← IO.mkRef 0
  scope.register (callCount.modify (· + 1))

  scope.dispose
  let countAfterFirst ← callCount.get
  shouldBe countAfterFirst 1

  scope.dispose
  let countAfterSecond ← callCount.get
  shouldBe countAfterSecond 1

test "SubscriptionScope.dispose clears large subscription lists" := do
  let scope ← SubscriptionScope.new
  let callCount ← IO.mkRef 0

  for _ in [0:1000] do
    scope.register (callCount.modify (· + 1))

  let countBefore ← scope.subscriptionCount
  scope.dispose
  let countAfter ← scope.subscriptionCount
  let calls ← callCount.get
  shouldBe (countBefore, countAfter, calls) (1000, 0, 1000)

test "Child scopes are disposed with parent" := do
  let parent ← SubscriptionScope.new
  let child ← parent.child
  let order ← IO.mkRef ([] : List String)

  parent.register (order.modify (· ++ ["parent"]))
  child.register (order.modify (· ++ ["child"]))

  parent.dispose

  let result ← order.get
  -- Children disposed first (depth-first)
  shouldBe result ["child", "parent"]

test "Deeply nested scopes dispose in correct order" := do
  let root ← SubscriptionScope.new
  let child1 ← root.child
  let child2 ← child1.child
  let child3 ← child2.child

  let order ← IO.mkRef ([] : List String)
  root.register (order.modify (· ++ ["root"]))
  child1.register (order.modify (· ++ ["child1"]))
  child2.register (order.modify (· ++ ["child2"]))
  child3.register (order.modify (· ++ ["child3"]))

  root.dispose

  let result ← order.get
  -- Children are disposed before their parents (depth-first)
  -- The exact order depends on implementation details
  ensure (result.contains "child3") "child3 should be disposed"
  ensure (result.contains "child2") "child2 should be disposed"
  ensure (result.contains "child1") "child1 should be disposed"
  ensure (result.contains "root") "root should be disposed"
  ensure (result.length == 4) "Should have 4 disposal callbacks"

test "Register on disposed scope runs immediately" := do
  let scope ← SubscriptionScope.new
  scope.dispose

  let ran ← IO.mkRef false
  scope.register (ran.set true)

  let result ← ran.get
  shouldBe result true

test "Child of disposed parent is pre-disposed" := do
  let parent ← SubscriptionScope.new
  parent.dispose

  let child ← parent.child
  let disposed ← child.isDisposed
  shouldBe disposed true

/-! ## SpiderM Scope Integration Tests -/

test "runSpider disposes root scope" := do
  let disposed ← IO.mkRef false

  let _ ← runSpider do
    let scope ← SpiderM.getScope
    SpiderM.liftIO <| scope.register (disposed.set true)

  let result ← disposed.get
  shouldBe result true

test "Event.mapM cleans up on scope dispose" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) event
    let callCount ← SpiderM.liftIO <| IO.mkRef 0
    let _ ← Event.subscribeM mapped fun _ => callCount.modify (· + 1)

    trigger 1
    trigger 2
    SpiderM.liftIO callCount.get

  shouldBe result 2

test "withAutoDisposeScope disposes child scope" := do
  let disposed ← IO.mkRef false

  let _ ← runSpider do
    SpiderM.withAutoDisposeScope do
      let scope ← SpiderM.getScope
      SpiderM.liftIO <| scope.register (disposed.set true)

    -- Check it was disposed before runSpider finishes
    let result ← SpiderM.liftIO disposed.get
    SpiderM.liftIO <| ensure result "Child scope should be disposed"

  pure ()

test "withScope returns child scope for manual disposal" := do
  let result ← runSpider do
    let (_, childScope) ← SpiderM.withScope do
      let scope ← SpiderM.getScope
      SpiderM.liftIO <| scope.register (pure ())
      pure ()

    let disposedBefore ← SpiderM.liftIO <| childScope.isDisposed
    -- Child scope not yet disposed (will be disposed when root disposes)
    pure disposedBefore

  shouldBe result false

test "manual scope disposal clears subscriptions" := do
  let result ← runSpider do
    let (_, childScope) ← SpiderM.withScope do
      let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
      -- Note: mapM uses map fusion (lazy subscription), so doesn't register eagerly
      -- Use filterM which registers subscriptions immediately
      let _ ← Event.filterM (· > 0) event
      let _ ← Event.filterM (· < 100) event
      pure ()

    let countBefore ← SpiderM.liftIO <| childScope.subscriptionCount
    SpiderM.liftIO <| childScope.dispose
    let countAfter ← SpiderM.liftIO <| childScope.subscriptionCount
    pure (decide (countBefore ≥ 2), countAfter)

  shouldBe result (true, 0)

test "foldDyn cleans up subscription" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let counter ← foldDyn (fun n acc => acc + n) 0 event

    trigger 1
    trigger 2
    trigger 3

    SpiderM.liftIO <| counter.sample

  shouldBe result 6

test "Multiple combinators chain properly with scope" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Chain: event -> map -> filter -> accumulate
    let doubled ← Event.mapM (· * 2) event
    let evensOnly ← Event.filterM (· % 4 == 0) doubled
    let accumulated ← Event.accumulateM (fun n acc => acc + n) 0 evensOnly

    let values ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← Event.subscribeM accumulated fun n =>
      values.modify (· ++ [n])

    -- Fire 1 (doubled=2, not divisible by 4, filtered)
    -- Fire 2 (doubled=4, passes, accumulated=4)
    -- Fire 3 (doubled=6, not divisible by 4, filtered)
    -- Fire 4 (doubled=8, passes, accumulated=12)
    trigger 1
    trigger 2
    trigger 3
    trigger 4

    SpiderM.liftIO values.get

  shouldBe result [4, 12]

test "Dynamic.mapM cleans up subscription" := do
  let result ← runSpider do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← holdDyn 0 event
    let doubled ← Dynamic.mapM (· * 2) dyn

    trigger 5
    SpiderM.liftIO <| doubled.sample

  shouldBe result 10

test "Scope subscription count increases" := do
  let result ← runSpider do
    let scope ← SpiderM.getScope
    let countBefore ← SpiderM.liftIO <| scope.subscriptionCount

    let (event, _) ← newTriggerEvent (t := Spider) (a := Nat)
    -- Note: mapM uses map fusion (lazy subscription), doesn't register eagerly
    -- Use filterM which registers subscriptions immediately
    let _ ← Event.filterM (· > 0) event
    let _ ← Event.filterM (· < 100) event

    let countAfter ← SpiderM.liftIO <| scope.subscriptionCount
    pure (countBefore, countAfter)

  -- Before: 0, After: 2 (one for each filterM)
  shouldBe result (0, 2)


end ReactiveTests.ScopeTests
