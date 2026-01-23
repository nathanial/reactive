import Crucible
import Reactive

namespace ReactiveTests.AsyncTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Async Tests"

/-! ## AsyncState Type Tests -/

test "AsyncState.pending is default" := do
  let state : AsyncState String Nat := default
  shouldBe state.isPending true

test "AsyncState.map transforms ready value" := do
  let state : AsyncState String Nat := .ready 5
  let mapped := state.map (· * 2)
  shouldBe (mapped.toOption) (some 10)

test "AsyncState.map preserves error" := do
  let state : AsyncState String Nat := .error "oops"
  let mapped := state.map (· * 2)
  shouldBe (mapped.toError) (some "oops")

test "AsyncState.map preserves loading" := do
  let state : AsyncState String Nat := .loading
  let mapped := state.map (· * 2)
  shouldBe state.isLoading true

test "AsyncState.isTerminal detects ready and error" := do
  let ready : AsyncState String Nat := .ready 1
  let error : AsyncState String Nat := .error "x"
  let loading : AsyncState String Nat := .loading
  shouldBe ready.isTerminal true
  shouldBe error.isTerminal true
  shouldBe loading.isTerminal false

/-! ## RetryConfig and RetryState Tests -/

test "RetryState.backoffDelayMs calculates exponential delay" := do
  let config : RetryConfig := { baseDelayMs := 100, maxDelayMs := 1000 }
  let s0 : RetryState := { retryCount := 0 }
  let s1 : RetryState := { retryCount := 1 }
  let s2 : RetryState := { retryCount := 2 }
  let s5 : RetryState := { retryCount := 5 }

  shouldBe (s0.backoffDelayMs config) 100   -- 100 * 2^0 = 100
  shouldBe (s1.backoffDelayMs config) 200   -- 100 * 2^1 = 200
  shouldBe (s2.backoffDelayMs config) 400   -- 100 * 2^2 = 400
  shouldBe (s5.backoffDelayMs config) 1000  -- 100 * 2^5 = 3200, capped at 1000

test "RetryState.isExhausted checks maxRetries" := do
  let config : RetryConfig := { maxRetries := 3 }
  let s0 : RetryState := { retryCount := 0 }
  let s2 : RetryState := { retryCount := 2 }
  let s3 : RetryState := { retryCount := 3 }
  let s4 : RetryState := { retryCount := 4 }

  shouldBe (s0.isExhausted config) false
  shouldBe (s2.isExhausted config) false
  shouldBe (s3.isExhausted config) true
  shouldBe (s4.isExhausted config) true

test "RetryState.recordRetryFailure increments count" := do
  let s0 : RetryState := RetryState.initialFailure 1000 "first error"
  let s1 := s0.recordRetryFailure 2000 "second error"

  shouldBe s0.retryCount 0
  shouldBe s1.retryCount 1
  shouldBe s1.lastError (some "second error")

/-! ## Pattern 1: Push-Based State Tests -/

test "pushState updates Dynamic immediately" := do
  let result ← runSpider do
    let (dyn, set) ← pushState (0 : Nat)
    let initial ← dyn.sample
    set 42
    let updated ← dyn.sample
    pure (initial, updated)

  shouldBe result (0, 42)

test "pushState triggers update event" := do
  let result ← runSpider do
    let (dyn, set) ← pushState (0 : Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← dyn.updated.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    set 1
    set 2
    set 3
    SpiderM.liftIO receivedRef.get

  shouldBe result [1, 2, 3]

test "pushStateWithModify applies function" := do
  let result ← runSpider do
    let (dyn, set, modify) ← pushStateWithModify (10 : Nat)
    modify (· + 5)
    let v1 ← dyn.sample
    set 100
    let v2 ← dyn.sample
    modify (· * 2)
    let v3 ← dyn.sample
    pure (v1, v2, v3)

  shouldBe result (15, 100, 200)

/-! ## Pattern 2: Async Resource Tests -/

test "asyncIO starts loading then completes" := do
  let result ← runSpider do
    let dyn ← asyncIO do
      IO.sleep 10
      pure 42

    let initial ← dyn.sample
    SpiderM.liftIO <| IO.sleep 50
    let final ← dyn.sample
    pure (initial.isLoading, final.toOption)

  shouldBe result (true, some 42)

test "asyncIO catches exceptions as error" := do
  let result ← runSpider do
    let dyn ← asyncIO do
      throw (IO.userError "test error")
      pure 0

    SpiderM.liftIO <| IO.sleep 50
    let final ← dyn.sample
    pure final.isError

  shouldBe result true

test "asyncIOCancelable can be canceled" := do
  let result ← runSpider do
    let (dyn, handle) ← asyncIOCancelable do
      IO.sleep 100
      pure 42

    let initial ← dyn.sample
    handle.cancel
    SpiderM.liftIO <| IO.sleep 150
    let final ← dyn.sample
    pure (initial.isLoading, final.isLoading)

  -- After cancel, state stays loading (no update sent)
  shouldBe result (true, true)

test "asyncOnEvent cancels previous on new event" := do
  let result ← runSpider do
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← asyncOnEvent trigger fun n => do
      IO.sleep 50
      pure (n * 10)

    -- Fire first event
    fire 1
    SpiderM.liftIO <| IO.sleep 10

    -- Fire second event before first completes
    fire 2
    SpiderM.liftIO <| IO.sleep 100

    -- Only second result should be visible
    let final ← dyn.sample
    pure final.toOption

  shouldBe result (some 20)

/-! ## Pattern 4: Retry Tests -/

test "asyncWithRetry retries on failure" := do
  let attemptRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : RetryConfig := { maxRetries := 3, baseDelayMs := 10, maxDelayMs := 50 }
    let dyn ← asyncWithRetry config do
      let attempt ← attemptRef.modifyGet fun a => (a + 1, a + 1)
      if attempt < 3 then
        throw (IO.userError s!"attempt {attempt} failed")
      pure attempt

    SpiderM.liftIO <| IO.sleep 200
    let final ← dyn.sample
    pure final.toOption

  let attempts ← attemptRef.get
  shouldBe attempts 3
  shouldBe result (some 3)

test "asyncWithRetry respects maxRetries" := do
  let attemptRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : RetryConfig := { maxRetries := 2, baseDelayMs := 5, maxDelayMs := 20 }
    let dyn ← asyncWithRetry config do
      attemptRef.modify (· + 1)
      throw (IO.userError "always fails")
      pure 0

    SpiderM.liftIO <| IO.sleep 200
    let final ← dyn.sample
    pure final.isError

  let attempts ← attemptRef.get
  -- Initial attempt + 2 retries = 3 attempts total
  shouldBe attempts 3
  shouldBe result true

test "asyncOnEventWithRetry cancels retries on new event" := do
  let attemptRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : RetryConfig := { maxRetries := 5, baseDelayMs := 30, maxDelayMs := 100 }
    let (trigger, fire) ← newTriggerEvent (t := Spider) (a := Nat)
    let dyn ← asyncOnEventWithRetry config trigger fun n => do
      attemptRef.modify (· + 1)
      if n == 1 then
        throw (IO.userError "retry me")
      pure (n * 10)

    -- Fire first event (will retry)
    fire 1
    SpiderM.liftIO <| IO.sleep 20

    -- Fire second event before retries complete (cancels first)
    fire 2
    SpiderM.liftIO <| IO.sleep 100

    -- Second event should succeed
    let final ← dyn.sample
    pure final.toOption

  shouldBe result (some 20)

/-! ## Pattern 3: Worker Pool Tests -/

test "worker pool processes jobs" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 2 }
    let pool ← WorkerPool.new config fun (n : Nat) => do
      IO.sleep 10
      pure (n * 2)

    let resultEvt ← pool.submit 5 0
    let receivedRef ← SpiderM.liftIO <| IO.mkRef (none : Option Nat)
    let _ ← resultEvt.subscribe fun r =>
      receivedRef.set (some r)

    SpiderM.liftIO <| IO.sleep 100
    pool.shutdown
    SpiderM.liftIO receivedRef.get

  shouldBe result (some 10)

test "worker pool processes in priority order" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker for deterministic order
    let resultsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let pool ← WorkerPool.new config fun (n : Nat) => do
      pure n

    let _ ← pool.completed.subscribe fun (_, r) =>
      resultsRef.modify (· ++ [r])

    -- Submit in reverse priority order
    let _ ← pool.submit 3 1  -- Low priority
    let _ ← pool.submit 2 5  -- High priority
    let _ ← pool.submit 1 3  -- Medium priority

    SpiderM.liftIO <| IO.sleep 100
    pool.shutdown
    SpiderM.liftIO resultsRef.get

  -- Should process in priority order: 2 (pri 5), 1 (pri 3), 3 (pri 1)
  shouldBe result [2, 1, 3]

test "worker pool graceful shutdown" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 2 }
    let pool ← WorkerPool.new config fun (_ : Nat) => do
      IO.sleep 10
      pure 0

    -- Submit some jobs
    let _ ← pool.submit 1 0
    let _ ← pool.submit 2 0

    -- Shutdown immediately
    pool.shutdown

    -- Pending should be 0 after shutdown clears queue
    SpiderM.liftIO pool.pending

  shouldBe result 0

end ReactiveTests.AsyncTests
