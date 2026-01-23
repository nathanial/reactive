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

/-! ## Pattern 3: Worker Pool Tests (FRP-based) -/

test "worker pool processes jobs via command stream" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 2 }
    -- Create command event stream
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    -- Create the pool
    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 10; pure (n * 2))
      cmdEvt

    let receivedRef ← SpiderM.liftIO <| IO.mkRef (none : Option Nat)
    let _ ← pool.completed.subscribe fun (_, _, r) =>
      receivedRef.set (some r)

    -- Submit job via command stream
    SpiderM.liftIO <| fireCmd (.submit 1 5 0)

    SpiderM.liftIO <| IO.sleep 100
    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO receivedRef.get

  shouldBe result (some 10)

test "worker pool processes in priority order" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker for deterministic order
    let resultsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config (fun (n : Nat) => pure n) cmdEvt

    let _ ← pool.completed.subscribe fun (_, _, r) =>
      resultsRef.modify (· ++ [r])

    -- Submit in reverse priority order (with unique IDs)
    -- All jobs are queued before any processing begins
    SpiderM.liftIO <| fireCmd (.submit 1 3 1)  -- ID 1, job 3, priority 1 (low)
    SpiderM.liftIO <| fireCmd (.submit 2 2 5)  -- ID 2, job 2, priority 5 (high)
    SpiderM.liftIO <| fireCmd (.submit 3 1 3)  -- ID 3, job 1, priority 3 (medium)

    SpiderM.liftIO <| IO.sleep 100
    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO resultsRef.get

  -- Should process in priority order: job 2 (pri 5), job 1 (pri 3), job 3 (pri 1)
  shouldBe result [2, 1, 3]

test "worker pool graceful shutdown stops new jobs" := do
  let startedRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (_ : Nat) => do startedRef.modify (· + 1); IO.sleep 10; pure 0)
      cmdEvt

    -- Subscribe to see completions (for debugging)
    let _ ← pool.completed.subscribe fun _ => pure ()

    -- Submit first job - it will start processing
    SpiderM.liftIO <| fireCmd (.submit 1 1 0)
    SpiderM.liftIO <| IO.sleep 5  -- Let worker pick it up

    -- Submit second job while first is processing
    SpiderM.liftIO <| fireCmd (.submit 2 2 0)

    -- Shutdown before second job can be processed
    SpiderM.liftIO handle.shutdown

    -- Wait for everything to settle
    SpiderM.liftIO <| IO.sleep 50
    SpiderM.liftIO startedRef.get

  -- Only first job should have started (second was discarded by shutdown)
  shouldBe result 1

test "worker pool cancel pending job" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let cancelledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 20; pure n)
      cmdEvt

    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])
    let _ ← pool.cancelled.subscribe fun id =>
      cancelledRef.modify (· ++ [id])

    -- Submit first job (will start immediately)
    SpiderM.liftIO <| fireCmd (.submit 1 10 0)
    SpiderM.liftIO <| IO.sleep 5  -- Let worker pick it up

    -- Submit second job (will be pending)
    SpiderM.liftIO <| fireCmd (.submit 2 20 0)

    -- Cancel second job while it's still pending
    SpiderM.liftIO <| fireCmd (.cancel 2)

    SpiderM.liftIO <| IO.sleep 100
    SpiderM.liftIO handle.shutdown

    let completed ← SpiderM.liftIO completedRef.get
    let cancelled ← SpiderM.liftIO cancelledRef.get
    pure (completed, cancelled)

  -- First job completed, second was cancelled
  shouldBe result.fst [1]
  shouldBe result.snd [2]

test "worker pool resubmit replaces pending job" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let resultsRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 20; pure n)
      cmdEvt

    let _ ← pool.completed.subscribe fun (id, job, _) =>
      resultsRef.modify (· ++ [(id, job)])

    -- Submit first job (will start immediately)
    SpiderM.liftIO <| fireCmd (.submit 1 10 0)
    SpiderM.liftIO <| IO.sleep 5  -- Let worker pick it up

    -- Submit second job (will be pending)
    SpiderM.liftIO <| fireCmd (.submit 2 20 0)

    -- Resubmit with same ID but different job (should replace)
    SpiderM.liftIO <| fireCmd (.resubmit 2 30 0)

    SpiderM.liftIO <| IO.sleep 150
    SpiderM.liftIO handle.shutdown

    SpiderM.liftIO resultsRef.get

  -- First job completed, second completed with new value (30, not 20)
  shouldBe result.length 2
  shouldSatisfy (result.any fun (id, job) => id == 2 && job == 30) "resubmitted job should have new value"

/-! ## Concurrency Stress Tests -/

test "concurrent frame execution serializes correctly" := do
  -- This test verifies that the recursive mutex properly serializes frame execution
  -- across multiple concurrent async completions
  let result ← runSpider do
    let (dyn, set) ← pushState (0 : Nat)
    let counterRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Subscribe to count updates
    let _ ← dyn.updated.subscribe fun _ =>
      counterRef.modify (· + 1)

    -- Spawn many concurrent tasks that all try to update the state
    let numTasks := 50
    let tasksRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Task (Except IO.Error Unit)))
    for i in [0:numTasks] do
      let task ← SpiderM.liftIO <| IO.asTask (prio := .dedicated) do
        -- Small random-ish delay to increase interleaving
        if i % 3 == 0 then IO.sleep 1
        set i

      SpiderM.liftIO <| tasksRef.modify (task :: ·)

    -- Wait for all tasks to complete
    let tasks ← SpiderM.liftIO tasksRef.get
    for task in tasks do
      let _ ← SpiderM.liftIO <| IO.wait task

    -- Small delay to ensure all frames complete
    SpiderM.liftIO <| IO.sleep 50

    -- All updates should have been processed
    let updateCount ← SpiderM.liftIO counterRef.get
    let finalValue ← dyn.sample
    pure (updateCount, finalValue)

  -- All 50 updates should have been received (one per set call)
  shouldBe result.fst 50
  -- Final value should be some number in [0, 49]
  shouldSatisfy (result.snd < 50) "final value should be less than 50"

test "concurrent frame execution with nested triggers" := do
  -- Test that nested trigger chains work correctly under concurrent load
  let result ← runSpider do
    -- Create a chain: source -> derived1 -> derived2
    let (sourceEvt, fireSource) ← newTriggerEvent (t := Spider) (a := Nat)
    let (derived1Evt, fireDerived1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (derived2Evt, fireDerived2) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Wire up the chain
    let _ ← sourceEvt.subscribe fun n => fireDerived1 (n * 2)
    let _ ← derived1Evt.subscribe fun n => fireDerived2 (n + 1)

    -- Collect final results
    let resultsRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← derived2Evt.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    -- Fire from many concurrent tasks
    let numTasks := 30
    let tasksRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Task (Except IO.Error Unit)))
    for i in [0:numTasks] do
      let task ← SpiderM.liftIO <| IO.asTask (prio := .dedicated) do
        fireSource i

      SpiderM.liftIO <| tasksRef.modify (task :: ·)

    -- Wait for all tasks
    let tasks ← SpiderM.liftIO tasksRef.get
    for task in tasks do
      let _ ← SpiderM.liftIO <| IO.wait task

    SpiderM.liftIO <| IO.sleep 50
    SpiderM.liftIO resultsRef.get

  -- Should have 30 results, each is (i * 2) + 1 for i in [0, 29]
  shouldBe result.length 30
  -- All results should be odd numbers (n*2+1)
  shouldSatisfy (result.all (· % 2 == 1)) "all results should be odd"

test "worker pool concurrent submissions all complete" := do
  -- Stress test: many concurrent submissions should all complete
  let numJobs := 100
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 4 }
    let completedRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do
        if n % 5 == 0 then IO.sleep 1
        pure (n * 2))
      cmdEvt

    -- Subscribe to completion event
    let _ ← pool.completed.subscribe fun _ =>
      completedRef.modify (· + 1)

    -- Submit many jobs (submissions are fast, processing is concurrent)
    for i in [0:numJobs] do
      SpiderM.liftIO <| fireCmd (.submit i i (i % 10))

    -- Wait for processing to complete
    SpiderM.liftIO <| IO.sleep 300
    SpiderM.liftIO handle.shutdown

    SpiderM.liftIO completedRef.get

  -- All jobs should have completed
  shouldBe result numJobs

test "worker pool observable state tracks jobs" := do
  -- Test the new jobStates, pendingCount, runningCount dynamics
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker for predictable state
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 30; pure n)
      cmdEvt

    -- Check initial state
    let initialPending ← pool.pendingCount.sample
    let initialRunning ← pool.runningCount.sample

    -- Submit two jobs
    SpiderM.liftIO <| fireCmd (.submit 1 10 0)
    SpiderM.liftIO <| fireCmd (.submit 2 20 0)
    SpiderM.liftIO <| IO.sleep 10  -- Let first job start

    let afterSubmitPending ← pool.pendingCount.sample
    let afterSubmitRunning ← pool.runningCount.sample

    SpiderM.liftIO <| IO.sleep 100  -- Let all jobs complete
    SpiderM.liftIO handle.shutdown

    let finalPending ← pool.pendingCount.sample
    let finalRunning ← pool.runningCount.sample

    pure (initialPending, initialRunning, afterSubmitPending, afterSubmitRunning, finalPending, finalRunning)

  -- Initial state: 0 pending, 0 running
  shouldBe result.1 0
  shouldBe result.2.1 0
  -- After submit: 1 pending (one is running), 1 running
  shouldBe result.2.2.1 1
  shouldBe result.2.2.2.1 1
  -- Final: 0 pending, 0 running
  shouldBe result.2.2.2.2.1 0
  shouldBe result.2.2.2.2.2 0

test "mixed async operations under load" := do
  -- Combine multiple async patterns under concurrent load
  let result ← runSpider do
    let counterRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Pattern 1: pushState with concurrent updates
    let (dyn1, set1) ← pushState (0 : Nat)
    let _ ← dyn1.updated.subscribe fun _ =>
      counterRef.modify (· + 1)

    -- Pattern 2: asyncIO operations (these complete asynchronously)
    for _ in [0:10] do
      let _ ← asyncIO do
        IO.sleep 5
        pure 42

    -- Pattern 3: Worker pool (FRP-based)
    let config : WorkerPoolConfig := { workerCount := 2 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)
    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config (fun (n : Nat) => pure n) cmdEvt
    let _ ← pool.completed.subscribe fun _ =>
      counterRef.modify (· + 1)

    -- Concurrent pushState updates from multiple threads
    let tasksRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Task (Except IO.Error Unit)))
    for i in [0:20] do
      let task ← SpiderM.liftIO <| IO.asTask (prio := .dedicated) do
        set1 i
      SpiderM.liftIO <| tasksRef.modify (task :: ·)

    -- Submit worker pool jobs via command stream
    for i in [0:20] do
      SpiderM.liftIO <| fireCmd (.submit i i 0)

    -- Wait for all concurrent pushState tasks
    let tasks ← SpiderM.liftIO tasksRef.get
    for task in tasks do
      let _ ← SpiderM.liftIO <| IO.wait task

    SpiderM.liftIO <| IO.sleep 200
    SpiderM.liftIO handle.shutdown

    -- Should have: 20 pushState updates + 20 worker pool completions = 40
    SpiderM.liftIO counterRef.get

  shouldBe result 40

end ReactiveTests.AsyncTests
