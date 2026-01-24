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

-- Helper to wait for a condition with timeout
private partial def waitUntil (condition : IO Bool) (timeoutMs : Nat) (intervalMs : Nat := 5) : IO Bool := do
  let rec loop (remaining : Nat) : IO Bool := do
    if remaining == 0 then return false
    if ← condition then return true
    IO.sleep (UInt32.ofNat intervalMs)
    if remaining > intervalMs then
      loop (remaining - intervalMs)
    else
      return false
  loop timeoutMs

test "worker pool fires errored event on job exception" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (_ : Nat) => do throw (IO.userError "job failed!") : Nat → IO Nat)
      cmdEvt

    let erroredRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let _ ← pool.errored.subscribe fun (id, msg) =>
      erroredRef.modify (· ++ [(id, msg)])

    -- Submit a job that will throw
    SpiderM.liftIO <| fireCmd (.submit 1 100 0)

    -- Wait until error event fires (with timeout)
    let _ ← SpiderM.liftIO <| waitUntil (do return (← erroredRef.get).length >= 1) 500

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO erroredRef.get

  -- Should have received error event for job 1
  shouldBe result.length 1
  match result.head? with
  | some (id, _) => shouldBe id 1
  | none => shouldBe false true  -- Fail: expected an error

test "worker pool rejects duplicate job ID" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 50; pure n)  -- Job takes 50ms
      cmdEvt

    let erroredRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × String))
    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let runningRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let _ ← pool.errored.subscribe fun (id, msg) =>
      erroredRef.modify (· ++ [(id, msg)])
    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])
    -- Track running count changes
    let _ ← pool.runningCount.updated.subscribe fun n =>
      runningRef.set n

    -- Submit job 1
    SpiderM.liftIO <| fireCmd (.submit 1 100 0)

    -- Wait until job 1 is running
    let _ ← SpiderM.liftIO <| waitUntil (do return (← runningRef.get) >= 1) 200

    -- Try to submit another job with same ID - should be rejected
    SpiderM.liftIO <| fireCmd (.submit 1 200 0)

    -- Wait for duplicate error and original completion
    let _ ← SpiderM.liftIO <| waitUntil (do
      let errors ← erroredRef.get
      let completed ← completedRef.get
      return errors.length >= 1 && completed.length >= 1) 500

    SpiderM.liftIO handle.shutdown

    let errors ← SpiderM.liftIO erroredRef.get
    let completed ← SpiderM.liftIO completedRef.get
    pure (errors, completed)

  -- Should have received error event for duplicate ID
  shouldBe result.1.length 1
  match result.1.head? with
  | some (id, _) => shouldBe id 1
  | none => shouldBe false true  -- Fail: expected an error
  -- Original job should still complete (not cancelled by duplicate attempt)
  shouldBe result.2 [1]

test "worker pool soft-cancels running job" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 100; pure n)  -- Slow job (100ms)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let cancelledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let runningRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])
    let _ ← pool.cancelled.subscribe fun id =>
      cancelledRef.modify (· ++ [id])
    let _ ← pool.runningCount.updated.subscribe fun n =>
      runningRef.set n

    -- Submit a slow job
    SpiderM.liftIO <| fireCmd (.submit 1 100 0)

    -- Wait until job is running
    let _ ← SpiderM.liftIO <| waitUntil (do return (← runningRef.get) >= 1) 200

    -- Cancel while running
    SpiderM.liftIO <| fireCmd (.cancel 1)

    -- Wait until cancelled event fires
    let _ ← SpiderM.liftIO <| waitUntil (do
      let cancelled ← cancelledRef.get
      return cancelled.length >= 1) 500

    -- Wait a bit more to ensure job IO completes (result discarded)
    SpiderM.liftIO <| IO.sleep 150

    SpiderM.liftIO handle.shutdown

    let completed ← SpiderM.liftIO completedRef.get
    let cancelled ← SpiderM.liftIO cancelledRef.get
    pure (completed, cancelled)

  -- Job should be cancelled, not completed (soft cancellation discards result)
  shouldBe result.1 []
  shouldBe result.2 [1]

test "worker pool resubmit cancels running job" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 50; pure n)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let cancelledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let runningRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let _ ← pool.completed.subscribe fun (id, payload, _) =>
      completedRef.modify (· ++ [(id, payload)])
    let _ ← pool.cancelled.subscribe fun id =>
      cancelledRef.modify (· ++ [id])
    let _ ← pool.runningCount.updated.subscribe fun n =>
      runningRef.set n

    -- Submit job with payload 100
    SpiderM.liftIO <| fireCmd (.submit 1 100 0)

    -- Wait until job is running
    let _ ← SpiderM.liftIO <| waitUntil (do return (← runningRef.get) >= 1) 200

    -- Resubmit with new payload 200 (should cancel original)
    SpiderM.liftIO <| fireCmd (.resubmit 1 200 0)

    -- Wait for cancellation and then completion with new payload
    let _ ← SpiderM.liftIO <| waitUntil (do
      let cancelled ← cancelledRef.get
      let completed ← completedRef.get
      return cancelled.length >= 1 && completed.length >= 1) 500

    SpiderM.liftIO handle.shutdown

    let completed ← SpiderM.liftIO completedRef.get
    let cancelled ← SpiderM.liftIO cancelledRef.get
    pure (completed, cancelled)

  -- Should have one cancellation and one completion with new payload
  shouldBe result.2 [1]  -- Cancelled once
  shouldBe result.1.length 1  -- One completion
  match result.1.head? with
  | some (_, payload) => shouldBe payload 200  -- With new payload
  | none => shouldBe false true  -- Fail: expected completion

test "worker pool updatePriority reorders pending jobs" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker for deterministic ordering
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 30; pure n)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let runningRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])
    let _ ← pool.runningCount.updated.subscribe fun n =>
      runningRef.set n

    -- Submit a blocking job first to queue up the others
    SpiderM.liftIO <| fireCmd (.submit 0 0 100)  -- High priority, will start immediately

    -- Wait until job 0 is running
    let _ ← SpiderM.liftIO <| waitUntil (do return (← runningRef.get) >= 1) 200

    -- Now submit the test jobs - they'll all be pending while job 0 runs
    SpiderM.liftIO <| fireCmd (.submit 1 10 1)   -- Priority 1 (low)
    SpiderM.liftIO <| fireCmd (.submit 2 20 5)   -- Priority 5 (medium)
    SpiderM.liftIO <| fireCmd (.submit 3 30 10)  -- Priority 10 (high)

    -- Without update, order after job 0 would be: 3, 2, 1
    -- Update job 1's priority to highest (20)
    SpiderM.liftIO <| fireCmd (.updatePriority 1 20)

    -- Wait for all 4 jobs to complete
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 4) 500

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO completedRef.get

  -- Job 0 completes first (it was running)
  -- After priority update, job 1 (priority 20) goes first, then 3 (10), then 2 (5)
  shouldBe result [0, 1, 3, 2]

test "worker pool jobStates tracks all job statuses" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do
        if n == 2 then throw (IO.userError "error!")
        IO.sleep 30
        pure n)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let erroredRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let cancelledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let runningRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])
    let _ ← pool.errored.subscribe fun (id, _) =>
      erroredRef.modify (· ++ [id])
    let _ ← pool.cancelled.subscribe fun id =>
      cancelledRef.modify (· ++ [id])
    let _ ← pool.runningCount.updated.subscribe fun n =>
      runningRef.set n

    -- Submit three jobs with different fates
    SpiderM.liftIO <| fireCmd (.submit 1 1 0)   -- Will complete
    SpiderM.liftIO <| fireCmd (.submit 2 2 0)   -- Will error
    SpiderM.liftIO <| fireCmd (.submit 3 3 0)   -- Will be cancelled

    -- Wait until job 1 is running
    let _ ← SpiderM.liftIO <| waitUntil (do return (← runningRef.get) >= 1) 200

    -- Cancel job 3 (still pending)
    SpiderM.liftIO <| fireCmd (.cancel 3)

    -- Wait until all jobs reach terminal states (1 completed + 1 errored + 1 cancelled)
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      let errored ← erroredRef.get
      let cancelled ← cancelledRef.get
      return completed.length >= 1 && errored.length >= 1 && cancelled.length >= 1) 500

    -- Sample jobStates (now that all events have fired)
    let states ← pool.jobStates.sample
    SpiderM.liftIO handle.shutdown

    pure (states[1]?, states[2]?, states[3]?)

  -- Check each job's final status
  shouldBe result.1 (some JobStatus.completed)
  shouldBe result.2.1 (some JobStatus.error)
  shouldBe result.2.2 (some JobStatus.cancelled)

test "worker pool processes same-priority jobs in FIFO order" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }  -- Single worker for deterministic ordering
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do IO.sleep 10; pure n)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])

    -- Submit jobs all with same priority (5)
    SpiderM.liftIO <| fireCmd (.submit 1 10 5)
    SpiderM.liftIO <| fireCmd (.submit 2 20 5)
    SpiderM.liftIO <| fireCmd (.submit 3 30 5)
    SpiderM.liftIO <| fireCmd (.submit 4 40 5)

    -- Wait for all 4 jobs to complete
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 4) 500

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO completedRef.get

  -- Should complete in FIFO order (1, 2, 3, 4)
  shouldBe result [1, 2, 3, 4]

-- Compile-time type check: fromCommands returns PoolOutput (not a tuple with handle)
-- This ensures the API exists and has the expected return type
#check (WorkerPool.fromCommands : WorkerPoolConfig → (Nat → IO Nat) →
        Evt (PoolCommand Nat Nat) → SpiderM (PoolOutput Nat Nat Nat))

test "worker pool fromCommands basic functionality" := do
  -- Tests that fromCommands creates a working pool
  -- Note: Workers created by fromCommands run until process exits, so we use
  -- fromCommandsWithShutdown for testability while verifying equivalent behavior
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    -- fromCommands internally calls fromCommandsWithShutdown and discards handle
    -- We test the same code path but keep the handle for cleanup
    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => pure (n * 2))
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← pool.completed.subscribe fun (id, _, res) =>
      completedRef.modify (· ++ [(id, res)])

    SpiderM.liftIO <| fireCmd (.submit 1 5 0)
    SpiderM.liftIO <| fireCmd (.submit 2 10 0)

    -- Wait for both jobs to complete
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 2) 500

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO completedRef.get

  -- Basic functionality should work
  shouldBe result.length 2
  -- Results should have correct values (job * 2)
  let sorted := result.toArray.qsort (·.1 < ·.1) |>.toList
  shouldBe sorted [(1, 10), (2, 20)]

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

/-! ## Extended Pool Features Tests -/

test "fromCommandsWithCancel fires started event" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    -- Create processor config
    let processor : ProcessorConfig Nat Nat := ProcessorConfig.simple fun n => do
      IO.sleep 20
      pure (n * 2)

    let (poolEx, handle) ← WorkerPool.fromCommandsWithCancel config processor cmdEvt

    let startedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let _ ← poolEx.started.subscribe fun id =>
      startedRef.modify (· ++ [id])
    let _ ← poolEx.base.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])

    SpiderM.liftIO <| fireCmd (.submit 1 10 0)
    SpiderM.liftIO <| fireCmd (.submit 2 20 0)

    -- Wait for completion
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 2) 500

    SpiderM.liftIO handle.shutdown

    let started ← SpiderM.liftIO startedRef.get
    let completed ← SpiderM.liftIO completedRef.get
    pure (started.length, completed.length)

  -- Both jobs should have fired started and completed events
  shouldBe result.fst 2
  shouldBe result.snd 2

test "fromCommandsChainable enqueues follow-up jobs" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    -- Create chainable processor that spawns follow-ups for job 1
    let processor : ChainableProcessor Nat Nat Nat := {
      process := fun n => do
        if n == 1 then
          -- Job 1 spawns jobs 10 and 11
          pure { result := n * 10, followUps := #[(10, 100, 0), (11, 110, 0)] }
        else
          -- Other jobs don't spawn follow-ups
          pure { result := n * 10 }
    }

    let (pool, handle) ← WorkerPool.fromCommandsChainable config processor cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let _ ← pool.completed.subscribe fun (id, _, res) =>
      completedRef.modify (· ++ [(id, res)])

    -- Submit job 1 (which will spawn 10 and 11)
    SpiderM.liftIO <| fireCmd (.submit 1 1 0)

    -- Wait for all 3 jobs to complete
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 3) 500

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO completedRef.get

  -- Should have completions for jobs 1, 10, and 11
  shouldBe result.length 3
  -- Job 1 should have result 10 (1 * 10)
  shouldSatisfy (result.any fun (id, res) => id == 1 && res == 10) "job 1 completed with result 10"
  -- Follow-up jobs use their payload as input
  shouldSatisfy (result.any fun (id, res) => id == 10 && res == 1000) "follow-up job 10 completed"
  shouldSatisfy (result.any fun (id, res) => id == 11 && res == 1100) "follow-up job 11 completed"

test "withRetryScheduling retries on error" := do
  let attemptRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => do
        let attempt ← attemptRef.modifyGet fun a => (a + 1, a + 1)
        if attempt < 3 then
          throw (IO.userError s!"attempt {attempt} failed")
        pure n)
      cmdEvt

    let retryConfig : RetryConfig := { maxRetries := 3, baseDelayMs := 10, maxDelayMs := 50 }

    let retryScheduledRef ← SpiderM.liftIO <| IO.mkRef ([] : List (Nat × Nat))
    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let scheduler ← withRetryScheduling retryConfig
      (fun _ => true)  -- Always retry
      (fun id => id)   -- Resubmit same job
      pool
      fireCmd

    let _ ← scheduler.retryScheduled.subscribe fun p =>
      retryScheduledRef.modify (· ++ [p])
    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])

    -- Submit job
    SpiderM.liftIO <| fireCmd (.submit 1 1 0)

    -- Wait for completion
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 1) 1000

    SpiderM.liftIO handle.shutdown

    let retries ← SpiderM.liftIO retryScheduledRef.get
    let completed ← SpiderM.liftIO completedRef.get
    pure (retries.length, completed.length)

  let attempts ← attemptRef.get
  -- Should have 3 attempts (2 failures + 1 success)
  shouldBe attempts 3
  -- Should have 2 retry schedules
  shouldBe result.fst 2
  -- Should have 1 completion
  shouldBe result.snd 1

test "withRetryScheduling exhausts retries" := do
  let attemptRef ← IO.mkRef (0 : Nat)
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown (result := Nat) config
      (fun (_ : Nat) => do
        attemptRef.modify (· + 1)
        throw (IO.userError "always fails"))
      cmdEvt

    let retryConfig : RetryConfig := { maxRetries := 2, baseDelayMs := 5, maxDelayMs := 20 }

    let exhaustedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let scheduler ← withRetryScheduling (result := Nat) retryConfig
      (fun _ => true)
      (fun id => id)
      pool
      fireCmd

    let _ ← scheduler.exhausted.subscribe fun p =>
      exhaustedRef.modify (· ++ [p.fst])

    -- Submit job
    SpiderM.liftIO <| fireCmd (.submit 1 1 0)

    -- Wait for exhaustion
    let _ ← SpiderM.liftIO <| waitUntil (do
      let exhausted ← exhaustedRef.get
      return exhausted.length >= 1) 1000

    SpiderM.liftIO handle.shutdown
    SpiderM.liftIO exhaustedRef.get

  let attempts ← attemptRef.get
  -- Should have 3 attempts (initial + 2 retries)
  shouldBe attempts 3
  -- Should have fired exhausted event
  shouldBe result [1]

test "withTwoTierCache uses fast cache" := do
  let processedRef ← IO.mkRef ([] : List Nat)
  let fastCacheRef ← IO.mkRef ({} : Std.HashMap Nat String)

  let cacheConfig : CacheConfig Nat String String := {
    tryFastCache := fun key => do
      let cache ← fastCacheRef.get
      return cache[key]?
    trySlowCache := fun _ => pure none
    promoteToFast := fun key value => fastCacheRef.modify (·.insert key value)
    saveToCache := fun key _ cached => fastCacheRef.modify (·.insert key cached)
    cachedToResult := fun cached => pure cached
  }

  let processor := withTwoTierCache cacheConfig
    (fun job => job)  -- key = job
    (fun job => do
      processedRef.modify (· ++ [job])
      pure s!"result-{job}")
    (fun result => pure result)

  -- First call should process
  let r1 ← processor 42
  shouldBe r1 "result-42"

  -- Pre-populate cache for second call
  fastCacheRef.modify (·.insert 99 "cached-99")

  -- Second call should hit cache
  let r2 ← processor 99
  shouldBe r2 "cached-99"

  -- Check what was processed
  let processed ← processedRef.get
  shouldBe processed [42]  -- Only 42 was processed, 99 was cached

test "submitDelayed command delays job submission" := do
  let result ← runSpider do
    let config : WorkerPoolConfig := { workerCount := 1 }
    let (cmdEvt, fireCmd) ← newTriggerEvent (t := Spider) (a := PoolCommand Nat Nat)

    let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config
      (fun (n : Nat) => pure n)
      cmdEvt

    let completedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)
    let _ ← pool.completed.subscribe fun (id, _, _) =>
      completedRef.modify (· ++ [id])

    -- Submit with delay
    let startTime ← SpiderM.liftIO IO.monoMsNow
    SpiderM.liftIO <| fireCmd (.submitDelayed 1 10 0 100)

    -- Wait for completion
    let _ ← SpiderM.liftIO <| waitUntil (do
      let completed ← completedRef.get
      return completed.length >= 1) 500

    let endTime ← SpiderM.liftIO IO.monoMsNow

    SpiderM.liftIO handle.shutdown

    let elapsed := endTime - startTime
    let completed ← SpiderM.liftIO completedRef.get
    pure (elapsed, completed)

  -- Should have completed after delay
  shouldBe result.snd [1]
  -- Elapsed time should be at least 100ms
  shouldSatisfy (result.fst >= 90) "delay was at least 90ms"

end ReactiveTests.AsyncTests
