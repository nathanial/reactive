/-
  Reactive/Host/Spider/WorkerPool.lean

  Priority-based worker pool for async job processing.
  Uses a binary heap for priority ordering with FIFO tiebreaking.
-/
import Reactive.Host.Spider.Core
import Std.Data.HashMap
import Std.Sync.Channel
import Std.Sync.Mutex

namespace Reactive.Host

open Reactive
open Std (HashMap)

/-- Priority for a job, with sequence number for FIFO tiebreaking -/
structure JobPriority where
  /-- Higher priority values are processed first -/
  priority : Int
  /-- Sequence number for FIFO ordering within same priority -/
  sequence : Nat
  deriving Repr, BEq, Inhabited

/-- A job in the worker pool -/
structure PoolJob (α : Type) where
  /-- Unique job ID -/
  id : Nat
  /-- Job priority and sequence -/
  priority : JobPriority
  /-- Generation counter for staleness detection -/
  generation : Nat
  /-- The job payload -/
  payload : α
  deriving Repr

instance [Inhabited α] : Inhabited (PoolJob α) where
  default := { id := 0, priority := default, generation := 0, payload := default }

/-- Configuration for the worker pool -/
structure WorkerPoolConfig where
  /-- Number of worker threads -/
  workerCount : Nat := 4
  deriving Repr, BEq, Inhabited

/-- Worker pool for processing jobs with priority ordering -/
structure WorkerPool (job result : Type) where
  /-- Submit a job with a given priority. Returns an event that fires with the result. -/
  submit : job → Int → SpiderM (Evt result)
  /-- Event that fires when any job completes (with job and result) -/
  completed : Evt (job × result)
  /-- Get number of pending jobs -/
  pending : IO Nat
  /-- Gracefully shutdown the pool -/
  shutdown : IO Unit

namespace WorkerPool

/-- Internal state for the priority queue (max-heap by priority) -/
private structure HeapState (α : Type) where
  /-- Binary heap array -/
  heap : Array (PoolJob α) := #[]
  /-- Next job ID -/
  nextId : Nat := 0
  /-- Next sequence number -/
  nextSequence : Nat := 0
  /-- Current generation (incremented on shutdown) -/
  generation : Nat := 0
  /-- Whether the queue is closed -/
  closed : Bool := false

/-- Check if job a has higher priority than job b -/
@[inline] private def jobHigher (a b : PoolJob α) : Bool :=
  if a.priority.priority == b.priority.priority then
    a.priority.sequence < b.priority.sequence
  else
    a.priority.priority > b.priority.priority

@[inline] private def parentIdx (i : Nat) : Nat := (i - 1) / 2
@[inline] private def leftChildIdx (i : Nat) : Nat := 2 * i + 1
@[inline] private def rightChildIdx (i : Nat) : Nat := 2 * i + 2

@[inline] private def swapJob [Inhabited α] (arr : Array (PoolJob α)) (i j : Nat) : Array (PoolJob α) :=
  let vi := arr[i]!
  let vj := arr[j]!
  (arr.set! i vj).set! j vi

private partial def siftUp [Inhabited α] (arr : Array (PoolJob α)) (i : Nat) : Array (PoolJob α) :=
  if i == 0 then arr
  else
    let pi := parentIdx i
    if pi < arr.size && i < arr.size then
      if jobHigher arr[i]! arr[pi]! then
        siftUp (swapJob arr i pi) pi
      else
        arr
    else
      arr

private partial def siftDown [Inhabited α] (arr : Array (PoolJob α)) (i : Nat) : Array (PoolJob α) :=
  let left := leftChildIdx i
  let right := rightChildIdx i
  let size := arr.size
  let best :=
    let b1 := if left < size && jobHigher arr[left]! arr[i]! then left else i
    if right < size && jobHigher arr[right]! arr[b1]! then right else b1
  if best != i then
    siftDown (swapJob arr i best) best
  else
    arr

private def heapInsert [Inhabited α] (arr : Array (PoolJob α)) (job : PoolJob α) : Array (PoolJob α) :=
  siftUp (arr.push job) arr.size

private def heapPop? [Inhabited α] (arr : Array (PoolJob α)) : Option (PoolJob α) × Array (PoolJob α) :=
  if arr.isEmpty then
    (none, arr)
  else if arr.size == 1 then
    (some arr[0]!, #[])
  else
    let root := arr[0]!
    let last := arr[arr.size - 1]!
    let arr' := arr.pop
    let arr'' := siftDown (arr'.set! 0 last) 0
    (some root, arr'')

/-- Internal job queue using Mutex and CloseableChannel -/
private structure JobQueue (α : Type) where
  state : Std.Mutex (HeapState α)
  signal : Std.CloseableChannel.Sync Unit

private def JobQueue.new (α : Type) : IO (JobQueue α) := do
  let state ← Std.Mutex.new ({} : HeapState α)
  let signal ← Std.CloseableChannel.Sync.new
  return { state, signal }

/-- Close the queue and return the IDs of all pending jobs that were discarded.
    These job IDs should have their triggers cleaned up. -/
private def JobQueue.closeAndGetPendingIds (queue : JobQueue α) : IO (Array Nat) := do
  let pendingIds ← queue.state.atomically do
    let st ← get
    let ids := st.heap.map (·.id)
    modify fun (s : HeapState α) => { s with closed := true, heap := #[], generation := s.generation + 1 }
    pure ids
  try
    let _ ← Std.CloseableChannel.Sync.close queue.signal
    pure ()
  catch _ =>
    pure ()
  return pendingIds

private def JobQueue.close (queue : JobQueue α) : IO Unit := do
  let _ ← queue.closeAndGetPendingIds
  pure ()

private def JobQueue.push [Inhabited α] (queue : JobQueue α) (job : PoolJob α) : IO Unit := do
  let shouldSignal ← queue.state.atomically do
    let st ← get
    if st.closed then
      return false
    else
      modify fun (s : HeapState α) => { s with heap := heapInsert s.heap job }
      return true
  if shouldSignal then
    try
      let _ ← Std.CloseableChannel.Sync.send queue.signal ()
      pure ()
    catch _ =>
      pure ()

/-- Push a job if the queue is open. Returns true if pushed, false if queue is closed. -/
private def JobQueue.pushIfOpen [Inhabited α] (queue : JobQueue α) (job : PoolJob α) : IO Bool := do
  let pushed ← queue.state.atomically do
    let st ← get
    if st.closed then
      return false
    else
      modify fun (s : HeapState α) => { s with heap := heapInsert s.heap job }
      return true
  if pushed then
    try
      let _ ← Std.CloseableChannel.Sync.send queue.signal ()
      pure ()
    catch _ =>
      pure ()
  return pushed

private partial def JobQueue.recv [Inhabited α] (queue : JobQueue α) : IO (Option (PoolJob α)) := do
  let signal? ← queue.signal.recv
  match signal? with
  | none => return none
  | some _ =>
    let job? ← queue.state.atomically do
      let st ← get
      let (job?, heap') := heapPop? st.heap
      modify fun (s : HeapState α) => { s with heap := heap' }
      return job?
    match job? with
    | some job => return some job
    | none =>
      let closed ← queue.state.atomically do
        let st ← get
        return st.closed
      if closed then
        return none
      else
        queue.recv

private def JobQueue.getGeneration (queue : JobQueue α) : IO Nat := do
  queue.state.atomically do
    let st ← get
    return st.generation

private def JobQueue.nextJob (queue : JobQueue α) (jobPayload : α) (priority : Int) : IO (PoolJob α) := do
  queue.state.atomically do
    let st ← get
    let job : PoolJob α := {
      id := st.nextId
      priority := { priority := priority, sequence := st.nextSequence }
      generation := st.generation
      payload := jobPayload
    }
    modify fun (s : HeapState α) => { s with nextId := s.nextId + 1, nextSequence := s.nextSequence + 1 }
    return job

/-- Worker loop that processes jobs until shutdown -/
private partial def workerLoop [Inhabited job] (queue : JobQueue job) (pendingRef : IO.Ref Nat)
    (triggersRef : IO.Ref (HashMap Nat (result → IO Unit)))
    (framedFireCompleted : (job × result) → IO Unit)
    (process : job → IO result) : IO Unit := do
  let rec loop : IO Unit := do
    let maybeJob ← queue.recv
    match maybeJob with
    | none => pure ()  -- Queue closed
    | some theJob =>
      -- Check staleness
      let currentGen ← queue.getGeneration
      if theJob.generation != currentGen then
        pendingRef.modify (· - 1)
        loop
      else
        -- Process the job
        try
          let theResult ← process theJob.payload
          pendingRef.modify (· - 1)

          -- Fire individual result event
          let triggers ← triggersRef.get
          if let some trigger := triggers[theJob.id]? then
            trigger theResult
            triggersRef.modify (·.erase theJob.id)

          -- Fire completed event
          framedFireCompleted (theJob.payload, theResult)
          loop
        catch _ =>
          pendingRef.modify (· - 1)
          triggersRef.modify (·.erase theJob.id)
          loop
  loop

/-- Create a new worker pool.

    **Exception handling**: If `process` throws an exception, it is silently dropped.
    The job's result event will never fire in this case. Callers should handle errors
    within `process` by returning an `Except` or `Option` type rather than throwing.

    **Shutdown behavior**: On shutdown, pending jobs are discarded without processing.
    Their result events will never fire. The `completed` event only fires for jobs
    that successfully complete processing. -/
def new [Inhabited job] (config : WorkerPoolConfig) (process : job → IO result)
    : SpiderM (WorkerPool job result) := ⟨fun env => do
  let jobQueue ← JobQueue.new job
  let pendingRef ← IO.mkRef (0 : Nat)

  -- Map from job ID to result trigger
  let triggersRef ← IO.mkRef ({} : HashMap Nat (result → IO Unit))

  -- Completed event (fires for all completions)
  let (completedEvent, fireCompleted) ← Event.newTrigger env.timelineCtx
  let framedFireCompleted := fun pair => env.withFrame (fireCompleted pair)

  -- Spawn workers
  for _ in [0:config.workerCount] do
    let _ ← IO.asTask (prio := .dedicated)
      (workerLoop jobQueue pendingRef triggersRef framedFireCompleted process)

  let pool : WorkerPool job result := {
    submit := fun jobPayload priority => ⟨fun _ => do
      -- Create result event
      let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx
      let framedFireResult := fun r => env.withFrame (fireResult r)

      -- Create job
      let theJob ← jobQueue.nextJob jobPayload priority

      -- Try to push; if queue is closed, don't register trigger or increment pending
      let wasPushed ← jobQueue.pushIfOpen theJob
      if wasPushed then
        triggersRef.modify (·.insert theJob.id framedFireResult)
        pendingRef.modify (· + 1)

      pure resultEvent⟩

    completed := completedEvent

    pending := pendingRef.get

    shutdown := do
      -- Get all pending job IDs before clearing
      let pendingIds ← jobQueue.closeAndGetPendingIds
      -- Clean up triggers for pending jobs (their result events will never fire)
      for id in pendingIds do
        triggersRef.modify (·.erase id)
      pendingRef.set 0
  }

  pure pool⟩

end WorkerPool

end Reactive.Host
