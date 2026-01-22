import Crucible
import Reactive
import Chronos

namespace ReactiveTests.QueueBenchmarks

open Crucible
open Reactive
open Reactive.Host

testSuite "Queue Micro-Benchmarks"

test "bench IO.Ref Array push 100" := do
  let arrRef ← IO.mkRef (#[] : Array Nat)
  let start ← Chronos.MonotonicTime.now

  for i in [:100] do
    arrRef.modify (·.push i)

  let elapsed ← start.elapsed
  let finalArr ← arrRef.get

  shouldBe finalArr.size 100
  IO.println s!"  [IO.Ref 100: {elapsed}]"

test "bench PropagationQueue insert 100" := do
  let queue ← PropagationQueue.new
  queue.setInFrame true
  let start ← Chronos.MonotonicTime.now

  for i in [:100] do
    let pending : PendingFire := {
      height := ⟨i⟩
      nodeId := ⟨i⟩
      fire := pure ()
    }
    queue.insert pending

  let elapsed ← start.elapsed
    -- Verify queue is not empty
  let isEmpty ← queue.isEmpty
  shouldBe isEmpty false
  IO.println s!"  [PropQueue Insert 100: {elapsed}]"

test "bench PropagationQueue pop 100" := do
  let queue ← PropagationQueue.new
  queue.setInFrame true

  -- Pre-fill queue
  for i in [:100] do
    let pending : PendingFire := {
      height := ⟨i⟩
      nodeId := ⟨i⟩
      fire := pure ()
    }
    queue.insert pending

  let start ← Chronos.MonotonicTime.now

  for _ in [:100] do
    let _ ← queue.popMin?

  let elapsed ← start.elapsed
  let isEmpty ← queue.isEmpty

  shouldBe isEmpty true
  IO.println s!"  [PropQueue Pop 100: {elapsed}]"

test "bench Pure Array push 100" := do
  let start ← Chronos.MonotonicTime.now

  -- Pure functional loop using Id monad
  let finalArray := Id.run do
    let mut arr := #[]
    for i in [:100] do
      arr := arr.push i
    arr

  let elapsed ← start.elapsed

  shouldBe finalArray.size 100
  IO.println s!"  [Pure Array 100: {elapsed}]"

test "bench IO.Ref Array push 10000" := do
  let arrRef ← IO.mkRef (#[] : Array Nat)
  let start ← Chronos.MonotonicTime.now

  for i in [:10000] do
    arrRef.modify (·.push i)

  let elapsed ← start.elapsed
  let finalArr ← arrRef.get

  shouldBe finalArr.size 10000
  IO.println s!"  [IO.Ref 10000: {elapsed}]"

test "bench Drain Loop Sim 100" := do
  let queue ← PropagationQueue.new
  queue.setInFrame true

  -- Pre-fill queue
  for i in [:100] do
    let pending : PendingFire := {
      height := ⟨i⟩
      nodeId := ⟨i⟩
      fire := pure ()
    }
    queue.insert pending

  let start ← Chronos.MonotonicTime.now

  -- Inlined loop logic (knowing size is 100)
  for _ in [:100] do
    match ← queue.popMin? with
    | none => pure ()
    | some pending =>
      try
        pending.fire
      catch _ =>
        pure ()

  let elapsed ← start.elapsed
  IO.println s!"  [Drain Loop 100: {elapsed}]"

