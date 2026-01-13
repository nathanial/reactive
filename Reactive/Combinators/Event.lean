/-
  Reactive/Combinators/Event.lean

  Combinators for working with Events.
-/
import Reactive.Core
import Reactive.Class

namespace Reactive

namespace Event

/-- Tag an event with the current value of a behavior (with explicit NodeId).
    On each event occurrence, samples the behavior and returns that value. -/
def tagWithId [Timeline t] (beh : Behavior t a) (e : Event t b) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun _ => do
    let v ← beh.sample
    derived.fire v
  pure derived

/-- Tag an event with the current value of a behavior.
    On each event occurrence, samples the behavior and returns that value.
    Requires TimelineCtx for type-safe timeline separation. -/
def tag [Timeline t] (ctx : TimelineCtx t) (beh : Behavior t a) (e : Event t b) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  tagWithId beh e nodeId

/-- Attach the current behavior value to each event occurrence (with explicit NodeId). -/
def attachWithId [Timeline t] (b : Behavior t a) (e : Event t c) (nodeId : NodeId) : IO (Event t (a × c)) := do
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (a, c)
  pure derived

/-- Attach the current behavior value to each event occurrence.
    Requires TimelineCtx for type-safe timeline separation. -/
def attach [Timeline t] (ctx : TimelineCtx t) (b : Behavior t a) (e : Event t c) : IO (Event t (a × c)) := do
  let nodeId ← ctx.freshNodeId
  attachWithId b e nodeId

/-- Attach with a combining function (with explicit NodeId). -/
def attachWithFnId [Timeline t] (f : a → c → d) (b : Behavior t a) (e : Event t c)
    (nodeId : NodeId) : IO (Event t d) := do
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (f a c)
  pure derived

/-- Attach with a combining function.
    Requires TimelineCtx for type-safe timeline separation. -/
def attachWith [Timeline t] (ctx : TimelineCtx t) (f : a → c → d) (b : Behavior t a) (e : Event t c) : IO (Event t d) := do
  let nodeId ← ctx.freshNodeId
  attachWithFnId f b e nodeId

/-- Gate events by a boolean behavior (with explicit NodeId).
    Only fires when the behavior is true. -/
def gateWithId [Timeline t] (beh : Behavior t Bool) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    let isOpen ← beh.sample
    if isOpen then derived.fire a else pure ()
  pure derived

/-- Gate events by a boolean behavior.
    Only fires when the behavior is true.
    Requires TimelineCtx for type-safe timeline separation. -/
def gate [Timeline t] (ctx : TimelineCtx t) (beh : Behavior t Bool) (e : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  gateWithId beh e nodeId

/-- Merge multiple events into one (with explicit NodeId).
    When multiple events fire simultaneously, all values are collected into a single list.
    Uses frame-based propagation to batch events at the same height. -/
def mergeListWithId [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t (List a)) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)

  -- Buffer for collecting simultaneous occurrences within a frame
  -- Use Array for O(1) push instead of List O(n) append
  let bufferRef ← IO.mkRef (#[] : Array a)
  -- Track whether a flush is already scheduled for this frame
  let flushScheduledRef ← IO.mkRef false

  for e in events do
    let _ ← Reactive.Event.subscribe e fun a => do
      -- Add value to buffer (O(1) amortized)
      bufferRef.modify (·.push a)

      -- Schedule flush at derived height if not already scheduled
      let alreadyScheduled ← flushScheduledRef.get
      if !alreadyScheduled then
        flushScheduledRef.set true

        -- Define the flush action
        let flushAction : IO Unit := do
          flushScheduledRef.set false
          let values ← bufferRef.modifyGet fun vs => (vs.toList, #[])
          if !values.isEmpty then
            derived.fire values

        -- Schedule at derived height so it runs after all source events
        match ← getPropagationContext with
        | some queue =>
          if ← queue.isInFrame then
            let pending : PendingFire := ⟨derived.height, nodeId, flushAction⟩
            queue.insert pending
          else
            -- Not in frame, flush immediately
            flushAction
        | none =>
          -- No propagation context, flush immediately
          flushAction

  pure derived

/-- Merge multiple events into one.
    When multiple events fire simultaneously, all values are collected into a single list.
    Requires TimelineCtx for type-safe timeline separation. -/
def mergeList [Timeline t] (ctx : TimelineCtx t) (events : List (Event t a)) : IO (Event t (List a)) := do
  let nodeId ← ctx.freshNodeId
  mergeListWithId events nodeId

/-- Take the leftmost event that fires (with explicit NodeId).
    When multiple fire simultaneously, takes the first in the list. -/
def leftmostWithId [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)

  for e in events do
    let _ ← Reactive.Event.subscribe e derived.fire

  pure derived

/-- Take the leftmost event that fires.
    When multiple fire simultaneously, takes the first in the list.
    Requires TimelineCtx for type-safe timeline separation. -/
def leftmost [Timeline t] (ctx : TimelineCtx t) (events : List (Event t a)) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  leftmostWithId events nodeId

/-- Split an event of Either into two events (with explicit NodeIds). -/
def fanEitherWithId [Timeline t] (e : Event t (Sum a b)) (nodeIdL : NodeId) (nodeIdR : NodeId)
    : IO (Event t a × Event t b) := do
  let leftEvent ← Event.newNodeWithId nodeIdL (e.height.inc)
  let rightEvent ← Event.newNodeWithId nodeIdR (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun ab =>
    match ab with
    | .inl a => leftEvent.fire a
    | .inr b => rightEvent.fire b
  pure (leftEvent, rightEvent)

/-- Split an event of Either into two events.
    Requires TimelineCtx for type-safe timeline separation. -/
def fanEither [Timeline t] (ctx : TimelineCtx t) (e : Event t (Sum a b)) : IO (Event t a × Event t b) := do
  let nodeIdL ← ctx.freshNodeId
  let nodeIdR ← ctx.freshNodeId
  fanEitherWithId e nodeIdL nodeIdR

/-- Delay an event by one propagation frame (with explicit NodeId).
    Useful for breaking dependency cycles.

    When the source event fires in frame N, the derived event fires
    at the start of frame N+1 (after all frame-N events have been processed).

    This uses the nextFramePending queue in PropagationQueue. -/
def delayFrameWithId [Timeline t] (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    match ← getPropagationContext with
    | some queue =>
      let action := derived.fire a
      let pending : PendingFire := ⟨derived.height, nodeId, action⟩
      -- Add to next frame instead of current frame
      queue.insertNextFrame pending
    | none =>
      -- No frame context, fire immediately (fallback)
      derived.fire a
  pure derived

/-- Delay an event by one propagation frame.
    Useful for breaking dependency cycles.
    Requires TimelineCtx for type-safe timeline separation. -/
def delayFrame [Timeline t] (ctx : TimelineCtx t) (e : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  delayFrameWithId e nodeId

/-- Deprecated alias for delayFrameWithId. Use delayFrame or delayFrameWithId. -/
abbrev delay := @delayFrameWithId

/-- Take only the first n occurrences (with explicit NodeId). -/
def takeNWithId [Timeline t] (n : Nat) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    if count < n then
      countRef.set (count + 1)
      derived.fire a
  pure derived

/-- Take only the first n occurrences.
    Requires TimelineCtx for type-safe timeline separation. -/
def takeN [Timeline t] (ctx : TimelineCtx t) (n : Nat) (e : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  takeNWithId n e nodeId

/-- Drop the first n occurrences (with explicit NodeId). -/
def dropNWithId [Timeline t] (n : Nat) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    countRef.set (count + 1)
    if count >= n then
      derived.fire a
  pure derived

/-- Drop the first n occurrences.
    Requires TimelineCtx for type-safe timeline separation. -/
def dropN [Timeline t] (ctx : TimelineCtx t) (n : Nat) (e : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  dropNWithId n e nodeId

/-- Accumulate a value over event occurrences (with explicit NodeId).
    Emits the new accumulated value on each event occurrence. -/
def accumulateWithId [Timeline t] (f : a → b → b) (initial : b) (e : Event t a)
    (nodeId : NodeId) : IO (Event t b) := do
  let stateRef ← IO.mkRef initial
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    let old ← stateRef.get
    let new := f a old
    stateRef.set new
    derived.fire new
  pure derived

/-- Accumulate a value over event occurrences.
    Emits the new accumulated value on each event occurrence.
    Requires TimelineCtx for type-safe timeline separation. -/
def accumulate [Timeline t] (ctx : TimelineCtx t) (f : a → b → b) (initial : b) (e : Event t a) : IO (Event t b) := do
  let nodeId ← ctx.freshNodeId
  accumulateWithId f initial e nodeId

/-- Alias for accumulate (familiar name from other FRP libraries).
    Like foldDyn but returns an Event instead of a Dynamic. -/
abbrev scan := @accumulate

end Event

end Reactive
