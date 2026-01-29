/-
  Reactive/Combinators/Event.lean

  Combinators for working with Events.
-/
import Reactive.Core
import Reactive.Class
import Std.Data.HashMap

namespace Reactive

namespace Event

/-- Event selector returned by `fan`.
    Provides per-key events for an Event carrying a HashMap. -/
structure Fan (t : Type) (k : Type) (v : Type) where
  /-- Select the event for a given key, creating it lazily on first use. -/
  select : k → IO (Event t v)

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

/-- Sample a behavior at event occurrence (alias for tagWithId).
    Fires the sampled behavior value whenever the event fires.
    The event value is discarded.

    Example:
    ```
    let mousePos : Behavior Spider Position := ...
    let clickEvent : Event Spider Unit := ...
    let sampledPos ← Event.sample ctx mousePos clickEvent
    -- sampledPos fires the current mouse position on each click
    ``` -/
abbrev sampleWithId := @tagWithId
abbrev sample := @tag

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

/-- Snapshot a behavior at event occurrence (alias for attachWithId).
    Fires a pair of (behavior_value, event_value) on each occurrence.

    Example:
    ```
    let counter : Behavior Spider Nat := ...
    let clickEvent : Event Spider String := ...
    let snapped ← Event.snapshot ctx counter clickEvent
    -- snapped fires (counter_value, click_value) pairs
    ``` -/
abbrev snapshotWithId := @attachWithId
abbrev snapshot := @attach

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
    When multiple fire simultaneously in the same frame, only the first one fires
    (Reflex-style first-only semantics). For all-fire behavior, use `mergeAllListWithId`. -/
def leftmostWithId [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)

  -- Track whether we've already fired in this frame (for first-only)
  let firedThisFrameRef ← IO.mkRef false
  let resetScheduledRef ← IO.mkRef false

  let tryFire (value : a) : IO Unit := do
    let alreadyFired ← firedThisFrameRef.get
    if !alreadyFired then
      firedThisFrameRef.set true
      -- Schedule reset at derived height for next frame
      let needsReset ← resetScheduledRef.get
      if !needsReset then
        resetScheduledRef.set true
        let resetAction : IO Unit := do
          resetScheduledRef.set false
          firedThisFrameRef.set false
        match ← getPropagationContext with
        | some queue =>
          if ← queue.isInFrame then
            queue.insert ⟨derived.height, nodeId, resetAction⟩
          else resetAction
        | none => resetAction
      derived.fire value

  for e in events do
    let _ ← Reactive.Event.subscribe e tryFire

  pure derived

/-- Take the leftmost event that fires.
    When multiple fire simultaneously in the same frame, only the first one fires
    (Reflex-style first-only semantics). For all-fire behavior, use `mergeAllList`. -/
def leftmost [Timeline t] (ctx : TimelineCtx t) (events : List (Event t a)) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  leftmostWithId events nodeId

/-- Merge all events from a list into one (with explicit NodeId).
    All events fire if simultaneous (pre-Reflex behavior).
    For first-only semantics, use `leftmostWithId`. -/
def mergeAllListWithId [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)

  for e in events do
    let _ ← Reactive.Event.subscribe e derived.fire

  pure derived

/-- Merge all events from a list into one.
    All events fire if simultaneous (pre-Reflex behavior).
    For first-only semantics, use `leftmost`. -/
def mergeAllList [Timeline t] (ctx : TimelineCtx t) (events : List (Event t a)) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  mergeAllListWithId events nodeId

/-- Fan out an Event of HashMaps into per-key Events.
    Creates a single subscription to the source event and dispatches only
    to keys that have been selected. -/
def fan [Timeline t] [BEq k] [Hashable k] (ctx : TimelineCtx t) (e : Event t (Std.HashMap k v))
    : IO (Fan t k v) := do
  let selectedRef ← IO.mkRef (∅ : Std.HashMap k (Event t v))
  let _ ← Reactive.Event.subscribe e fun values => do
    let selected ← selectedRef.get
    for (k, v) in values do
      match selected.get? k with
      | some target => target.fire v
      | none => pure ()
  let selectFn := fun key => do
    let selected ← selectedRef.get
    match selected.get? key with
    | some target => pure target
    | none =>
        let nodeId ← ctx.freshNodeId
        let target ← Event.newNodeWithId nodeId (e.height.inc)
        selectedRef.modify (·.insert key target)
        pure target
  pure ⟨selectFn⟩

/-- Select a keyed Event from a fan-out selector. -/
def select (fan : Fan t k v) (key : k) : IO (Event t v) :=
  fan.select key

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

/-- Split an event into two based on a predicate (with explicit NodeIds).
    Returns (trueEvent, falseEvent) where trueEvent fires when predicate is true,
    and falseEvent fires when predicate is false. -/
def splitEWithId [Timeline t] (p : a → Bool) (e : Event t a) (nodeIdT : NodeId) (nodeIdF : NodeId)
    : IO (Event t a × Event t a) := do
  let trueEvent ← Event.newNodeWithId nodeIdT (e.height.inc)
  let falseEvent ← Event.newNodeWithId nodeIdF (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a =>
    if p a then trueEvent.fire a else falseEvent.fire a
  pure (trueEvent, falseEvent)

/-- Split an event into two based on a predicate.
    Returns (trueEvent, falseEvent) where trueEvent fires when predicate is true,
    and falseEvent fires when predicate is false.
    Requires TimelineCtx for type-safe timeline separation.

    Example:
    ```
    let (evens, odds) ← Event.splitE ctx (· % 2 == 0) numbers
    -- evens fires for even numbers, odds fires for odd numbers
    ``` -/
def splitE [Timeline t] (ctx : TimelineCtx t) (p : a → Bool) (e : Event t a)
    : IO (Event t a × Event t a) := do
  let nodeIdT ← ctx.freshNodeId
  let nodeIdF ← ctx.freshNodeId
  splitEWithId p e nodeIdT nodeIdF

/-- Partition an event into two based on a predicate (with explicit NodeIds).
    Returns (matching, nonMatching) where matching fires when predicate is true.
    Alias for splitEWithId with Haskell-style naming. -/
abbrev partitionEWithId := @splitEWithId

/-- Partition an event into two based on a predicate.
    Returns (matching, nonMatching) where matching fires when predicate is true.
    Alias for splitE with Haskell-style naming.

    Example:
    ```
    let (evens, odds) ← Event.partitionE ctx (· % 2 == 0) numbers
    ``` -/
abbrev partitionE := @splitE

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

/-- Take only the first occurrence of an event (with explicit NodeId).
    Specialization of `takeNWithId 1` for convenience. -/
abbrev onceWithId [Timeline t] (e : Event t a) (nodeId : NodeId) : IO (Event t a) :=
  takeNWithId 1 e nodeId

/-- Take only the first occurrence of an event.
    Specialization of `takeN 1` for convenience.
    Requires TimelineCtx for type-safe timeline separation. -/
abbrev once [Timeline t] (ctx : TimelineCtx t) (e : Event t a) : IO (Event t a) :=
  takeN ctx 1 e

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

/-- Emit (previous, current) pairs on each event occurrence (with explicit NodeId).
    Skips the first occurrence since there's no previous value.
    Useful for detecting changes and computing deltas. -/
def withPreviousWithId [Timeline t] (e : Event t a) (nodeId : NodeId) : IO (Event t (a × a)) := do
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    prevRef.set (some curr)
    match prev with
    | some p => derived.fire (p, curr)
    | none => pure ()
  pure derived

/-- Emit (previous, current) pairs on each event occurrence.
    Skips the first occurrence since there's no previous value.
    Useful for detecting changes and computing deltas.

    Example:
    ```
    let positions : Event Spider Float := ...
    let deltas ← Event.withPrevious ctx positions
    -- deltas fires with (oldPos, newPos) for computing velocity
    ``` -/
def withPrevious [Timeline t] (ctx : TimelineCtx t) (e : Event t a) : IO (Event t (a × a)) := do
  let nodeId ← ctx.freshNodeId
  withPreviousWithId e nodeId

/-- Skip consecutive duplicate values (with explicit NodeId).
    Requires BEq to compare values. Only fires when value differs from previous. -/
def distinctWithId [Timeline t] [BEq a] (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    let shouldFire := match prev with
      | none => true  -- First value always fires
      | some p => curr != p
    if shouldFire then
      prevRef.set (some curr)
      derived.fire curr
  pure derived

/-- Skip consecutive duplicate values.
    Requires BEq to compare values. Only fires when value differs from previous.
    Common need to avoid redundant downstream updates.

    Example:
    ```
    let mouseX : Event Spider Int := ...
    let uniqueX ← Event.distinct ctx mouseX
    -- uniqueX only fires when X actually changes
    ``` -/
def distinct [Timeline t] [BEq a] (ctx : TimelineCtx t) (e : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  distinctWithId e nodeId

/-- Alias for distinct (familiar name from RxJS/reactive libraries). -/
abbrev dedupe := @distinct

/-- Collect n events before emitting them as a batch (with explicit NodeId).
    Useful for batch processing patterns. -/
def bufferWithId [Timeline t] (n : Nat) (e : Event t a) (nodeId : NodeId)
    : IO (Event t (Array a)) := do
  let bufRef ← IO.mkRef (#[] : Array a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let _ ← Reactive.Event.subscribe e fun a => do
    let buf ← bufRef.get
    let newBuf := buf.push a
    if newBuf.size >= n then
      bufRef.set #[]
      derived.fire newBuf
    else
      bufRef.set newBuf
  pure derived

/-- Collect n events before emitting them as a batch.
    Useful for batch processing patterns.

    Example:
    ```
    let clicks : Event Spider Unit := ...
    let batches ← Event.buffer ctx 5 clicks
    -- batches fires with array of 5 clicks
    ``` -/
def buffer [Timeline t] (ctx : TimelineCtx t) (n : Nat) (e : Event t a)
    : IO (Event t (Array a)) := do
  let nodeId ← ctx.freshNodeId
  bufferWithId n e nodeId

/-- Combine two events that fire simultaneously (with explicit NodeId).
    If both events fire in the same frame, fires once with paired values.
    If only one fires, nothing is emitted for that occurrence. -/
def zipEWithId [Timeline t] (e1 : Event t a) (e2 : Event t b) (nodeId : NodeId)
    : IO (Event t (a × b)) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height

  let value1Ref ← IO.mkRef (none : Option a)
  let value2Ref ← IO.mkRef (none : Option b)
  let flushScheduledRef ← IO.mkRef false

  let scheduleFlush : IO Unit := do
    let alreadyScheduled ← flushScheduledRef.get
    if !alreadyScheduled then
      flushScheduledRef.set true
      let flushAction : IO Unit := do
        flushScheduledRef.set false
        let v1opt ← value1Ref.get
        let v2opt ← value2Ref.get
        -- Always clear values after checking - values only valid within same frame
        value1Ref.set none
        value2Ref.set none
        match (v1opt, v2opt) with
        | (some v1, some v2) => derived.fire (v1, v2)
        | _ => pure ()
      match ← getPropagationContext with
      | some queue =>
        if ← queue.isInFrame then
          let pending : PendingFire := ⟨derived.height, nodeId, flushAction⟩
          queue.insert pending
        else flushAction
      | none => flushAction

  let _ ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush

  let _ ← Reactive.Event.subscribe e2 fun b => do
    value2Ref.set (some b)
    scheduleFlush

  pure derived

/-- Combine two events that fire simultaneously.
    Requires TimelineCtx for type-safe timeline separation. -/
def zipE [Timeline t] (ctx : TimelineCtx t) (e1 : Event t a) (e2 : Event t b)
    : IO (Event t (a × b)) := do
  let nodeId ← ctx.freshNodeId
  zipEWithId e1 e2 nodeId

/-- Fire when e1 occurs but e2 does not (in the same frame).
    Useful for "A but not B" patterns.

    Example:
    ```
    let clicks : Event Spider Unit := ...
    let drags : Event Spider Unit := ...
    let clicksOnly ← Event.difference ctx clicks drags
    -- clicksOnly fires only for clicks that aren't also drags
    ``` -/
def differenceWithId [Timeline t] (e1 : Event t a) (e2 : Event t b) (nodeId : NodeId)
    : IO (Event t a) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height

  let value1Ref ← IO.mkRef (none : Option a)
  let value2FiredRef ← IO.mkRef false
  let flushScheduledRef ← IO.mkRef false

  let scheduleFlush : IO Unit := do
    let alreadyScheduled ← flushScheduledRef.get
    if !alreadyScheduled then
      flushScheduledRef.set true
      let flushAction : IO Unit := do
        flushScheduledRef.set false
        let v1opt ← value1Ref.get
        let v2fired ← value2FiredRef.get
        value1Ref.set none
        value2FiredRef.set false
        match v1opt with
        | some v1 => if !v2fired then derived.fire v1 else pure ()
        | none => pure ()
      match ← getPropagationContext with
      | some queue =>
        if ← queue.isInFrame then
          queue.insert ⟨derived.height, nodeId, flushAction⟩
        else flushAction
      | none => flushAction

  let _ ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush

  let _ ← Reactive.Event.subscribe e2 fun _ => do
    value2FiredRef.set true
    scheduleFlush

  pure derived

/-- Fire when e1 occurs but e2 does not (in the same frame).
    Requires TimelineCtx for type-safe timeline separation. -/
def difference [Timeline t] (ctx : TimelineCtx t) (e1 : Event t a) (e2 : Event t b)
    : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  differenceWithId e1 e2 nodeId

end Event

end Reactive
