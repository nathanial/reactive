/-
  Reactive/Host/Spider/Event.lean

  Event SpiderM combinators for the Spider FRP runtime.
-/
import Reactive.Host.Spider.Core
import Chronos

namespace Reactive.Host

/-! ## Event SpiderM Combinators

These provide ergonomic versions of Event operations that auto-allocate NodeIds
and register subscriptions with the current scope for automatic cleanup. -/

namespace Event

/-- Subscribe to an event within SpiderM, auto-registering with the current scope.
    The subscription is automatically cleaned up when the scope is disposed.

    WARNING: This is protected for internal use by combinators. Application code
    should use pure FRP combinators like `foldDyn`, `holdDyn`, `mapM`, `filterM`,
    `attachWith`, etc. instead of manual subscriptions. Manual subscribe can lead
    to imperative patterns that break FRP semantics. -/
protected def subscribeM (e : Event Spider a) (callback : Subscriber a) : SpiderM (IO Unit) :=
  ⟨fun env => Reactive.Event.subscribeScoped e env.currentScope callback⟩

/-- The event that never fires.
    Useful as a placeholder or identity for merge operations. -/
def neverM : SpiderM (Event Spider a) := ⟨fun env => do
  let ctx := env.timelineCtx
  Event.never ctx⟩

/-- The event that never fires (fluent style).
    Enables: `Event.never'` -/
abbrev never' : SpiderM (Event Spider a) := neverM

/-- Map a function over an Event, auto-allocating NodeId and registering with scope. -/
def mapM (f : a → b) (e : Event Spider a) : SpiderM (Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.mapWithId f e nodeId
  env.decrementDepth
  pure derived⟩

/-- Map a constant addition over an Event, fusing successive add-const maps. -/
def mapAddConstM [HAdd a a a] (c : a) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapAddConstM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.mapAddConstWithId c e nodeId
  env.decrementDepth
  pure derived⟩

/-- Filter an Event by a predicate, auto-allocating NodeId and registering with scope. -/
def filterM (p : a → Bool) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.filterM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    if p a then derived.fire a else pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Filter and map an Event, auto-allocating NodeId and registering with scope. -/
def mapMaybeM (f : a → Option b) (e : Event Spider a) : SpiderM (Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mapMaybeM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    match f a with
    | some b => derived.fire b
    | none => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Discard event values, mapping all occurrences to Unit. -/
def voidM (e : Event Spider a) : SpiderM (Event Spider Unit) :=
  mapM (fun _ => ()) e

/-- Map an event to a constant value, ignoring the original values. -/
def mapConstM (b : β) (e : Event Spider α) : SpiderM (Event Spider β) :=
  mapM (fun _ => b) e

/-- Merge two Events with left-bias (Reflex-style semantics).
    When both events fire simultaneously in the same frame, only the left event's
    value is delivered. For all-fire behavior, use `mergeAllM` instead. -/
def mergeM (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeM"
  let nodeId ← env.timelineCtx.freshNodeId
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height

  -- Track whether we've already fired in this frame (for left-bias)
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

  -- e1 (left) fires first due to subscription order
  let unsub1 ← Reactive.Event.subscribe e1 tryFire
  let unsub2 ← Reactive.Event.subscribe e2 tryFire
  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Merge two Events, firing all values (both events fire if simultaneous).
    This preserves the pre-Reflex behavior where simultaneous events both fire.
    For left-bias semantics, use `mergeM` instead. -/
def mergeAllM (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeAllM"
  let nodeId ← env.timelineCtx.freshNodeId
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId nodeId height
  let unsub1 ← Reactive.Event.subscribe e1 derived.fire
  let unsub2 ← Reactive.Event.subscribe e2 derived.fire
  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Tag an Event with a Behavior's current value, auto-allocating NodeId and registering with scope. -/
def tagM (beh : Behavior Spider a) (e : Event Spider b) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.tagM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun _ => do
    let v ← beh.sample
    derived.fire v
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Sample a behavior at event occurrence (SpiderM version, alias for tagM). -/
abbrev sampleM := @tagM

/-- Attach a Behavior's value to an Event, auto-allocating NodeId and registering with scope. -/
def attachM (b : Behavior Spider a) (e : Event Spider c) : SpiderM (Event Spider (a × c)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.attachM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (a, c)
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Snapshot a behavior at event occurrence (SpiderM version, alias for attachM). -/
abbrev snapshotM := @attachM

/-- Attach with a combining function, auto-allocating NodeId and registering with scope. -/
def attachWithM (f : a → c → d) (b : Behavior Spider a) (e : Event Spider c)
    : SpiderM (Event Spider d) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.attachWithM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun c => do
    let a ← b.sample
    derived.fire (f a c)
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Gate an Event by a Boolean Behavior, auto-allocating NodeId and registering with scope. -/
def gateM (beh : Behavior Spider Bool) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.gateM"
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let isOpen ← beh.sample
    if isOpen then derived.fire a else pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Merge a list of Events into a list Event, auto-allocating NodeId and registering with scope. -/
def mergeListM (events : List (Event Spider a)) : SpiderM (Event Spider (List a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeListM"
  let nodeId ← env.timelineCtx.freshNodeId
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)
  -- Use Array for O(1) push instead of List O(n) append
  let bufferRef ← IO.mkRef (#[] : Array a)
  let flushScheduledRef ← IO.mkRef false

  for e in events do
    let unsub ← Reactive.Event.subscribe e fun a => do
      bufferRef.modify (·.push a)
      let alreadyScheduled ← flushScheduledRef.get
      if !alreadyScheduled then
        flushScheduledRef.set true
        let flushAction : IO Unit := do
          flushScheduledRef.set false
          let values ← bufferRef.modifyGet fun vs => (vs.toList, #[])
          if !values.isEmpty then derived.fire values
        match ← getPropagationContext with
        | some queue =>
          if ← queue.isInFrame then
            let pending : PendingFire := ⟨derived.height, nodeId, flushAction⟩
            queue.insert pending
          else flushAction
        | none => flushAction
    env.currentScope.register unsub

  env.decrementDepth
  pure derived⟩

/-- Take the leftmost firing Event from a list (Reflex-style first-only semantics).
    When multiple events in the list fire simultaneously, only the first one's value
    is delivered. For all-fire behavior, use `mergeAllListM` instead. -/
def leftmostM (events : List (Event Spider a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.leftmostM"
  let nodeId ← env.timelineCtx.freshNodeId
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
    let unsub ← Reactive.Event.subscribe e tryFire
    env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Merge all events from a list into one (all events fire if simultaneous).
    This preserves the pre-Reflex behavior where all simultaneous events fire.
    For first-only semantics, use `leftmostM` instead. -/
def mergeAllListM (events : List (Event Spider a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.mergeAllListM"
  let nodeId ← env.timelineCtx.freshNodeId
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNodeWithId nodeId (maxHeight.inc)
  for e in events do
    let unsub ← Reactive.Event.subscribe e derived.fire
    env.currentScope.register unsub
  env.decrementDepth
  pure derived⟩

/-- Fan out an Event of HashMaps into per-key Events, using a single subscription.
    The fan-out subscription is registered with the current scope. -/
def fanM [BEq k] [Hashable k] (e : Event Spider (Std.HashMap k v))
    : SpiderM (Event.Fan Spider k v) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.fanM"
  let selectedRef ← IO.mkRef (∅ : Std.HashMap k (Event Spider v))
  let ctx := env.timelineCtx
  let unsub ← Reactive.Event.subscribe e fun values => do
    let selected ← selectedRef.get
    for (k, v) in values do
      match selected.get? k with
      | some target => target.fire v
      | none => pure ()
  env.currentScope.register unsub
  let selectFn := fun key => do
    let selected ← selectedRef.get
    match selected.get? key with
    | some target => pure target
    | none =>
        let nodeId ← ctx.freshNodeId
        let target ← Event.newNodeWithId nodeId (e.height.inc)
        selectedRef.modify (·.insert key target)
        pure target
  env.decrementDepth
  pure ⟨selectFn⟩⟩

/-- Select a keyed Event from a fan-out selector within SpiderM. -/
def selectM (fan : Event.Fan Spider k v) (key : k) : SpiderM (Event Spider v) :=
  SpiderM.liftIO <| Event.select fan key

/-- Fan out a Sum Event into two Events, auto-allocating NodeIds and registering with scope. -/
def fanEitherM (e : Event Spider (Sum a b)) : SpiderM (Event Spider a × Event Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.fanEitherM"
  let nodeIdL ← env.timelineCtx.freshNodeId
  let nodeIdR ← env.timelineCtx.freshNodeId
  let leftEvent ← Event.newNodeWithId nodeIdL (e.height.inc)
  let rightEvent ← Event.newNodeWithId nodeIdR (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun ab =>
    match ab with
    | .inl a => leftEvent.fire a
    | .inr b => rightEvent.fire b
  env.currentScope.register unsub
  env.decrementDepth
  pure (leftEvent, rightEvent)⟩

/-- Split an event into two based on a predicate.
    Returns (trueEvent, falseEvent) where trueEvent fires when predicate is true,
    and falseEvent fires when predicate is false. -/
def splitEM (p : a → Bool) (e : Event Spider a)
    : SpiderM (Event Spider a × Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.splitEM"
  let nodeIdT ← env.timelineCtx.freshNodeId
  let nodeIdF ← env.timelineCtx.freshNodeId
  let trueEvent ← Event.newNodeWithId nodeIdT (e.height.inc)
  let falseEvent ← Event.newNodeWithId nodeIdF (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a =>
    if p a then trueEvent.fire a else falseEvent.fire a
  env.currentScope.register unsub
  env.decrementDepth
  pure (trueEvent, falseEvent)⟩

/-- Split an event into two based on a predicate (fluent style).
    Enables: `event.splitE' predicate` -/
def splitE' (e : Event Spider a) (p : a → Bool)
    : SpiderM (Event Spider a × Event Spider a) :=
  splitEM p e

/-- Partition an event into two based on a predicate.
    Returns (matching, nonMatching) where matching fires when predicate is true.
    Alias for splitEM with Haskell-style naming. -/
abbrev partitionEM := @splitEM

/-- Partition an event into two based on a predicate (fluent style).
    Enables: `event.partitionE' predicate`
    Alias for splitE' with Haskell-style naming. -/
abbrev partitionE' := @splitE'

/-- Delay an Event by one propagation frame, auto-allocating NodeId and registering with scope.
    Useful for breaking dependency cycles. -/
def delayFrameM (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    match ← getPropagationContext with
    | some queue =>
      let action := derived.fire a
      let pending : PendingFire := ⟨derived.height, nodeId, action⟩
      queue.insertNextFrame pending
    | none => derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Take at most n occurrences from an Event, auto-allocating NodeId and registering with scope. -/
def takeNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    if count < n then
      countRef.set (count + 1)
      derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Take only the first occurrence of an event.
    Specialization of `takeNM 1` for convenience. -/
abbrev onceM (e : Event Spider a) : SpiderM (Event Spider a) :=
  takeNM 1 e

/-- Take only the first occurrence of an event (fluent style).
    Enables: `event.once'` -/
abbrev once' (e : Event Spider a) : SpiderM (Event Spider a) :=
  onceM e

/-- Drop the first n occurrences from an Event, auto-allocating NodeId and registering with scope. -/
def dropNM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let countRef ← IO.mkRef 0
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let count ← countRef.get
    countRef.set (count + 1)
    if count >= n then derived.fire a
  env.currentScope.register unsub
  pure derived⟩

/-- Accumulate values over an Event, auto-allocating NodeId and registering with scope.
    Like foldDyn but returns an Event instead of a Dynamic. -/
def accumulateM (f : a → b → b) (initial : b) (e : Event Spider a)
    : SpiderM (Event Spider b) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let stateRef ← IO.mkRef initial
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let old ← stateRef.get
    let new := f a old
    stateRef.set new
    derived.fire new
  env.currentScope.register unsub
  pure derived⟩

/-- Alias for accumulateM (familiar name from other FRP libraries). -/
abbrev scanM := @accumulateM

/-- Emit (previous, current) pairs on each event occurrence.
    Skips the first occurrence since there's no previous value.
    Useful for detecting changes and computing deltas. -/
def withPreviousM (e : Event Spider a) : SpiderM (Event Spider (a × a)) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    prevRef.set (some curr)
    match prev with
    | some p => derived.fire (p, curr)
    | none => pure ()
  env.currentScope.register unsub
  pure derived⟩

/-- Emit (previous, current) pairs on each event occurrence (fluent style).
    Enables: `event.withPrevious'` -/
def withPrevious' (e : Event Spider a) : SpiderM (Event Spider (a × a)) :=
  withPreviousM e

/-- Skip consecutive duplicate values.
    Requires BEq to compare values. Only fires when value differs from previous. -/
def distinctM [BEq a] (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let prevRef ← IO.mkRef (none : Option a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun curr => do
    let prev ← prevRef.get
    let shouldFire := match prev with
      | none => true
      | some p => curr != p
    if shouldFire then
      prevRef.set (some curr)
      derived.fire curr
  env.currentScope.register unsub
  pure derived⟩

/-- Skip consecutive duplicate values (fluent style).
    Enables: `event.distinct'` -/
def distinct' [BEq a] (e : Event Spider a) : SpiderM (Event Spider a) :=
  distinctM e

/-- Alias for distinctM (familiar name from RxJS/reactive libraries). -/
abbrev dedupeM := @distinctM

/-- Alias for distinct' (familiar name from RxJS/reactive libraries). -/
abbrev dedupe' := @distinct'

/-- Collect n events before emitting them as a batch.
    Useful for batch processing patterns. -/
def bufferM (n : Nat) (e : Event Spider a) : SpiderM (Event Spider (Array a)) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let bufRef ← IO.mkRef (#[] : Array a)
  let derived ← Event.newNodeWithId nodeId (e.height.inc)
  let unsub ← Reactive.Event.subscribe e fun a => do
    let buf ← bufRef.get
    let newBuf := buf.push a
    if newBuf.size >= n then
      bufRef.set #[]
      derived.fire newBuf
    else
      bufRef.set newBuf
  env.currentScope.register unsub
  pure derived⟩

/-- Collect n events before emitting them as a batch (fluent style).
    Enables: `event.buffer' 5` -/
def buffer' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider (Array a)) :=
  bufferM n e

/-- Combine two Events that fire simultaneously, auto-allocating NodeId
    and registering with scope.
    If both events fire in the same frame, fires once with paired values.
    If only one fires, nothing is emitted for that occurrence. -/
def zipEM (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider (a × b)) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.zipEM"
  let nodeId ← env.timelineCtx.freshNodeId
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

  let unsub1 ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush
  let unsub2 ← Reactive.Event.subscribe e2 fun b => do
    value2Ref.set (some b)
    scheduleFlush

  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Combine two Events that fire simultaneously (fluent style).
    Enables: `event1.zipE' event2` -/
def zipE' (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider (a × b)) :=
  zipEM e1 e2

/-- Fire when e1 occurs but e2 does not (in the same frame).
    Auto-allocates NodeId and registers subscriptions with scope. -/
def differenceM (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.differenceM"
  let nodeId ← env.timelineCtx.freshNodeId
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

  let unsub1 ← Reactive.Event.subscribe e1 fun a => do
    value1Ref.set (some a)
    scheduleFlush
  let unsub2 ← Reactive.Event.subscribe e2 fun _ => do
    value2FiredRef.set true
    scheduleFlush

  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure derived⟩

/-- Fire when e1 occurs but e2 does not (fluent style).
    Enables: `event1.difference' event2` -/
def difference' (e1 : Event Spider a) (e2 : Event Spider b)
    : SpiderM (Event Spider a) :=
  differenceM e1 e2

/-- Switch events based on a Dynamic selector.
    Fires from whichever event the Dynamic currently holds.
    All subscriptions are registered with the current scope. -/
def switchDynM (de : Dynamic Spider (Event Spider a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Event.switchDynM"
  let nodeId ← env.timelineCtx.freshNodeId
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  let initialEvent ← de.sample
  let derivedHeight := Height.inc (max initialEvent.height de.updated.height)
  let derived ← Event.newNodeWithId nodeId derivedHeight
  let unsub ← Reactive.Event.subscribe initialEvent derived.fire
  currentUnsubRef.set unsub

  let unsubOuter ← Reactive.Event.subscribe de.updated fun newEvent => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe newEvent derived.fire
    currentUnsubRef.set unsub

  env.currentScope.register unsubOuter
  env.currentScope.register do
    let unsub ← currentUnsubRef.get
    unsub

  env.decrementDepth
  pure derived⟩

/-- Switch events based on a Dynamic selector (fluent style).
    Enables: `dynEvent.switchDyn'` -/
def switchDyn' (de : Dynamic Spider (Event Spider a)) : SpiderM (Event Spider a) :=
  switchDynM de

end Event

/-! ## Map Fusion Macros

These macros rewrite common add-constant map patterns to the fused
`Event.mapAddConstM` combinator. This avoids deep chains of `x + c`
calls by collapsing constants at construction time.

Note: Fusion assumes `x + c1 + c2` is equivalent to `x + (c1 + c2)`.
-/

set_option linter.unusedVariables false in
macro_rules
| `(Event.mapM (· + $c) $e) => `(Event.mapAddConstM $c $e)
| `(Event.mapM (fun $x => $x + $c) $e) => `(Event.mapAddConstM $c $e)
| `(Event.mapM Nat.succ $e) => `(Event.mapAddConstM 1 $e)
| `(Reactive.Host.Event.mapM (· + $c) $e) => `(Reactive.Host.Event.mapAddConstM $c $e)
| `(Reactive.Host.Event.mapM (fun $x => $x + $c) $e) => `(Reactive.Host.Event.mapAddConstM $c $e)
| `(Reactive.Host.Event.mapM Nat.succ $e) => `(Reactive.Host.Event.mapAddConstM 1 $e)

/-! ## Temporal Combinators

Time-based event combinators for delay, debounce, and throttle.
These use async tasks with IO.sleep for timing. -/

namespace Event

/-- Delay event firings by a specified duration.
    Each firing is independently delayed - if the source fires
    at times t1, t2, the derived fires at t1+d, t2+d.

    Uses an async task with IO.sleep. The delayed fire happens
    in a new propagation frame. Subscription is registered with current scope. -/
def delayDurationM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let unsub ← Reactive.Event.subscribe e fun a => do
      let _ ← IO.asTask (prio := .dedicated) do
        let ms := d.toMilliseconds.toNat
        IO.sleep (UInt32.ofNat ms)
        env.withFrame (derived.fire a)
    env.currentScope.register unsub
    pure derived⟩

/-- Debounce: only fire after the source has been quiet for the specified duration.

    If the source fires rapidly (t1, t2, t3 where gaps < d), only one fire
    occurs at t3+d with the value from t3.

    Useful for text input where you want to wait until the user stops typing.
    Subscription is registered with current scope. -/
def debounceM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let generationRef ← IO.mkRef (0 : Nat)
    let pendingValueRef ← IO.mkRef (none : Option a)

    let unsub ← Reactive.Event.subscribe e fun a => do
      -- Increment generation to "cancel" any pending fires
      let gen ← generationRef.modifyGet fun g => (g + 1, g + 1)
      pendingValueRef.set (some a)

      -- Spawn async task that fires after quiet period
      let _ ← IO.asTask (prio := .dedicated) do
        let ms := d.toMilliseconds.toNat
        IO.sleep (UInt32.ofNat ms)
        -- Check if still the latest generation
        let currentGen ← generationRef.get
        if gen == currentGen then
          match ← pendingValueRef.get with
          | some v => env.withFrame (derived.fire v)
          | none => pure ()
    env.currentScope.register unsub
    pure derived⟩

/-- Throttle: rate limit to at most one fire per interval.

    @param leading If true (default), fire immediately on first occurrence in interval
    @param trailing If true (default), fire at end of interval if events occurred during cooldown
    Subscription is registered with current scope. -/
def throttleM (d : Chronos.Duration) (e : Event Spider a)
    (leading : Bool := true) (trailing : Bool := true) : SpiderM (Event Spider a) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let lastFireTimeRef ← IO.mkRef (none : Option Nat)  -- milliseconds since start
    let trailingValueRef ← IO.mkRef (none : Option a)
    let trailingScheduledRef ← IO.mkRef false
    let startTime ← IO.monoMsNow

    let unsub ← Reactive.Event.subscribe e fun a => do
      let now ← IO.monoMsNow
      let elapsed := now - startTime
      let lastFire ← lastFireTimeRef.get
      let intervalMs := d.toMilliseconds.toNat

      let shouldFireNow := match lastFire with
        | none => leading
        | some t => (elapsed - t >= intervalMs) && leading

      if shouldFireNow then
        lastFireTimeRef.set (some elapsed)
        derived.fire a
      else if trailing then
        trailingValueRef.set (some a)
        let alreadyScheduled ← trailingScheduledRef.get
        if !alreadyScheduled then
          trailingScheduledRef.set true
          let _ ← IO.asTask (prio := .dedicated) do
            let ms := intervalMs
            IO.sleep (UInt32.ofNat ms)
            trailingScheduledRef.set false
            match ← trailingValueRef.get with
            | some v =>
              trailingValueRef.set none
              let now' ← IO.monoMsNow
              lastFireTimeRef.set (some (now' - startTime))
              env.withFrame (derived.fire v)
            | none => pure ()
    env.currentScope.register unsub
    pure derived⟩

/-- Collect events within a time window and emit as batch.
    Uses tumbling windows: collects events for the duration, emits, then resets.
    The window starts when the first event arrives.
    Subscription is registered with current scope. -/
def windowM (d : Chronos.Duration) (e : Event Spider a) : SpiderM (Event Spider (Array a)) :=
  ⟨fun env => do
    let nodeId ← env.timelineCtx.freshNodeId
    let derived ← Event.newNodeWithId nodeId (e.height.inc)
    let bufferRef ← IO.mkRef (#[] : Array a)
    let windowActiveRef ← IO.mkRef false
    let generationRef ← IO.mkRef (0 : Nat)

    let unsub ← Reactive.Event.subscribe e fun a => do
      -- Add to buffer
      bufferRef.modify (·.push a)

      -- Start window timer if not already active
      let wasActive ← windowActiveRef.modifyGet fun active => (active, true)
      if !wasActive then
        let gen ← generationRef.modifyGet fun g => (g + 1, g + 1)
        -- Spawn timer that fires after window duration
        let _ ← IO.asTask (prio := .dedicated) do
          let ms := d.toMilliseconds.toNat
          IO.sleep (UInt32.ofNat ms)
          -- Check generation to ensure we haven't been disposed
          let currentGen ← generationRef.get
          if gen == currentGen then
            -- Collect and emit buffer
            let batch ← bufferRef.modifyGet fun buf => (buf, #[])
            windowActiveRef.set false
            if batch.size > 0 then
              env.withFrame (derived.fire batch)

    env.currentScope.register unsub
    pure derived⟩

/-! ### Fluent Chainable Combinators

Extension methods enabling dot-notation chaining:
```lean
event.map' f |>.filter' p |>.gate' behavior
-- or with bind
event.map' f >>= (·.filter' p)
```

These are wrappers around the standard combinators with flipped argument order. -/

/-- Map a function over an Event (fluent style).
    Enables: `event.map' f` instead of `Event.mapM f event` -/
def map' (e : Event Spider a) (f : a → b) : SpiderM (Event Spider b) :=
  mapM f e

/-- Filter an Event by a predicate (fluent style).
    Enables: `event.filter' p` -/
def filter' (e : Event Spider a) (p : a → Bool) : SpiderM (Event Spider a) :=
  filterM p e

/-- Filter and map an Event (fluent style).
    Enables: `event.mapMaybe' f` -/
def mapMaybe' (e : Event Spider a) (f : a → Option b) : SpiderM (Event Spider b) :=
  mapMaybeM f e

/-- Map to a constant value (fluent style).
    Enables: `event.mapConst' value` -/
def mapConst' (e : Event Spider α) (b : β) : SpiderM (Event Spider β) :=
  mapConstM b e

/-- Merge with another Event with left-bias (fluent style).
    Enables: `event1.merge' event2` -/
def merge' (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) :=
  mergeM e1 e2

/-- Merge with another Event, firing all values (fluent style).
    Enables: `event1.mergeAll' event2` -/
def mergeAll' (e1 : Event Spider a) (e2 : Event Spider a) : SpiderM (Event Spider a) :=
  mergeAllM e1 e2

/-- Tag with a Behavior's value (fluent style).
    Enables: `event.tag' behavior` -/
def tag' (e : Event Spider b) (beh : Behavior Spider a) : SpiderM (Event Spider a) :=
  tagM beh e

/-- Sample a behavior at event occurrence (fluent style, alias for tag'). -/
abbrev sample' := @tag'

/-- Attach a Behavior's value (fluent style).
    Enables: `event.attach' behavior` -/
def attach' (e : Event Spider c) (b : Behavior Spider a) : SpiderM (Event Spider (a × c)) :=
  attachM b e

/-- Snapshot a behavior at event occurrence (fluent style, alias for attach'). -/
abbrev snapshot' := @attach'

/-- Attach with a combining function (fluent style).
    Enables: `event.attachWith' f behavior` -/
def attachWith' (e : Event Spider c) (f : a → c → d) (b : Behavior Spider a)
    : SpiderM (Event Spider d) :=
  attachWithM f b e

/-- Gate by a Boolean Behavior (fluent style).
    Enables: `event.gate' boolBehavior` -/
def gate' (e : Event Spider a) (beh : Behavior Spider Bool) : SpiderM (Event Spider a) :=
  gateM beh e

/-- Take first n occurrences (fluent style).
    Enables: `event.take' 5` -/
def take' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider a) :=
  takeNM n e

/-- Drop first n occurrences (fluent style).
    Enables: `event.drop' 5` -/
def drop' (e : Event Spider a) (n : Nat) : SpiderM (Event Spider a) :=
  dropNM n e

/-- Accumulate values (fluent style).
    Enables: `event.scan' f initial` -/
def scan' (e : Event Spider a) (f : a → b → b) (initial : b) : SpiderM (Event Spider b) :=
  accumulateM f initial e

/-- Delay by one frame (fluent style).
    Enables: `event.delayFrame'` -/
def delayFrame' (e : Event Spider a) : SpiderM (Event Spider a) :=
  delayFrameM e

/-- Delay by duration (fluent style).
    Enables: `event.delay' duration` -/
def delay' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider a) :=
  delayDurationM d e

/-- Debounce (fluent style).
    Enables: `event.debounce' duration` -/
def debounce' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider a) :=
  debounceM d e

/-- Throttle (fluent style).
    Enables: `event.throttle' duration` -/
def throttle' (e : Event Spider a) (d : Chronos.Duration)
    (leading : Bool := true) (trailing : Bool := true) : SpiderM (Event Spider a) :=
  throttleM d e leading trailing

/-- Collect events within a time window and emit as batch (fluent style).
    Enables: `event.window' duration` -/
def window' (e : Event Spider a) (d : Chronos.Duration) : SpiderM (Event Spider (Array a)) :=
  windowM d e

/-- Split Sum into two events (fluent style).
    Enables: `event.fanEither'` -/
def fanEither' (e : Event Spider (Sum a b)) : SpiderM (Event Spider a × Event Spider b) :=
  fanEitherM e

/-! ### Debugging Combinators -/

/-- Debug logging for events. Prints each event occurrence with a label.
    Useful for debugging reactive networks.

    Example:
    ```
    let debuggedClicks ← Event.traceM "click" clickEvent
    -- Prints: [click] <value> for each occurrence
    ``` -/
def traceM (label : String) (e : Event Spider a) [ToString a] : SpiderM (Event Spider a) := ⟨fun env => do
  let unsub ← Reactive.Event.subscribe e fun x =>
    IO.println s!"[{label}] {x}"
  env.currentScope.register unsub
  pure e⟩

/-- Debug logging with custom formatter.

    Example:
    ```
    let debugged ← Event.traceWithM "user" (fun u => u.name) userEvent
    ``` -/
def traceWithM (label : String) (f : a → String) (e : Event Spider a) : SpiderM (Event Spider a) := ⟨fun env => do
  let unsub ← Reactive.Event.subscribe e fun x =>
    IO.println s!"[{label}] {f x}"
  env.currentScope.register unsub
  pure e⟩

/-- Trace event occurrences (fluent style).
    Enables: `event.trace' "label"` -/
def trace' (e : Event Spider a) (label : String) [ToString a] : SpiderM (Event Spider a) :=
  traceM label e

/-- Trace with custom formatter (fluent style).
    Enables: `event.traceWith' "label" formatter` -/
def traceWith' (e : Event Spider a) (label : String) (f : a → String) : SpiderM (Event Spider a) :=
  traceWithM label f e

end Event

end Reactive.Host
