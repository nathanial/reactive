/-
  Reactive/Core/Event.lean

  Event type representing discrete occurrences over time.
  Events are push-based with subscriber management.
-/
import Reactive.Core.Types
import Reactive.Core.Scope

namespace Reactive

/-! ## Global Propagation Context

The propagation context enables frame-based event handling. When set, events
are queued by height instead of firing immediately, preventing glitches. -/

/-- Global propagation context for frame-based firing.
    When `some`, events enqueue to the referenced queue instead of firing immediately.
    Set by SpiderEnv when entering a frame, read by EventNode.fire. -/
private initialize globalPropagationQueue : IO.Ref (Option PropagationQueue) ←
  IO.mkRef none

/-- Set the global propagation context for frame-based firing -/
def setPropagationContext (queue : PropagationQueue) : IO Unit :=
  globalPropagationQueue.set (some queue)

/-- Clear the global propagation context -/
def clearPropagationContext : IO Unit :=
  globalPropagationQueue.set none

/-- Get the current propagation context -/
def getPropagationContext : IO (Option PropagationQueue) :=
  globalPropagationQueue.get

/-- A subscriber callback that receives event values.

    Subscribers are called synchronously when an event fires.
    Within a propagation frame, subscribers may be called in any order
    (except that lower-height events always fire before higher-height ones). -/
abbrev Subscriber (a : Type) := a → IO Unit

/-- Internal representation of an event with subscriber management.
    Push-based: when fired, all subscribers are notified.

    Uses lazy deletion for O(1) unsubscribe: callbacks are set to `none`
    instead of filtering the array. Dead entries are skipped during fire
    and compacted when the dead count exceeds a threshold. -/
structure EventNode (a : Type) where
  /-- Unique identifier for this node -/
  nodeId : NodeId
  /-- Height in dependency graph for topological ordering -/
  height : Height
  /-- Registered subscribers (Option for lazy deletion) -/
  subscribers : IO.Ref (Array (SubscriberId × Option (Subscriber a)))
  /-- Number of active (non-deleted) subscribers -/
  activeCount : IO.Ref Nat
  /-- Map-chain connection for fusion (connects to upstream with composed map) -/
  mapConnect : IO.Ref (Option (Subscriber a → IO (IO Unit)))
  /-- Base connection for map-add fusion (connects upstream before add-const). -/
  mapAddBaseConnect : IO.Ref (Option (Subscriber a → IO (IO Unit)))
  /-- Accumulated add-constant value for map-add fusion. -/
  mapAddConst : IO.Ref (Option a)
  /-- Active upstream subscription when this map node is connected -/
  upstreamUnsub : IO.Ref (Option (IO Unit))
  /-- Counter for generating unique subscriber IDs -/
  nextSubId : IO.Ref Nat

namespace EventNode

/-- Create a new event node -/
def new (nodeId : NodeId) (height : Height := ⟨0⟩) : IO (EventNode a) := do
  let subs ← IO.mkRef #[]
  let activeCount ← IO.mkRef 0
  let mapConnect ← IO.mkRef none
  let mapAddBaseConnect ← IO.mkRef none
  let mapAddConst ← IO.mkRef none
  let upstreamUnsub ← IO.mkRef none
  let nextId ← IO.mkRef 0
  pure {
    nodeId,
    height,
    subscribers := subs,
    activeCount := activeCount,
    mapConnect := mapConnect,
    mapAddBaseConnect := mapAddBaseConnect,
    mapAddConst := mapAddConst,
    upstreamUnsub := upstreamUnsub,
    nextSubId := nextId
  }

/-- Fire this event with a value, notifying all subscribers.
    If a propagation context is active and we're in a frame, the fire is
    enqueued for height-ordered processing. Otherwise fires immediately. -/
def fire (node : EventNode a) (value : a) : IO Unit := do
  match ← getPropagationContext with
  | none =>
    -- No propagation context, fire immediately
    fireImmediate node value
  | some queue =>
    let inFrame ← queue.isInFrame
    if inFrame then
      -- We're in a frame, enqueue for height-ordered processing
      let action : IO Unit := fireImmediate node value
      let pending : PendingFire := ⟨node.height, node.nodeId, action⟩
      queue.insert pending
    else
      -- Context exists but not in frame, fire immediately
      fireImmediate node value
where
  /-- Fire immediately without queueing - calls all active subscribers -/
  @[inline] fireImmediate (node : EventNode a) (value : a) : IO Unit := do
    let subs ← node.subscribers.get
    -- Skip deleted (none) entries
    for (_, callback?) in subs do
      if let some callback := callback? then
        callback value

/-- Ensure a map-only node is connected to its root when it gains subscribers. -/
def ensureConnected (node : EventNode a) : IO Unit := do
  if (← node.upstreamUnsub.get).isSome then
    pure ()
  else
    match ← node.mapConnect.get with
    | none => pure ()
    | some connect => do
      let unsub ← connect node.fire
      node.upstreamUnsub.set (some unsub)

/-- Disconnect a map-only node from its root when it has no active subscribers. -/
def maybeDisconnect (node : EventNode a) : IO Unit := do
  let count ← node.activeCount.get
  if count == 0 then
    match ← node.upstreamUnsub.get with
    | none => pure ()
    | some unsub =>
      unsub
      node.upstreamUnsub.set none
    -- Clear dead entries when fully disconnected
    node.subscribers.set #[]

/-- Subscribe to this event, returning an unsubscribe action.
    Unsubscribe is O(1) using lazy deletion - the callback is set to `none`
    instead of filtering the array. -/
def subscribe (node : EventNode a) (callback : Subscriber a) : IO (IO Unit) := do
  let wasEmpty ← node.activeCount.modifyGet fun n => (n == 0, n + 1)
  let subId ← node.nextSubId.modifyGet fun n => (⟨n⟩, n + 1)
  let idx := (← node.subscribers.get).size
  node.subscribers.modify (·.push (subId, some callback))
  if wasEmpty then
    ensureConnected node
  -- Return O(1) unsubscribe action using lazy deletion
  pure do
    -- Mark as deleted by setting to none (O(1) array update)
    node.subscribers.modify fun subs =>
      if h : idx < subs.size then
        subs.set idx (subId, none)
      else
        subs  -- Index out of bounds (shouldn't happen)
    node.activeCount.modify (· - 1)
    maybeDisconnect node

/-- Get the number of active subscribers -/
def subscriberCount (node : EventNode a) : IO Nat :=
  node.activeCount.get

/-- Compact the subscriber array by removing deleted entries.
    Called automatically when disconnecting, but can be called manually
    for long-lived nodes with high subscription churn. -/
def compact (node : EventNode a) : IO Unit := do
  let subs ← node.subscribers.get
  let active := subs.filter (·.2.isSome)
  node.subscribers.set active

end EventNode

/-- An Event represents discrete occurrences of values over time.
    Events are parameterized by:
    - `t`: The timeline (phantom type for type-safe separation)
    - `a`: The type of values carried by the event

    Conceptually an Event is like `[(Time, a)]` but implemented efficiently
    as a push-based subscriber system. -/
structure Event (t : Type) (a : Type) where
  private mk ::
  private node : EventNode a

namespace Event

/-- Create a new event node (internal use).
    Requires a TimelineCtx for type-safe timeline separation. -/
protected def newNode [Timeline t] (ctx : TimelineCtx t) (height : Height := ⟨0⟩) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  let node ← EventNode.new nodeId height
  pure ⟨node⟩

/-- Create a new event node with explicit NodeId (internal use).
    Prefer `newNode` which auto-allocates NodeIds. -/
protected def newNodeWithId [Timeline t] (nodeId : NodeId) (height : Height := ⟨0⟩) : IO (Event t a) := do
  let node ← EventNode.new nodeId height
  pure ⟨node⟩

/-- The event that never fires.

    Useful as a placeholder or for constant dynamics.
    Requires a TimelineCtx for type-safe timeline separation.

    Example:
    ```
    -- Within SpiderM:
    let neverFires ← Event.neverM (t := Spider)
    -- This event will never fire, so subscribers are never called
    ``` -/
def never [Timeline t] (ctx : TimelineCtx t) : IO (Event t a) := do
  Event.newNode ctx

/-- Create a new triggerable event.
    Returns the event and a function to fire it.
    Requires a TimelineCtx for type-safe timeline separation.

    This is the primary way to create events that can be fired from external code.
    The returned trigger function fires the event when called.

    Example:
    ```
    -- Within SpiderM, use newTriggerEvent instead:
    let (clickEvent, fireClick) ← newTriggerEvent (t := Spider) (a := Unit)
    -- Later, to fire the event:
    fireClick ()
    ``` -/
def newTrigger [Timeline t] (ctx : TimelineCtx t) : IO (Event t a × (a → IO Unit)) := do
  let nodeId ← ctx.freshNodeId
  let node ← EventNode.new nodeId
  pure (⟨node⟩, node.fire)

/-- Create a new triggerable event with explicit NodeId (internal use).
    Prefer the SpiderM `newTriggerEvent` which handles context automatically. -/
def newTriggerWithId [Timeline t] (nodeId : NodeId) : IO (Event t a × (a → IO Unit)) := do
  let node ← EventNode.new nodeId
  pure (⟨node⟩, node.fire)

/-- Subscribe to an event.
    Returns an unsubscribe action that removes this subscription.

    WARNING: This is protected for internal use by combinators. Application code
    should use pure FRP combinators like `foldDyn`, `holdDyn`, `mapM`, `filterM`,
    `attachWith`, etc. instead of manual subscriptions. Manual subscribe can lead
    to imperative patterns that break FRP semantics.

    Example:
    ```
    let unsub ← myEvent.subscribe fun value =>
      IO.println s!"Received: {value}"
    -- Later, to stop receiving events:
    unsub
    ``` -/
protected def subscribe (e : Event t a) (callback : Subscriber a) : IO (IO Unit) :=
  e.node.subscribe callback

/-- Subscribe with scope-based cleanup.
    The subscription is automatically unsubscribed when the scope is disposed.

    WARNING: This is protected for internal use by combinators. Application code
    should use pure FRP combinators like `foldDyn`, `holdDyn`, `mapM`, `filterM`,
    `attachWith`, etc. instead of manual subscriptions. Manual subscribe can lead
    to imperative patterns that break FRP semantics.

    Prefer this over `subscribe` when working within a `SpiderM` context,
    as it ensures proper cleanup of subscriptions. -/
protected def subscribeScoped (e : Event t a) (scope : SubscriptionScope)
    (callback : Subscriber a) : IO (IO Unit) := do
  let unsub ← e.node.subscribe callback
  scope.register unsub
  pure unsub

/-- Fire an event (internal use - normally done via trigger) -/
protected def fire (e : Event t a) (value : a) : IO Unit :=
  e.node.fire value

/-- Get the height of this event in the dependency graph -/
def height (e : Event t a) : Height :=
  e.node.height

/-- Get the node ID -/
def nodeId (e : Event t a) : NodeId :=
  e.node.nodeId

/-- Helper to create a derived event from a source event (with explicit NodeId).
    Creates a new event at source.height + 1 and subscribes with the given handler.
    The handler receives the source value and the derived event's fire function. -/
private def deriveWithId [Timeline t] (source : Event t a) (derivedNodeId : NodeId)
    (handler : a → (b → IO Unit) → IO Unit) : IO (Event t b) := do
  let derived ← Event.newNodeWithId derivedNodeId (source.height.inc)
  let _ ← Reactive.Event.subscribe source fun a => handler a derived.fire
  pure derived

/-- Helper to create a derived event from a source event.
    Creates a new event at source.height + 1 and subscribes with the given handler.
    The handler receives the source value and the derived event's fire function. -/
private def deriveWith [Timeline t] (ctx : TimelineCtx t) (source : Event t a)
    (handler : a → (b → IO Unit) → IO Unit) : IO (Event t b) := do
  let nodeId ← ctx.freshNodeId
  deriveWithId source nodeId handler

/-- Map a function over event values (with explicit NodeId).
    Creates a new derived event that transforms values from the source. -/
def mapWithId [Timeline t] (f : a → b) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t b) :=
  do
    let derived ← Event.newNodeWithId derivedNodeId (source.height.inc)
    let sourceConnect ← source.node.mapConnect.get
    let composed : (Subscriber b → IO (IO Unit)) :=
      match sourceConnect with
      | some connect => fun cb => connect (fun a => cb (f a))
      | none => fun cb => source.node.subscribe fun a => cb (f a)
    derived.node.mapConnect.set (some composed)
    pure derived

/-- Map a constant addition over event values, with fusion across successive add-const maps.
    This collapses chains of `x + c` into a single add at the endpoint.
    Requires homogeneous addition. -/
def mapAddConstWithId [Timeline t] [HAdd a a a]
    (c : a) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNodeWithId derivedNodeId (source.height.inc)

  -- Prefer base connect if available to avoid stacking add-const wrappers.
  let baseConnect? ← source.node.mapAddBaseConnect.get
  let baseConnect ←
    match baseConnect? with
    | some connect => pure connect
    | none =>
      match ← source.node.mapConnect.get with
      | some connect => pure connect
      | none => pure (fun cb => source.node.subscribe cb)

  let totalConst ←
    match ← source.node.mapAddConst.get with
    | some existing => pure (existing + c)
    | none => pure c

  derived.node.mapAddBaseConnect.set (some baseConnect)
  derived.node.mapAddConst.set (some totalConst)

  let composed : (Subscriber a → IO (IO Unit)) :=
    fun cb => baseConnect (fun a => cb (a + totalConst))
  derived.node.mapConnect.set (some composed)
  pure derived

/-- Map a function over event values.
    Creates a new derived event that transforms values from the source.

    Example:
    ```
    -- Within SpiderM, prefer Event.mapM:
    let doubled ← Event.mapM (· * 2) numberEvent
    -- When numberEvent fires 5, doubled fires 10
    ``` -/
def map [Timeline t] (ctx : TimelineCtx t) (f : a → b) (source : Event t a) : IO (Event t b) :=
  do
    let nodeId ← ctx.freshNodeId
    mapWithId f source nodeId

/-- Map a constant addition over event values, with fusion across successive add-const maps. -/
def mapAddConst [Timeline t] [HAdd a a a] (ctx : TimelineCtx t)
    (c : a) (source : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  mapAddConstWithId c source nodeId

/-- Filter event occurrences by a predicate (with explicit NodeId).
    Only values that satisfy the predicate pass through. -/
def filterWithId [Timeline t] (p : a → Bool) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t a) :=
  deriveWithId source derivedNodeId fun a fire =>
    if p a then fire a else pure ()

/-- Filter event occurrences by a predicate.
    Only values that satisfy the predicate pass through.

    Example:
    ```
    -- Within SpiderM, prefer Event.filterM:
    let positives ← Event.filterM (· > 0) numberEvent
    -- When numberEvent fires -5, 3, 0, 7: positives fires 3, 7
    ``` -/
def filter [Timeline t] (ctx : TimelineCtx t) (p : a → Bool) (source : Event t a) : IO (Event t a) :=
  deriveWith ctx source fun a fire =>
    if p a then fire a else pure ()

/-- Filter and map simultaneously (with explicit NodeId).
    Only `some` results pass through; `none` results are dropped. -/
def mapMaybeWithId [Timeline t] (f : a → Option b) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t b) :=
  deriveWithId source derivedNodeId fun a fire =>
    match f a with
    | some b => fire b
    | none => pure ()

/-- Filter and map simultaneously.
    Only `some` results pass through; `none` results are dropped.

    Example:
    ```
    -- Within SpiderM, prefer Event.mapMaybeM:
    let parsed ← Event.mapMaybeM String.toNat? stringEvent
    -- When stringEvent fires "42", "hello", "7": parsed fires 42, 7
    ``` -/
def mapMaybe [Timeline t] (ctx : TimelineCtx t) (f : a → Option b) (source : Event t a) : IO (Event t b) :=
  deriveWith ctx source fun a fire =>
    match f a with
    | some b => fire b
    | none => pure ()

/-- Merge two events into one with left-bias (with explicit NodeId).
    When either fires, the merged event fires with that value.
    When both fire simultaneously (same frame), only the left event's value
    is delivered (Reflex-style semantics). For all-fire behavior, use `mergeAllWithId`. -/
def mergeWithId [Timeline t] (e1 : Event t a) (e2 : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId derivedNodeId height

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
            queue.insert ⟨derived.height, derivedNodeId, resetAction⟩
          else resetAction
        | none => resetAction
      derived.fire value

  -- e1 (left) fires first due to subscription order
  let _ ← Reactive.Event.subscribe e1 tryFire
  let _ ← Reactive.Event.subscribe e2 tryFire
  pure derived

/-- Merge two events into one with left-bias.
    When either fires, the merged event fires with that value.
    When both fire simultaneously (same frame), only the left event's value
    is delivered (Reflex-style semantics). For all-fire behavior, use `mergeAll`.

    Example:
    ```
    -- Within SpiderM, prefer Event.mergeM:
    let combined ← Event.mergeM clickEvent keyEvent
    -- Fires when either click or key fires
    ``` -/
def merge [Timeline t] (ctx : TimelineCtx t) (e1 : Event t a) (e2 : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  mergeWithId e1 e2 nodeId

/-- Merge two events into one, firing all values (with explicit NodeId).
    When both fire simultaneously (same frame), both values are delivered.
    This preserves the pre-Reflex behavior. For left-bias semantics, use `mergeWithId`. -/
def mergeAllWithId [Timeline t] (e1 : Event t a) (e2 : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId derivedNodeId height
  let _ ← Reactive.Event.subscribe e1 derived.fire
  let _ ← Reactive.Event.subscribe e2 derived.fire
  pure derived

/-- Merge two events into one, firing all values.
    When both fire simultaneously (same frame), both values are delivered.
    This preserves the pre-Reflex behavior. For left-bias semantics, use `merge`. -/
def mergeAll [Timeline t] (ctx : TimelineCtx t) (e1 : Event t a) (e2 : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  mergeAllWithId e1 e2 nodeId

end Event

end Reactive
