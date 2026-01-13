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
    Push-based: when fired, all subscribers are notified. -/
structure EventNode (a : Type) where
  /-- Unique identifier for this node -/
  nodeId : NodeId
  /-- Height in dependency graph for topological ordering -/
  height : Height
  /-- Registered subscribers (generic callbacks) -/
  subscribers : IO.Ref (Array (SubscriberId × Subscriber a))
  /-- Fast-path map subscribers (used for map chain fusion) -/
  mapSubscribers : IO.Ref (Array (SubscriberId × Subscriber a))
  /-- Counter for generating unique subscriber IDs -/
  nextSubId : IO.Ref Nat

namespace EventNode

/-- Create a new event node -/
def new (nodeId : NodeId) (height : Height := ⟨0⟩) : IO (EventNode a) := do
  let subs ← IO.mkRef #[]
  let mapSubs ← IO.mkRef #[]
  let nextId ← IO.mkRef 0
  pure { nodeId, height, subscribers := subs, mapSubscribers := mapSubs, nextSubId := nextId }

/-- Subscribe to this event, returning an unsubscribe action -/
def subscribe (node : EventNode a) (callback : Subscriber a) : IO (IO Unit) := do
  let subId ← node.nextSubId.modifyGet fun n => (⟨n⟩, n + 1)
  node.subscribers.modify (·.push (subId, callback))
  pure do
    node.subscribers.modify fun subs =>
      subs.filter (·.1 != subId)

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
  /-- Fire immediately without queueing - calls all subscribers -/
  @[inline] fireImmediate (node : EventNode a) (value : a) : IO Unit := do
    let subs ← node.subscribers.get
    for (_, callback) in subs do
      callback value
    let mapSubs ← node.mapSubscribers.get
    for (_, callback) in mapSubs do
      callback value

/-- Propagate a mapped value to a downstream node, skipping map-only nodes. -/
def propagateMapChain (node : EventNode a) (value : a) : IO Unit := do
  let subs ← node.subscribers.get
  if subs.isEmpty then
    let mapSubs ← node.mapSubscribers.get
    if mapSubs.isEmpty then
      pure ()
    else
      for (_, callback) in mapSubs do
        callback value
  else
    EventNode.fire node value

/-- Subscribe with a pure map into a downstream node.
    This enables fast-path fusion for map-only chains. -/
def subscribeMap (node : EventNode a) (target : EventNode b) (f : a → b) : IO (IO Unit) := do
  let subId ← node.nextSubId.modifyGet fun n => (⟨n⟩, n + 1)
  let callback : Subscriber a := fun a => propagateMapChain target (f a)
  node.mapSubscribers.modify (·.push (subId, callback))
  pure do
    node.mapSubscribers.modify fun subs =>
      subs.filter (·.1 != subId)

/-- Get the number of subscribers -/
def subscriberCount (node : EventNode a) : IO Nat := do
  let subs ← node.subscribers.get
  let mapSubs ← node.mapSubscribers.get
  pure (subs.size + mapSubs.size)

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

/-- Subscribe with a pure map into a downstream event (fast-path for map chains). -/
protected def subscribeMap (source : Event t a) (target : Event t b) (f : a → b) : IO (IO Unit) :=
  source.node.subscribeMap target.node f

/-- Subscribe with a pure map into a downstream event, scoped for cleanup. -/
protected def subscribeMapScoped (source : Event t a) (target : Event t b) (scope : SubscriptionScope)
    (f : a → b) : IO (IO Unit) := do
  let unsub ← source.node.subscribeMap target.node f
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
    let _ ← Event.subscribeMap source derived f
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
  deriveWith ctx source fun a fire => fire (f a)

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

/-- Merge two events into one (with explicit NodeId).
    When either fires, the merged event fires with that value.
    When both fire simultaneously (same frame), both values are delivered. -/
def mergeWithId [Timeline t] (e1 : Event t a) (e2 : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNodeWithId derivedNodeId height
  let _ ← Reactive.Event.subscribe e1 derived.fire
  let _ ← Reactive.Event.subscribe e2 derived.fire
  pure derived

/-- Merge two events into one.
    When either fires, the merged event fires with that value.
    When both fire simultaneously (same frame), both values are delivered.

    Example:
    ```
    -- Within SpiderM, prefer Event.mergeM:
    let combined ← Event.mergeM clickEvent keyEvent
    -- Fires when either click or key fires
    ``` -/
def merge [Timeline t] (ctx : TimelineCtx t) (e1 : Event t a) (e2 : Event t a) : IO (Event t a) := do
  let nodeId ← ctx.freshNodeId
  mergeWithId e1 e2 nodeId

end Event

end Reactive
