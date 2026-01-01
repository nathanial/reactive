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
private initialize globalPropagationQueue : IO.Ref (Option (IO.Ref PropagationQueue)) ←
  IO.mkRef none

/-- Set the global propagation context for frame-based firing -/
def setPropagationContext (queue : IO.Ref PropagationQueue) : IO Unit :=
  globalPropagationQueue.set (some queue)

/-- Clear the global propagation context -/
def clearPropagationContext : IO Unit :=
  globalPropagationQueue.set none

/-- Get the current propagation context -/
def getPropagationContext : IO (Option (IO.Ref PropagationQueue)) :=
  globalPropagationQueue.get

/-- A subscriber callback that receives event values -/
abbrev Subscriber (a : Type) := a → IO Unit

/-- Internal representation of an event with subscriber management.
    Push-based: when fired, all subscribers are notified. -/
structure EventNode (a : Type) where
  /-- Unique identifier for this node -/
  nodeId : NodeId
  /-- Height in dependency graph for topological ordering -/
  height : Height
  /-- Registered subscribers -/
  subscribers : IO.Ref (Array (SubscriberId × Subscriber a))
  /-- Counter for generating unique subscriber IDs -/
  nextSubId : IO.Ref Nat

namespace EventNode

/-- Create a new event node -/
def new (nodeId : NodeId) (height : Height := ⟨0⟩) : IO (EventNode a) := do
  let subs ← IO.mkRef #[]
  let nextId ← IO.mkRef 0
  pure { nodeId, height, subscribers := subs, nextSubId := nextId }

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
    let subs ← node.subscribers.get
    for (_, callback) in subs do
      callback value
  | some queueRef =>
    let q ← queueRef.get
    if q.inFrame then
      -- We're in a frame, enqueue for height-ordered processing
      let action : IO Unit := do
        let subs ← node.subscribers.get
        for (_, callback) in subs do
          callback value
      let pending : PendingFire := ⟨node.height, node.nodeId, action⟩
      queueRef.set (q.insert pending)
    else
      -- Context exists but not in frame, fire immediately
      let subs ← node.subscribers.get
      for (_, callback) in subs do
        callback value

/-- Get the number of subscribers -/
def subscriberCount (node : EventNode a) : IO Nat := do
  let subs ← node.subscribers.get
  pure subs.size

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

/-- Create a new event node (internal use) -/
protected def newNode [Timeline t] (nodeId : NodeId) (height : Height := ⟨0⟩) : IO (Event t a) := do
  let node ← EventNode.new nodeId height
  pure ⟨node⟩

/-- The event that never fires -/
def never [Timeline t] : IO (Event t a) := do
  Event.newNode ⟨0⟩  -- ID 0 reserved for never

/-- Create a new triggerable event.
    Returns the event and a function to fire it. -/
def newTrigger [Timeline t] (nodeId : NodeId) : IO (Event t a × (a → IO Unit)) := do
  let node ← EventNode.new nodeId
  pure (⟨node⟩, node.fire)

/-- Subscribe to an event -/
def subscribe (e : Event t a) (callback : Subscriber a) : IO (IO Unit) :=
  e.node.subscribe callback

/-- Subscribe with scope-based cleanup.
    The subscription is automatically unsubscribed when the scope is disposed. -/
def subscribeScoped (e : Event t a) (scope : SubscriptionScope)
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

/-- Map a function over event values.
    Creates a new derived event that transforms values from the source. -/
def map [Timeline t] (f : a → b) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t b) := do
  let derived ← Event.newNode derivedNodeId (source.height.inc)
  let _ ← source.subscribe fun a => derived.fire (f a)
  pure derived

/-- Filter event occurrences by a predicate -/
def filter [Timeline t] (p : a → Bool) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode derivedNodeId (source.height.inc)
  let _ ← source.subscribe fun a =>
    if p a then derived.fire a else pure ()
  pure derived

/-- Filter and map simultaneously -/
def mapMaybe [Timeline t] (f : a → Option b) (source : Event t a) (derivedNodeId : NodeId) : IO (Event t b) := do
  let derived ← Event.newNode derivedNodeId (source.height.inc)
  let _ ← source.subscribe fun a =>
    match f a with
    | some b => derived.fire b
    | none => pure ()
  pure derived

/-- Merge two events. When both fire simultaneously, both values are delivered. -/
def merge [Timeline t] (e1 : Event t a) (e2 : Event t a) (derivedNodeId : NodeId) : IO (Event t a) := do
  let height := Height.inc (max e1.height e2.height)
  let derived ← Event.newNode derivedNodeId height
  let _ ← e1.subscribe derived.fire
  let _ ← e2.subscribe derived.fire
  pure derived

end Event

end Reactive
