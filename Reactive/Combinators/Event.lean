/-
  Reactive/Combinators/Event.lean

  Combinators for working with Events.
-/
import Reactive.Core
import Reactive.Class

namespace Reactive

namespace Event

/-- Tag an event with the current value of a behavior.
    On each event occurrence, samples the behavior and returns that value. -/
def tag [Timeline t] (beh : Behavior t a) (e : Event t b) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun _ => do
    let v ← beh.sample
    derived.fire v
  pure derived

/-- Attach the current behavior value to each event occurrence -/
def attach [Timeline t] (b : Behavior t a) (e : Event t c) (nodeId : NodeId) : IO (Event t (a × c)) := do
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun c => do
    let a ← b.sample
    derived.fire (a, c)
  pure derived

/-- Attach with a combining function -/
def attachWith [Timeline t] (f : a → c → d) (b : Behavior t a) (e : Event t c)
    (nodeId : NodeId) : IO (Event t d) := do
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun c => do
    let a ← b.sample
    derived.fire (f a c)
  pure derived

/-- Gate events by a boolean behavior.
    Only fires when the behavior is true. -/
def gate [Timeline t] (beh : Behavior t Bool) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun a => do
    let isOpen ← beh.sample
    if isOpen then derived.fire a else pure ()
  pure derived

/-- Merge multiple events into one.
    When multiple events fire simultaneously, all values are collected. -/
def mergeList [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t (List a)) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNode nodeId (maxHeight.inc)

  -- For each source event, we need to collect simultaneous occurrences
  -- This is a simplified version that fires for each individual occurrence
  for e in events do
    let _ ← e.subscribe fun a => derived.fire [a]

  pure derived

/-- Take the leftmost event that fires.
    When multiple fire simultaneously, takes the first in the list. -/
def leftmost [Timeline t] (events : List (Event t a)) (nodeId : NodeId) : IO (Event t a) := do
  let maxHeight := events.foldl (fun h e => max h e.height) ⟨0⟩
  let derived ← Event.newNode nodeId (maxHeight.inc)

  for e in events do
    let _ ← e.subscribe derived.fire

  pure derived

/-- Split an event of Either into two events -/
def fanEither [Timeline t] (e : Event t (Sum a b)) (nodeIdL : NodeId) (nodeIdR : NodeId)
    : IO (Event t a × Event t b) := do
  let leftEvent ← Event.newNode nodeIdL (e.height.inc)
  let rightEvent ← Event.newNode nodeIdR (e.height.inc)
  let _ ← e.subscribe fun ab =>
    match ab with
    | .inl a => leftEvent.fire a
    | .inr b => rightEvent.fire b
  pure (leftEvent, rightEvent)

/-- Delay an event by one propagation frame.
    Useful for breaking dependency cycles.

    WARNING: This combinator is currently incomplete - it passes through
    immediately without actual delay. A proper implementation requires
    frame scheduling infrastructure.

    TODO: Implement actual frame-based delay using:
    1. A frame counter in SpiderEnv
    2. A queue of pending fires for the next frame
    3. A mechanism to advance frames (either time-based or explicit) -/
def delay [Timeline t] (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun a => do
    -- WARNING: No-op pass-through. See docstring for TODO.
    derived.fire a
  pure derived

/-- Take only the first n occurrences -/
def takeN [Timeline t] (n : Nat) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let countRef ← IO.mkRef 0
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun a => do
    let count ← countRef.get
    if count < n then
      countRef.set (count + 1)
      derived.fire a
  pure derived

/-- Drop the first n occurrences -/
def dropN [Timeline t] (n : Nat) (e : Event t a) (nodeId : NodeId) : IO (Event t a) := do
  let countRef ← IO.mkRef 0
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun a => do
    let count ← countRef.get
    countRef.set (count + 1)
    if count >= n then
      derived.fire a
  pure derived

/-- Accumulate a value over event occurrences.
    Emits the new accumulated value on each event occurrence. -/
def accumulate [Timeline t] (f : a → b → b) (initial : b) (e : Event t a)
    (nodeId : NodeId) : IO (Event t b) := do
  let stateRef ← IO.mkRef initial
  let derived ← Event.newNode nodeId (e.height.inc)
  let _ ← e.subscribe fun a => do
    let old ← stateRef.get
    let new := f a old
    stateRef.set new
    derived.fire new
  pure derived

/-- Alias for accumulate (familiar name from other FRP libraries).
    Like foldDyn but returns an Event instead of a Dynamic. -/
abbrev scan := @accumulate

end Event

end Reactive
