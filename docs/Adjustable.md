# Adjustable

The `Adjustable` typeclass enables **higher-order reactive programming** in FRP systems. While basic FRP deals with values flowing through a static network of events and behaviors, Adjustable allows the network itself to change at runtime.

## Overview

In traditional FRP, you build a reactive network at setup time and values flow through it. But what if you need to:

- Switch between completely different reactive computations based on user input?
- Replace a widget with a new one that has its own internal reactive state?
- Dynamically add or remove reactive elements from a collection?

This is where `Adjustable` comes in. It provides operations to run reactive computations that can be replaced wholesale, not just have their values updated.

**Key insight**: `runWithReplace` doesn't just swap values—it runs an entirely new `SpiderM` computation, which can create new events, behaviors, and dynamics with their own subscription lifecycles.

## Type Hierarchy

Adjustable builds on the FRP typeclass hierarchy:

```
MonadSample    ← Base: can sample current Behavior values
    ↑
MonadHold      ← Can create Behaviors/Dynamics from Events (hold state)
    ↑
Adjustable     ← Can switch between reactive computations dynamically
```

### MonadSample

The foundation. Provides the ability to read current values:

```lean
class MonadSample (t : Type) (m : Type → Type) where
  sample : Behavior t a → m a
```

### MonadHold

Adds the ability to create stateful reactive values:

```lean
class MonadHold (t : Type) (m : Type → Type) extends MonadSample t m where
  hold    : a → Event t a → m (Behavior t a)
  holdDyn : a → Event t a → m (Dynamic t a)
  foldDyn : (a → b → b) → b → Event t a → m (Dynamic t b)
```

### Adjustable

Adds dynamic computation switching:

```lean
class Adjustable (t : Type) (m : Type → Type) extends MonadHold t m where
  runWithReplace : m a → Event t (m a) → m (a × Event t a)
```

## Core Operation

### runWithReplace

```lean
runWithReplace : m a → Event t (m a) → m (a × Event t a)
```

Runs an initial computation and switches to replacement computations when the event fires.

**Parameters:**
- `initial : m a` — The computation to run immediately
- `replaceEvent : Event t (m a)` — Event carrying replacement computations

**Returns:**
- `a` — Result of the initial computation
- `Event t a` — Event that fires with results of replacement computations

**Behavior:**
1. The initial computation runs immediately, producing the first result
2. When `replaceEvent` fires with a new computation, that computation executes
3. The new computation's result fires on the returned event

This is the primitive for hot-swapping reactive logic at runtime.

## Convenience Function

The Spider runtime provides a concrete version that avoids universe inference issues:

### runWithReplaceM

```lean
def runWithReplaceM (initial : SpiderM a) (replaceEvent : Event Spider (SpiderM a))
    : SpiderM (a × Event Spider a)
```

Identical semantics to the typeclass method, with explicit Spider types.

## Integration Helpers

### runWithReplaceRequester

```lean
def runWithReplaceRequester (computation : SpiderM (a × Event Spider (SpiderM a)))
    : SpiderM (a × Event Spider a)
```

For self-replacing computations. The computation itself returns the event that triggers its replacement—useful for state machines or widgets that manage their own lifecycle.

**Example use case:** A multi-step form wizard where each step returns an event that transitions to the next step.

### traverseDynList

```lean
def traverseDynList (f : a → SpiderM b) (dynList : Dynamic Spider (List a))
    : SpiderM (Dynamic Spider (List b))
```

Maps a reactive computation over a Dynamic list, producing a Dynamic of results that updates whenever the input list changes.

**Note:** Current implementation rebuilds all results on each change. For large lists with frequent updates, a more sophisticated incremental implementation would be beneficial.

## Usage Patterns

### Basic Replacement

Switch between computations when an event fires:

```lean
test "runWithReplaceM fires result event on replacement" := do
  let result ← runSpider do
    let (replaceEvent, triggerReplace) ← newTriggerEvent (a := SpiderM Nat)
    let (initial, resultEvent) ← SpiderM.runWithReplaceM (pure 1) replaceEvent

    let resultsRef ← SpiderM.liftIO <| IO.mkRef [initial]
    let _ ← SpiderM.liftIO <| resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    SpiderM.liftIO <| triggerReplace (pure 2)
    SpiderM.liftIO <| triggerReplace (pure 3)

    SpiderM.liftIO resultsRef.get
  shouldBe result [1, 2, 3]
```

### Replacement with Internal State

Each replacement computation can create its own reactive state:

```lean
let computeWithState : Nat → SpiderM Nat := fun multiplier => do
  let (evt, trigger) ← newTriggerEvent (a := Nat)
  let dyn ← foldDyn (fun x acc => acc + x) 0 evt
  -- Fire some values within this computation's frame
  SpiderM.liftIO <| trigger (multiplier * 1)
  SpiderM.liftIO <| trigger (multiplier * 2)
  pure multiplier

let (initial, resultEvent) ← SpiderM.runWithReplaceM (computeWithState 5) replaceEvent
```

### Self-Replacing Computation

A computation that provides its own replacement trigger:

```lean
let (replaceEvent, triggerReplace) ← newTriggerEvent (a := SpiderM Nat)

let computation : SpiderM (Nat × Event Spider (SpiderM Nat)) :=
  pure (42, replaceEvent)

let (initial, resultEvent) ← runWithReplaceRequester computation
```

### Dynamic List Traversal

Map over a changing list:

```lean
let (listEvent, fireList) ← newTriggerEvent (a := List Nat)
let listDyn ← holdDyn [1, 2] listEvent

let f : Nat → SpiderM Nat := fun n => pure (n * 10)
let resultDyn ← traverseDynList f listDyn

-- Initial: [1, 2] → [10, 20]
-- After fireList [3, 4, 5]: [30, 40, 50]
```

## Design Notes

### Reflex Heritage

Adjustable is based on the [Reflex FRP](https://reflex-frp.org/) library's typeclass of the same name. Reflex pioneered practical higher-order FRP in Haskell, and its design has proven effective for building complex interactive applications.

### Subscription Lifecycle

When a replacement computation runs:
1. New subscriptions are registered with the current `SubscriptionScope`
2. Previous computation's subscriptions remain active unless explicitly disposed
3. This allows gradual transition but may require explicit cleanup for long-running applications

### Trade-offs

**Current implementation prioritizes:**
- Simplicity over incremental optimization
- Correctness over performance
- Explicit resource management

**Future improvements could include:**
- Automatic cleanup of replaced computation's subscriptions
- Incremental `traverseDynList` that only recomputes changed elements
- Keyed list diffing for efficient DOM-like updates

### When to Use Adjustable

Use `runWithReplace` when you need to:
- Switch between entirely different reactive behaviors (not just values)
- Replace a component that has internal state
- Implement state machines with distinct states having their own reactive logic

Use `traverseDynList` when you need to:
- Render a dynamic collection of items
- Each item needs its own reactive computation
- The list can grow, shrink, or have items replaced

For simpler cases where you just need to switch values (not computations), prefer `switch` or `switchPromptlyDyn` from the Switch combinators.
