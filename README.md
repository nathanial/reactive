# Reactive

A Reflex-style Functional Reactive Programming (FRP) library for Lean 4.

## Overview

Reactive provides three core abstractions for building reactive applications:

- **Event**: Discrete occurrences over time (push-based)
- **Behavior**: Time-varying values (pull-based, sampable anytime)
- **Dynamic**: Behavior with change notification Event

## Installation

Add to your `lakefile.lean`:

```lean
require reactive from git "https://github.com/nathanial/reactive" @ "v0.0.4"
```

## Quick Start

```lean
import Reactive

open Reactive
open Reactive.Host

def main : IO Unit := runSpider do
  -- Create a triggerable event
  let (clickEvent, fireClick) ← newTriggerEvent (t := Spider) (a := Unit)

  -- Hold a click count using foldDyn
  let clickCount ← foldDyn (fun _ n => n + 1) 0 clickEvent

  -- Subscribe to changes
  let _ ← SpiderM.liftIO <| clickCount.updated.subscribe fun n =>
    IO.println s!"Click count: {n}"

  -- Fire some events
  SpiderM.liftIO <| fireClick ()
  SpiderM.liftIO <| fireClick ()
  SpiderM.liftIO <| fireClick ()
```

## Core Types

### Event t a

Discrete occurrences that push values to subscribers when fired.

```lean
-- Create a triggerable event
let (event, fire) ← newTriggerEvent (t := Spider) (a := Nat)

-- Subscribe to events
let _ ← event.subscribe fun n => IO.println s!"Got: {n}"

-- Fire the event
fire 42
```

### Behavior t a

Time-varying values that can be sampled at any time.

```lean
-- Constant behavior
let b : Behavior Spider Nat := Behavior.constant 42

-- Sample a behavior
let value ← b.sample

-- Behaviors are monadic
let combined : Behavior Spider Nat := do
  let x ← Behavior.constant 10
  let y ← Behavior.constant 20
  pure (x + y)
```

### Dynamic t a

Combines a Behavior (current value) with an Event (change notifications).

```lean
-- Create from initial value and update event
let dyn ← holdDyn initialValue updateEvent

-- Sample current value
let current ← dyn.sample

-- Subscribe to updates
let _ ← dyn.updated.subscribe fun newValue => ...

-- Get as a Behavior
let behavior := dyn.current
```

## Typeclasses

| Typeclass | Purpose |
|-----------|---------|
| `MonadSample t m` | Sample behaviors: `sample : Behavior t a → m a` |
| `MonadHold t m` | Create behaviors from events: `hold`, `holdDyn`, `foldDyn` |
| `TriggerEvent t m` | Fire external events: `newTriggerEvent` |
| `PostBuild t m` | Post-construction effects: `getPostBuild` |
| `Adjustable t m` | Dynamic switching (advanced) |

## Combinators

### Event Combinators

```lean
-- Transform events
Event.map (· * 2) event

-- Filter events
Event.filter (· > 0) event

-- Merge events (right-biased for simultaneous)
Event.merge event1 event2

-- Sample behavior on event
tag behavior event

-- Combine behavior value with event value
attach behavior event

-- Gate events through a boolean behavior
gate boolBehavior event
```

### Behavior Combinators

```lean
-- Combine behaviors
Behavior.zipWith (· + ·) b1 b2

-- Boolean operations
Behavior.allTrue [b1, b2, b3]
Behavior.anyTrue [b1, b2, b3]
```

### Dynamic Combinators

```lean
-- Combine dynamics
Dynamic.zipWith (· + ·) d1 d2

-- Fold over events
foldDyn (fun input acc => acc + input) 0 event
```

### Switching Combinators

```lean
-- Switch to event inside behavior
switch behaviorOfEvents

-- Switch using dynamic of events
switchDyn dynamicOfEvents

-- Switch behaviors
switchBehavior behaviorOfBehaviors
```

### SpiderM Combinators

These work in `SpiderM` context without explicit `TimelineCtx`:

```lean
-- Transform/filter events
Event.mapM (· * 2) event           -- SpiderM (Event Spider Nat)
Event.filterM (· > 0) event        -- SpiderM (Event Spider Nat)
Event.voidM event                  -- SpiderM (Event Spider Unit)

-- Execute IO effects when event fires
performEvent_ (event.map' fun x => IO.println s!"Got: {x}")
```

### Subscription Scopes

Manage subscription lifetimes with hierarchical scopes:

```lean
let scope ← SubscriptionScope.new
scope.register unsubscribeAction    -- Register cleanup
let child ← scope.child             -- Create child scope
scope.dispose                       -- Disposes children first, then self
```

## Architecture

### Timeline Phantom Type

All types are parameterized by a timeline `t` for type-safe separation:

```lean
def Event (t : Type) (a : Type) : Type
def Behavior (t : Type) (a : Type) : Type
def Dynamic (t : Type) (a : Type) : Type
```

### Hybrid Push-Pull Model

- **Events** are push-based: subscribers are notified when events fire
- **Behaviors** are pull-based: values are computed when sampled
- **Dynamics** combine both: push notification + pull value

### Height-Based Ordering

Nodes have heights for topological ordering to prevent glitches. Derived nodes have higher heights than their sources.

## Building

```bash
# Build the library
lake build

# Run tests
lake build reactive_tests && .lake/build/bin/reactive_tests
```

## License

MIT License - see [LICENSE](LICENSE) for details.
