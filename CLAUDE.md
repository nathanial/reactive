# CLAUDE.md

This file provides guidance to Claude Code when working with the Reactive library.

## Overview

Reactive is a Reflex-style Functional Reactive Programming (FRP) library for Lean 4. It provides:

- **Event**: Discrete occurrences over time (push-based)
- **Behavior**: Time-varying values (pull-based, can sample anytime)
- **Dynamic**: Behavior with change notification Event

## Build Commands

```bash
# Build the library
lake build

# Run tests
lake build reactive_tests && .lake/build/bin/reactive_tests
```

## Architecture

### Core Types (`Reactive/Core/`)

- `Types.lean` - Timeline phantom type, NodeId, Height, SubscriberId
- `Event.lean` - Event type with subscriber management
- `Behavior.lean` - Behavior type with Monad instance
- `Dynamic.lean` - Dynamic combining Behavior + change Event

### Typeclasses (`Reactive/Class/`)

- `MonadSample` - Sample behaviors: `sample : Behavior t a → m a`
- `MonadHold` - Create behaviors from events: `hold`, `holdDyn`, `foldDyn`
- `TriggerEvent` - Fire external events: `newTriggerEvent`
- `PostBuild` - Post-construction effects: `getPostBuild`
- `Adjustable` - Dynamic switching (advanced)

### Combinators (`Reactive/Combinators/`)

- `Event.lean` - `tag`, `attach`, `gate`, `filter`, `merge`, `leftmost`, etc.
- `Behavior.lean` - `zipWith`, `allTrue`, `anyTrue`, `and`, `or`, etc.
- `Dynamic.lean` - `zipWith`, `changes`, `tagUpdated`, etc.
- `Switch.lean` - `switch`, `switchDyn`, `switchBehavior`, `switchDynamic`

### Host (`Reactive/Host/`)

- `Spider.lean` - IO-based push runtime (`SpiderM` monad)

## Usage Example

```lean
import Reactive

open Reactive
open Reactive.Host

def example : IO Unit := runSpider do
  -- Create a triggerable event
  let (clickEvent, fireClick) ← newTriggerEvent (t := Spider) (a := Unit)

  -- Hold a click count
  let clickCount ← foldDyn (fun _ n => n + 1) 0 clickEvent

  -- Subscribe to changes
  let _ ← liftM (m := IO) <| clickCount.updated.subscribe fun n =>
    IO.println s!"Click count: {n}"

  -- Fire some events
  liftM (m := IO) <| fireClick ()
  liftM (m := IO) <| fireClick ()
  liftM (m := IO) <| fireClick ()

  pure ()
```

## Key Design Patterns

### Timeline Phantom Type

All types are parameterized by a timeline `t` for type-safe separation:
```lean
def Event (t : Type) (a : Type) : Type
def Behavior (t : Type) (a : Type) : Type
def Dynamic (t : Type) (a : Type) : Type
```

### Hybrid Push-Pull

- Events are push-based (subscribers notified when fired)
- Behaviors are pull-based (value computed on sample)
- Dynamics combine both (push notification + pull value)

### Height-Based Ordering

Nodes have height for topological ordering to prevent glitches. Derived nodes have higher heights than their sources.

## Testing

Tests use the Crucible framework. See `ReactiveTests/` for examples.
