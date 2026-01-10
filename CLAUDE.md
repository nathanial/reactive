# CLAUDE.md

Guidance for Claude Code when working with the Reactive library.

## Overview

Reactive is a Reflex-style FRP library for Lean 4. See README.md for API documentation.

## Build Commands

```bash
lake build                                              # Build library
lake build reactive_tests && .lake/build/bin/reactive_tests  # Run tests
```

## Architecture

```
Reactive/
├── Core/           # Event, Behavior, Dynamic, SubscriptionScope
├── Class/          # MonadSample, MonadHold, TriggerEvent, Adjustable
├── Combinators/    # Event/Behavior/Dynamic combinators, Switch
├── Host/Spider.lean # IO-based runtime (SpiderM monad)
└── Proofs/         # Formal verification (monad laws, propagation)
```

## Key Gotchas

### ForIn Instances for Custom Monads

**Critical**: When defining monads that wrap `SpiderM` (e.g., via `ReaderT`), you must define an explicit `ForIn` instance. Without one, Lean's synthesized instance may cause infinite loops.

```lean
abbrev ReactiveM := ReaderT ReactiveEvents SpiderM

-- REQUIRED: Explicit ForIn instance
instance [ForIn SpiderM ρ α] : ForIn ReactiveM ρ α where
  forIn x init f := fun ctx => ForIn.forIn x init fun a b => f a b ctx
```

### SpiderM Lifting

`SpiderM` is a structure, not a type alias. To run IO:

```lean
SpiderM.liftIO (someIOAction)
```

To construct a SpiderM action that captures the environment:

```lean
let action : SpiderM Unit := ⟨fun env => do
  -- env.currentScope for subscription management
  -- env.ctx for timeline context
  someIOAction
⟩
```

### Event Subscription Cleanup

Use `SubscriptionScope` for automatic cleanup:

```lean
let scope ← SubscriptionScope.new
let unsub ← event.subscribe callback
scope.register unsub
-- Later: scope.dispose cleans up all subscriptions
```

SpiderM tracks a `currentScope` in its environment for automatic registration.

## Testing

Tests use Crucible framework in `ReactiveTests/`. Key test files:
- `EventTests.lean`, `BehaviorTests.lean`, `DynamicTests.lean` - Core type tests
- `SwitchTests.lean` - Switching combinator tests
- `PropagationTests.lean` - Event propagation ordering
- `ScopeTests.lean` - Subscription scope lifecycle
