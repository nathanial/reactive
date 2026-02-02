# CLAUDE.md

Guidance for Claude Code when working with the Reactive library.

## Overview

Reactive is a Reflex-style FRP library for Lean 4 with frame-based glitch-free propagation.

## Build Commands

```bash
lake build                                              # Build library
lake build reactive_tests && .lake/build/bin/reactive_tests  # Run tests
```

## Architecture

```
Reactive/
├── Core/           # Event, Behavior, Dynamic, SubscriptionScope, Types
├── Class/          # MonadSample, MonadHold, TriggerEvent, PostBuild, Adjustable
├── Combinators/    # Event/Behavior/Dynamic/Switch combinators
├── Host/
│   └── Spider/     # IO-based runtime
│       ├── Core.lean      # SpiderM monad, typeclasses, recursive combinators
│       ├── Event.lean     # Event SpiderM combinators
│       ├── Dynamic.lean   # Dynamic SpiderM combinators
│       ├── Behavior.lean  # Behavior SpiderM combinators
│       ├── Integration.lean # IO integration helpers
│       ├── Async.lean     # Async patterns (asyncIO, asyncOnEvent)
│       └── WorkerPool.lean # Priority-based worker pool
└── Proofs/         # Formal verification (monad laws)
```

## Type Aliases

After `open Reactive.Host`:
- `Evt a` = `Event Spider a`
- `Beh a` = `Behavior Spider a`
- `Dyn a` = `Dynamic Spider a`

---

## Complete Feature Reference

### Core Types

| Type | Description |
|------|-------------|
| `Event t a` | Discrete occurrences, push-based |
| `Behavior t a` | Time-varying values, pull-based (sampable) |
| `Dynamic t a` | Behavior + change Event |
| `SubscriptionScope` | Hierarchical subscription lifetime management |
| `TimelineCtx t` | Type-safe timeline separation |

### Typeclasses

| Typeclass | Key Methods |
|-----------|-------------|
| `MonadSample t m` | `sample : Behavior t a → m a` |
| `MonadHold t m` | `hold`, `holdDyn`, `foldDyn`, `foldDynM` |
| `TriggerEvent t m` | `newTriggerEvent`, `newEventWithTrigger` |
| `PostBuild t m` | `getPostBuild` |
| `Adjustable t m` | `runWithReplace` |

---

### Event Combinators (SpiderM)

All combinators auto-allocate NodeIds and register subscriptions with current scope.

#### Core Transformations
| Combinator | Type | Description |
|------------|------|-------------|
| `Event.mapM f e` | `(a → b) → Evt a → SpiderM (Evt b)` | Transform values |
| `Event.filterM p e` | `(a → Bool) → Evt a → SpiderM (Evt a)` | Filter by predicate |
| `Event.mapMaybeM f e` | `(a → Option b) → Evt a → SpiderM (Evt b)` | Filter + transform |
| `Event.voidM e` | `Evt a → SpiderM (Evt Unit)` | Discard values |
| `Event.mapConstM b e` | `β → Evt α → SpiderM (Evt β)` | Map to constant |

#### Merging (Reflex-style Left-Bias)
| Combinator | Description |
|------------|-------------|
| `Event.mergeM e1 e2` | Merge two events with left-bias (only left fires if simultaneous) |
| `Event.mergeAllM e1 e2` | Merge two events, firing all (both fire if simultaneous) |
| `Event.mergeListM es` | Merge list, batching simultaneous fires into a list |
| `Event.leftmostM es` | Take first firing event (first-only if simultaneous) |
| `Event.mergeAllListM es` | Merge list, all fire if simultaneous |

#### Behavior Interaction
| Combinator | Description |
|------------|-------------|
| `Event.tagM beh e` / `sampleM` | Sample behavior on event, discard event value |
| `Event.attachM beh e` / `snapshotM` | Pair behavior value with event value |
| `Event.attachWithM f beh e` | Combine behavior and event with function |
| `Event.gateM beh e` | Only fire when boolean behavior is true |

#### Fan-out / Splitting
| Combinator | Description |
|------------|-------------|
| `Event.fanM e` | Fan HashMap event to per-key events |
| `Event.selectM fan key` | Select key from fan |
| `Event.fanEitherM e` | Split `Sum a b` into two events |
| `Event.splitEM p e` / `partitionEM` | Split by predicate → (true, false) |

#### Accumulation / State
| Combinator | Description |
|------------|-------------|
| `Event.accumulateM f init e` / `scanM` | Fold over events, emit each value |
| `Event.withPreviousM e` | Emit `(prev, current)` pairs |
| `Event.distinctM e` / `dedupeM` | Skip consecutive duplicates |
| `Event.bufferM n e` | Collect n events before emitting batch |

#### Timing / Frame Control
| Combinator | Description |
|------------|-------------|
| `Event.delayFrameM e` | Delay to next propagation frame |
| `Event.takeNM n e` / `onceM` | Take first n occurrences |
| `Event.dropNM n e` | Drop first n occurrences |

#### Time-Based (requires Chronos.Duration)
| Combinator | Description |
|------------|-------------|
| `Event.delayDurationM d e` | Delay each event by duration |
| `Event.debounceM d e` | Fire after quiet period |
| `Event.throttleM d e leading trailing` | Rate limit (configurable leading/trailing) |
| `Event.windowM d e` | Tumbling time windows → batched arrays |

#### Simultaneous Event Handling
| Combinator | Description |
|------------|-------------|
| `Event.zipEM e1 e2` | Pair events firing in same frame |
| `Event.differenceM e1 e2` | Fire e1 only when e2 doesn't fire |

#### Switching
| Combinator | Description |
|------------|-------------|
| `Event.switchDynM de` | Switch to event inside Dynamic |

#### Fluent Variants (event-first argument order)
All have `'` suffix: `map'`, `filter'`, `mapMaybe'`, `merge'`, `mergeAll'`, `tag'`, `attach'`, `attachWith'`, `gate'`, `take'`, `drop'`, `scan'`, `delayFrame'`, `delay'`, `debounce'`, `throttle'`, `window'`, `once'`, `distinct'`, `buffer'`, `zipE'`, `difference'`, `fanEither'`, `splitE'`, `withPrevious'`

---

### Dynamic Combinators (SpiderM)

| Combinator | Description |
|------------|-------------|
| `Dynamic.mapM f d` | Map (no deduplication) |
| `Dynamic.mapUniqM f d` | Map with BEq deduplication on output |
| `Dynamic.memoizeM f d` | Map with BEq deduplication on input (skips computation when input unchanged) |
| `Dynamic.zipWithM f d1 d2` | Combine two dynamics |
| `Dynamic.zipWith3M f d1 d2 d3` | Combine three dynamics |
| `Dynamic.pureM x` | Constant dynamic |
| `Dynamic.apM df da` | Applicative apply |
| `Dynamic.changesM d` | Event of `(old, new)` pairs |
| `Dynamic.holdUniqDynM d` | Deduplicate updates |
| `Dynamic.switchM dd` | Flatten `Dyn (Dyn a)` → `Dyn a` |
| `Dynamic.bindOptionM d f default` | Bind/flatMap for `Dyn (Option a)` |
| `Dynamic.switchOptionM dd default` | Flatten `Dyn (Option (Dyn a))` with default |
| `Dynamic.traverseM f xs` | Map over list, collecting into `Dyn (List b)` |

#### Fluent Variants
`map'`, `mapUniq'`, `memoize'`, `zipWith'`, `zip'`, `zipWith3'`, `ap'`, `switch'`, `bindOption'`, `switchOption'`

---

### Behavior Combinators

| Combinator | Description |
|------------|-------------|
| `Behavior.constant x` | Constant behavior |
| `Behavior.fromSample action` | Behavior from IO sample action |
| `Behavior.map f b` | Functor map |
| `Behavior.zipWith f b1 b2` | Combine behaviors |
| `Behavior.allTrue bs` | All behaviors true |
| `Behavior.anyTrue bs` | Any behavior true |
| `Behavior.holdM init e` | Hold latest event value |
| `Behavior.foldBM f init e` | Fold over events |

---

### Switching Combinators

| Combinator | Description |
|------------|-------------|
| `switchDyn de` | Switch `Dyn (Evt a)` → `Evt a` |
| `switchDynamic dd` | Switch `Dyn (Dyn a)` → `Dyn a` |
| `switchHold init updates` | Hold event, switch on updates |
| `switchBehavior bb` | Switch `Beh (Beh a)` → `Beh a` |

---

### Recursive Event Networks

For circular dependencies between events/dynamics:

| Combinator | Description |
|------------|-------------|
| `SpiderM.fixDynM f` | Self-referential dynamic via lazy behavior |
| `SpiderM.fixDyn2M f` | Mutually recursive dynamic pair |
| `SpiderM.fixEventM f` | Self-referential event |

```lean
-- Counter that stops at maxValue
fixDynM fun counterBehavior => do
  let (clicks, fire) ← newTriggerEvent
  let gated ← Event.gateM (counterBehavior.map (· < maxValue)) clicks
  foldDyn (fun _ n => n + 1) 0 gated
```

---

### Integration Helpers

| Function | Description |
|----------|-------------|
| `fromIO poll` | Poll-based event source |
| `toCallback e cb` | Export event as callback |
| `performEvent e` | Run IO on event, return result event |
| `performEvent_ e` | Run IO on event, discard result |
| `fromRef init` | Event + update function from ref |
| `fromRefWithBehavior init` | Event + Behavior + update from ref |
| `runSpider m` | Run SpiderM network |
| `runSpiderLoop m source quit` | Run with event loop |
| `traverseDynList getKey f dynList` | Incremental list traversal with caching |
| `runWithReplaceRequester m` | Self-replacing computation |

---

### Async Patterns

| Function | Description |
|----------|-------------|
| `pushState init` | Dynamic with push update function |
| `pushStateWithModify init` | Dynamic with set + modify functions |
| `asyncIO action` | Run IO async, track as `Dyn (AsyncState e a)` |
| `asyncIOE action` | Async with typed errors (`Except`) |
| `asyncIOCancelable action` | Async with cancellation handle |
| `asyncOnEvent e action` | Run async on each event (cancels previous) |
| `asyncWithRetry config action` | Async with exponential backoff retry |
| `asyncOnEventWithRetry config e action` | Event-driven async with retry |

---

### Subscription Scope

```lean
let scope ← SubscriptionScope.new
scope.register unsub          -- Register cleanup action
let child ← scope.child       -- Create child scope
scope.dispose                 -- Dispose all (children first)
```

SpiderM tracks `currentScope` - all combinators auto-register.

---

### Error Handling

```lean
-- Set error handler
SpiderM.setErrorHandler strictErrorHandler  -- Re-raise first error
SpiderM.setErrorHandler defaultErrorHandler -- Log and continue (default)

-- Custom handler
SpiderM.setErrorHandler fun err => do
  IO.eprintln s!"Error: {err}"
  pure true  -- true = continue, false = stop
```

---

### Worker Pool (FRP-based)

FRP-based worker pool for async job processing with priority queue ordering and generation-based soft cancellation.

**Type constraints:** `jobId` requires `BEq`, `Hashable`, `Inhabited`; `job` requires `Inhabited`

#### Types

```lean
-- Configuration
structure WorkerPoolConfig where
  workerCount : Nat := 4  -- Number of worker threads

-- Commands for controlling the pool
inductive PoolCommand (jobId job : Type) where
  | submit (id : jobId) (job : job) (priority : Int)
  | cancel (id : jobId)
  | updatePriority (id : jobId) (newPriority : Int)
  | resubmit (id : jobId) (job : job) (priority : Int)

-- Job statuses
inductive JobStatus where
  | pending | running | completed | cancelled | error

-- Output structure with observable state
structure PoolOutput (jobId job result : Type) where
  completed : Evt (jobId × job × result)    -- Fires on successful completion
  cancelled : Evt jobId                      -- Fires on cancellation
  errored : Evt (jobId × String)            -- Fires on error
  jobStates : Dyn (HashMap jobId JobStatus) -- All job statuses
  pendingCount : Dyn Nat                     -- Jobs waiting in queue
  runningCount : Dyn Nat                     -- Jobs currently processing
```

#### API

| Function | Description |
|----------|-------------|
| `WorkerPool.fromCommands config process commands` | Create pool, returns `SpiderM (PoolOutput)` |
| `WorkerPool.fromCommandsWithShutdown config process commands` | Create pool with shutdown handle, returns `SpiderM (PoolOutput × PoolHandle)` |

#### Usage

```lean
-- Create pool from command stream
let config : WorkerPoolConfig := { workerCount := 4 }
let (cmdEvt, fireCmd) ← newTriggerEvent (a := PoolCommand Nat MyJob)
let (pool, handle) ← WorkerPool.fromCommandsWithShutdown config process cmdEvt

-- Or simpler version without shutdown handle
let pool ← WorkerPool.fromCommands config process cmdEvt

-- Observable outputs
let _ ← pool.completed.subscribe fun (id, job, result) => ...
let _ ← pool.cancelled.subscribe fun id => ...
let _ ← pool.errored.subscribe fun (id, errMsg) => ...
let pending ← pool.pendingCount.sample
let running ← pool.runningCount.sample
let states ← pool.jobStates.sample

-- Submit/cancel via commands
fireCmd (.submit 1 myJob 5)       -- Submit job with ID 1, priority 5
fireCmd (.cancel 1)               -- Cancel job 1 (soft cancellation if running)
fireCmd (.updatePriority 1 10)    -- Update priority of pending job
fireCmd (.resubmit 1 newJob 5)    -- Cancel existing and submit fresh

-- Graceful shutdown (cancels all pending, closes signal channel)
handle.shutdown
```

#### Cancellation Semantics

- **Pending jobs:** Removed from queue immediately
- **Running jobs:** Soft cancellation via generation counters - the IO operation continues but its result is discarded
- Higher priority values are processed first; FIFO ordering within same priority

---

## Key Gotchas

### ForIn Instances for Custom Monads

When wrapping `SpiderM`, define explicit `ForIn`:

```lean
abbrev ReactiveM := ReaderT Context SpiderM

instance [ForIn SpiderM ρ α] : ForIn ReactiveM ρ α where
  forIn x init f := fun ctx => ForIn.forIn x init fun a b => f a b ctx
```

### SpiderM Lifting

```lean
SpiderM.liftIO someIOAction

-- Or construct directly:
let action : SpiderM Unit := ⟨fun env => do
  -- env.currentScope, env.timelineCtx available
  someIOAction
⟩
```

### Avoid subscribe/sample/set Anti-pattern

Don't sample and set the same Dynamic in a subscription. Use `foldDyn`:

```lean
-- BAD: Can cause issues
let _ ← event.subscribe fun _ => do
  let n ← counter.sample
  setCounter (n + 1)

-- GOOD: Use foldDyn
let counter ← foldDyn (fun _ n => n + 1) 0 event
```

### BEq Requirements

`Dynamic.zipWithM`, `mapUniqM`, etc. require `BEq` for deduplication. Use `mapM` (no dedup) if `BEq` is unavailable.

---

## Testing

Tests in `ReactiveTests/` using Crucible:
- `EventTests.lean`, `BehaviorTests.lean`, `DynamicTests.lean` - Core types
- `SwitchTests.lean` - Switching combinators
- `PropagationTests.lean` - Frame-based ordering
- `ScopeTests.lean` - Subscription lifecycle
- `TemporalTests.lean` - Debounce/throttle/delay
- `RecursiveTests.lean` - fixDynM/fixEventM
- `AsyncTests.lean` - Async patterns
