# Missing Features in Reactive

This document catalogs features that would enhance the Reactive FRP library, organized by category and priority.

**Note**: This document was audited against the codebase. Features marked [EXISTS] have already been implemented.

---

## What Already Exists

Before listing missing features, here's what Reactive already has:

### Time-Based Combinators [EXISTS]
- `delayDurationM` - Delay events by a duration
- `debounceM` - Only fire after quiet period
- `throttleM` - Rate limit with leading/trailing options
- `windowM` - Tumbling time windows
- `delayFrameM` - Delay by one propagation frame

### Recursive Event Networks [EXISTS]
- `fixDynM` - Self-referential dynamics
- `fixDyn2M` - Mutually recursive dynamic pairs
- `fixEventM` - Self-referential events

### Scope-Based Subscription Management [EXISTS]
- `SubscriptionScope` with `new`, `child`, `register`, `dispose`
- All SpiderM combinators auto-register with current scope

### Error Handling [EXISTS]
- `PropagationErrorHandler` type
- `defaultErrorHandler` - logs and continues
- `strictErrorHandler` - re-raises first error

### Integration Helpers [EXISTS]
- `fromIO` - Poll-based event source
- `toCallback` - Export event as callback
- `performEvent` / `performEvent_` - Run IO on event
- `fromRef` / `fromRefWithBehavior` - Event from mutable ref

---

## 1. Periodic Events (Medium Priority)

### Interval

Create events that fire periodically.

```lean
def Event.interval (d : Chronos.Duration) : SpiderM (Evt Unit)
```

**Use case**: Polling, animation ticks, heartbeats.

### Timer

Fire once after a delay.

```lean
def Event.timer (d : Chronos.Duration) : SpiderM (Evt Unit)
```

**Use case**: Timeouts, delayed actions.

### Timeout

Emit a default value if no event occurs within a time window.

```lean
def Event.timeout (d : Chronos.Duration) (default : a) (e : Evt a) : SpiderM (Evt a)
```

---

## 2. Windowing Variants (Low Priority)

The library has `windowM` (tumbling windows) and `bufferM` (count-based). Still missing:

### Sliding Windows

Maintain a sliding window of the last N events.

```lean
def Event.slidingWindow (n : Nat) (e : Evt a) : SpiderM (Evt (Array a))
```

### Buffer Until Signal

Buffer events until a signal event fires.

```lean
def Event.bufferUntil (signal : Evt b) (e : Evt a) : SpiderM (Evt (Array a))
```

---

## 3. Concurrent Event Patterns (High Priority)

### Parallel Map

Process events in parallel with bounded concurrency.

```lean
def Event.parMap (concurrency : Nat) (f : a → IO b) (e : Evt a) : SpiderM (Evt b)
```

**Use case**: HTTP requests, file processing.

### Race

Take the first of multiple events, conceptually "canceling" others.

```lean
def Event.race (events : List (Evt a)) : SpiderM (Evt a)
```

### Distribute / Partition for Workers

Distribute events across N consumers (for worker pool patterns).

```lean
inductive DistributionStrategy
  | roundRobin
  | leastBusy
  | random
  | byKey (f : a → Nat)

def Event.distribute (n : Nat) (strategy : DistributionStrategy) (e : Evt a)
    : SpiderM (Array (Evt a))
```

---

## 4. Testing Utilities (High Priority)

### Virtual Time / Test Scheduler

Control time for deterministic testing of time-based combinators.

```lean
structure TestScheduler where
  advanceBy : Nat → IO Unit
  advanceTo : Nat → IO Unit
  flush : IO Unit

def withTestScheduler (test : TestScheduler → SpiderM a) : IO a
```

**Why important**: Without this, tests for `debounce`, `throttle`, `delay` require actual waiting.

### Event Recording

Record events for assertions.

```lean
structure EventRecorder (a : Type) where
  values : IO (Array a)
  timestamps : IO (Array Nat)
  clear : IO Unit

def Event.record (e : Evt a) : SpiderM (EventRecorder a)
```

---

## 5. Hot vs Cold Events (Low Priority)

### Replay

Cache recent events for late subscribers.

```lean
def Event.replay (count : Nat) (e : Evt a) : SpiderM (Evt a)
def Event.replayAll (e : Evt a) : SpiderM (Evt a)
```

### Share / Multicast

Ensure a single subscription is shared among multiple downstream subscribers.

```lean
def Event.share (e : Evt a) : SpiderM (Evt a)
```

---

## 6. Backpressure (Low Priority)

### Bounded Buffers

Limit queue size with overflow strategies.

```lean
inductive OverflowStrategy
  | dropOldest
  | dropNewest
  | block
  | error

def Event.boundedBuffer (size : Nat) (strategy : OverflowStrategy) (e : Evt a)
    : SpiderM (Evt a)
```

### Sample/Audit

Sample the most recent value at intervals (lossy).

```lean
def Event.auditTime (d : Chronos.Duration) (e : Evt a) : SpiderM (Evt a)
```

---

## 7. Debugging (Low Priority)

### Tap / Do

Side effects for debugging without affecting the stream.

```lean
def Event.tap (f : a → IO Unit) (e : Evt a) : SpiderM (Evt a)
def Event.log (label : String) (e : Evt a) [ToString a] : SpiderM (Evt a)
```

### Metrics

Built-in metrics collection.

```lean
structure EventMetrics where
  eventCount : IO Nat
  lastEventTime : IO (Option Nat)

def Event.withMetrics (e : Evt a) : SpiderM (Evt a × EventMetrics)
```

---

## 8. Behavior Combinators (Low Priority)

### Behavior from Polling

Create a behavior by polling an IO action at intervals.

```lean
def Behavior.poll (interval : Chronos.Duration) (action : IO a) : SpiderM (Beh a)
```

---

## Implementation Priority

### Phase 1 (Highest Value)
1. **Testing utilities** - TestScheduler for deterministic time-based tests
2. **Interval/Timer** - Basic periodic events
3. **Distribute** - Enable FRP worker pool patterns

### Phase 2 (Nice to Have)
1. Parallel map with concurrency control
2. Race combinator
3. Timeout combinator

### Phase 3 (Polish)
1. Debugging (tap, log, metrics)
2. Hot/cold event distinction (replay, share)
3. Windowing variants

---

## Design Considerations

### Consistency with Existing API

New combinators should follow established patterns:
- `fooM` variants using `SpiderM` (auto-allocate NodeId, register with scope)
- `foo'` fluent variants with event-first argument order
- Proper height tracking for glitch-free propagation

### Time-Based Features

The library uses `Chronos.Duration` for time. New time-based features should:
- Follow the pattern in `debounceM`/`throttleM` (async tasks with `IO.sleep`)
- Fire in new propagation frames via `env.withFrame`
- Use generation-based cancellation for stale operations
