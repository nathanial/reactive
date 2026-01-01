# Roadmap

This document outlines potential improvements, new features, and cleanup tasks for the Reactive FRP library.

---

## Recently Completed

### [DONE] Dynamic SpiderM Combinators

Added ergonomic `SpiderM`-based combinators that auto-allocate NodeIds:
- `Dynamic.mapM`, `Dynamic.zipWithM`, `Dynamic.zipWith3M`
- `Dynamic.pureM`, `Dynamic.apM`

Located in `Reactive/Host/Spider.lean`.

### [DONE] Fix switchDynamic

Fixed `switchDynamic` to properly propagate inner dynamic value changes using the update function from `Dynamic.new`.

### [DONE] Remove Broken switch Combinator

Removed `switch : Behavior t (Event t a) → Event t a` - it fundamentally cannot work because `Behavior` has no change notification. Users should use `switchDyn` with a `Dynamic` instead.

### [DONE] Add liftIO Helper

Added `SpiderM.liftIO` as a shorter alias for `liftM (m := IO)`.

### [DONE] Remove PropagationState

Removed unused `PropagationState` from `Types.lean`.

### [DONE] Document Height as Scaffolding

Added documentation noting that `Height` is tracked but not yet used for ordering - scaffolding for future glitch-free propagation.

### [DONE] Document Incomplete delay

Added WARNING/TODO to `delay` combinator noting it's currently a no-op pass-through.

### [DONE] Add Plausible Property Tests

Added 44 property tests covering boolean algebra, arithmetic, and function composition laws.

### [DONE] Add Switch Combinator Tests

Added comprehensive tests for `switchDyn`, `switchDynamic`, `switchHold`, and `switchBehavior`.

### [DONE] Event SpiderM Combinators

Added ergonomic `SpiderM`-based combinators for Events that auto-allocate NodeIds:
- Core: `Event.mapM`, `filterM`, `mapMaybeM`, `mergeM`
- Combinators: `tagM`, `attachM`, `attachWithM`, `gateM`, `mergeListM`, `leftmostM`, `fanEitherM`, `delayM`, `takeNM`, `dropNM`, `accumulateM`
- Plus `scanM` alias for `accumulateM`

Located in `Reactive/Host/Spider.lean`.

### [DONE] Add scan Alias

Added `Event.scan` as an alias for `Event.accumulate` (familiar name from other FRP libraries). Like `foldDyn` but returns an Event instead of a Dynamic.

### [DONE] Temporal Combinators (delay, debounce, throttle)

Implemented comprehensive temporal event combinators:

**Frame-based delay (`delayFrame`):**
- Delays event to the next propagation frame
- Uses `nextFramePending` queue in `PropagationQueue`
- Useful for breaking dependency cycles

**Time-based delay (`delayDurationM`):**
- Delays event by a specified `Chronos.Duration`
- Uses async tasks with `IO.sleep`
- Fires delayed events in new propagation frames

**Debounce (`debounceM`):**
- Only fires after source has been quiet for specified duration
- Uses generation-based cancellation pattern
- Useful for text input stabilization

**Throttle (`throttleM`):**
- Rate limits to at most one fire per interval
- Supports both leading and trailing fire options
- Configurable via `leading` and `trailing` parameters

**Files Modified:**
- `Reactive/Core/Types.lean` (added `nextFramePending` to PropagationQueue)
- `Reactive/Combinators/Event.lean` (implemented `delayFrame`)
- `Reactive/Host/Spider.lean` (added temporal combinators, modified drainQueue)
- `lakefile.lean` (added chronos dependency)
- `ReactiveTests/TemporalTests.lean` (new test file with 11 tests)

### [DONE] Event Batching for mergeList

Fixed `Event.mergeList` to properly batch simultaneous events into a single list instead of firing separate `[a]` lists for each event. Uses frame-based propagation: collects values in a buffer, schedules flush at derived height, fires all collected values as one batch.

### [DONE] Frame-Based Glitch-Free Propagation

Implemented true frame-based event handling with height-ordered processing:
- Added `PendingFire` and `PropagationQueue` types for queueing events by (height, nodeId)
- External triggers start a propagation frame via `SpiderEnv.withFrame`
- Events are processed in height order using priority queue semantics
- Derived events enqueue to the current frame instead of firing immediately
- Stable insertion maintains FIFO order for events at the same height/nodeId

This prevents glitches where derived nodes would see inconsistent intermediate states. Now all height-1 events process before height-2 events, etc.

**Files Modified:**
- `Reactive/Core/Types.lean` (PendingFire, PropagationQueue)
- `Reactive/Core/Event.lean` (global propagation context, modified fire)
- `Reactive/Host/Spider.lean` (withFrame, drainQueue, framed triggers)
- `ReactiveTests/PropagationTests.lean` (new test suite)

### [DONE] Adjustable Typeclass Implementation

Implemented the `Adjustable` typeclass enabling higher-order FRP patterns:
- Updated signature to match Reflex design: replacement event is a parameter, not produced by the computation
- Added `Adjustable Spider SpiderM` instance with `runWithReplace` and `traverseWithAdjust`
- Added convenience functions `runWithReplaceM` and `traverseWithAdjustM` to avoid universe polymorphism issues
- Added `runWithReplaceRequester` for computations that produce their own replacement event
- Added `traverseDynList` for traversing dynamic lists with automatic rebuilding on changes

**Key Design Decisions:**
- `runWithReplace : m a → Event t (m a) → m (a × Event t a)` - takes replacement event as input (practical) rather than having computation produce it
- Old subscriptions are not explicitly cleaned up (rely on GC)
- `traverseWithAdjust` returns never-firing update event (full incremental updates would require more infrastructure)

**Files Modified:**
- `Reactive/Class/Adjustable.lean` (updated signature)
- `Reactive/Host/Spider.lean` (Adjustable instance + helpers)
- `ReactiveTests/AdjustableTests.lean` (new test file with 8 tests)
- `ReactiveTests/Main.lean` (import AdjustableTests)

---

## Feature Proposals

### [Priority: High] Memory Management and Subscription Cleanup

**Description:** Implement proper subscription lifecycle management to prevent memory leaks in long-running applications.

**Rationale:** Currently, subscriptions are created but the unsubscribe actions are not systematically tracked or called. In long-running applications (e.g., UI frameworks), this leads to memory leaks as old subscriptions accumulate. Need:
1. Automatic cleanup when derived events are no longer referenced
2. Weak reference support or explicit disposal patterns
3. Subscription scope management (e.g., cleanup all subscriptions in a widget when it's removed)

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Dynamic.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Combinators/Switch.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host/Spider.lean`

**Estimated Effort:** Medium

**Dependencies:** None

---

### [DONE] Proper delay Combinator Implementation

Implemented. See "Temporal Combinators" in Recently Completed section.

---

### [DONE] Debounce and Throttle Combinators

Implemented. See "Temporal Combinators" in Recently Completed section.

---

### [DONE] Event Batching for mergeList

Fixed `Event.mergeList` to properly batch simultaneous events into a single list.

**Implementation:**
- Uses a buffer to collect values from all source events within a frame
- Schedules a single flush action at the derived node's height
- When flush runs (after all lower-height sources have fired), fires collected values as one batch

**Tests Added:** 4 new tests in `PropagationTests.lean`:
- `mergeList batches simultaneous events`
- `mergeList batches from diamond pattern`
- `mergeList separate frames produce separate batches`
- `mergeList with single event fires single-element list`

---

### [Priority: Low] Dynamic Functor and Applicative Instances

**Description:** Add pure `Functor`, `Applicative`, and potentially `Monad` instances for `Dynamic`.

**Rationale:** `Behavior` has full monad instances, making it ergonomic to compose. `Dynamic` lacks these instances because Dynamic operations are inherently effectful (require NodeId allocation).

**Current Workaround:** SpiderM-based combinators (`Dynamic.mapM`, `zipWithM`, `pureM`, `apM`) provide ergonomic composition within SpiderM context. See `Reactive/Host/Spider.lean`.

**Remaining Work:** Pure typeclass instances would require a `ReaderT NodeIdGenerator` approach or similar to handle NodeId allocation transparently.

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Dynamic.lean`

**Estimated Effort:** Medium

**Dependencies:** Design decision on NodeId generation strategy

---

### [Priority: Medium] Behavior from Dynamic Event

**Description:** Add a combinator to create a `Behavior t a` from an initial value and an update `Event t a`.

**Rationale:** Currently `MonadHold.hold` is available but a simpler pure function would be useful when you only need the sampling capability without the `Dynamic`'s update event.

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Behavior.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Class/MonadHold.lean`

**Estimated Effort:** Small

**Dependencies:** None

---

### [Priority: Low] Alternative Host Implementations

**Description:** Add alternative host implementations beyond Spider, such as:
1. Pure/test host for deterministic testing without IO
2. Async host with proper concurrency support
3. Single-threaded host optimized for UI frameworks

**Rationale:** Different use cases benefit from different runtime characteristics. A pure host would enable property-based testing of reactive networks. An async host would support concurrent event sources.

**Affected Files:**
- New files in `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host/`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host.lean` (re-exports)

**Estimated Effort:** Large

**Dependencies:** None, but would benefit from cleaner separation of host-agnostic abstractions

---

### [Priority: Low] Performance Optimizations

**Description:** Profile and optimize the FRP network for:
1. Reduce allocations in hot paths (event firing, behavior sampling)
2. Batch subscriber notifications when possible
3. Consider using packed arrays instead of `Array (SubscriberId x Subscriber a)`

**Rationale:** For use in games or high-frequency UI updates, performance is critical. The current implementation prioritizes clarity over performance.

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Behavior.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Dynamic.lean`

**Estimated Effort:** Medium

**Dependencies:** Profiling infrastructure

---

### [Priority: Low] Error Handling in Event Propagation

**Description:** Add configurable error handling for subscriber callbacks that throw exceptions.

**Rationale:** Currently, if a subscriber callback throws, it can break the entire propagation chain. Options:
1. Catch and log errors, continuing with other subscribers
2. Propagate first error after notifying all subscribers
3. Allow configurable error handlers per event

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean` (EventNode.fire)
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host/Spider.lean` (error handling config)

**Estimated Effort:** Small

**Dependencies:** None

---

## Code Improvements

### [DONE] Hide Node ID Management from Public API

**Current State:** SpiderM versions of all major combinators now auto-allocate NodeIds.

**Completed:**
- ✓ Dynamic SpiderM combinators (`mapM`, `zipWithM`, `zipWith3M`, `pureM`, `apM`)
- ✓ Event SpiderM combinators (15 functions including `mapM`, `filterM`, `mergeM`, `scanM`, etc.)

**Benefits:**
- Dramatically simpler API for common use cases
- Reduces boilerplate in user code
- Less error-prone (no risk of reusing node IDs)

**Remaining (optional):** Switch combinator SpiderM versions if needed.

---

### [Priority: Low] Consistent liftM Usage Pattern

**Current State:** Test code and examples use verbose `liftM (m := IO) <| ...` for lifting IO actions into SpiderM.

**Progress:**
1. ✓ Added `SpiderM.liftIO` convenience function
2. Remaining: Update existing tests to use `liftIO` instead of `liftM (m := IO)`
3. Remaining: Document preferred lifting patterns

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/ReactiveTests/*.lean` (update examples)

**Estimated Effort:** Small

---

### [Priority: Medium] Type-Safe Timeline Separation

**Current State:** Timeline is a phantom type parameter, but nothing prevents mixing events from different timelines at runtime.

**Proposed Change:** Add compile-time enforcement that prevents mixing `Event Spider a` with `Event OtherTimeline a` in combinators. Consider using dependent types or more refined type constraints.

**Benefits:**
- Catch timeline mixing errors at compile time
- Better guarantees for multi-timeline scenarios

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Types.lean`
- All files that use timeline parameters

**Estimated Effort:** Medium

---

### [Priority: Low] Replace IO.Ref with More Efficient Mutable State

**Current State:** Uses `IO.Ref` for all mutable state (subscriber lists, current values).

**Proposed Change:** Consider using `ST.Ref` within an `ST` region for better performance, or `IO.Mutex` for thread-safety if concurrent access is needed.

**Benefits:**
- Potential performance improvements
- Clearer concurrency story

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Dynamic.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host/Spider.lean`

**Estimated Effort:** Medium

---

## Code Cleanup

### [Priority: High] Add Documentation Comments to All Public APIs

**Issue:** Many public functions lack documentation comments or have minimal documentation.

**Location:** All files in `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/`

**Action Required:**
1. Add `/-- ... -/` docstrings to all public functions
2. Include usage examples in docstrings
3. Document type parameters and return values
4. Explain when to use each combinator

**Estimated Effort:** Medium

---

### [Priority: Medium] Consolidate Event.map Patterns

**Issue:** `Event.map`, `Event.filter`, and `Event.mapMaybe` have very similar structure. The pattern of creating a derived node and subscribing to the source is repeated.

**Location:** `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean` (lines 99-120)

**Action Required:** Extract a helper function for the common "derive from source event" pattern.

**Estimated Effort:** Small

---

### [DONE] Implement Height-Based Propagation Ordering

Completed as part of frame-based glitch-free propagation. Heights are now used for ordering events in the propagation queue.

---

### [Priority: Medium] Expand Test Coverage

**Issue:** Tests cover basic functionality but lack coverage for:
- Edge cases (empty events, zero subscribers)
- Combinator interactions
- Error conditions
- Memory leak scenarios

**Progress:**
- ✓ Added switch combinator tests (`ReactiveTests/SwitchTests.lean`)
- ✓ Added property tests for FRP laws (`ReactiveTests/PropertyTests.lean`)
- ✓ Added propagation tests for frame-based ordering (`ReactiveTests/PropagationTests.lean`)

**Location:** `/Users/Shared/Projects/lean-workspace/data/reactive/ReactiveTests/`

**Remaining Work:**
1. Add tests for all combinators in Event.lean, Behavior.lean
2. Add stress tests for subscription management
3. Add tests for complex network topologies

**Estimated Effort:** Medium

---

### [Priority: Low] Consistent Private Modifier Usage

**Issue:** Some constructors use `private mk ::` while others are public. The pattern is inconsistent.

**Location:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Event.lean`
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Core/Dynamic.lean`

**Action Required:** Review and standardize which constructors should be private vs public across all core types.

**Estimated Effort:** Small

---

### [Priority: Low] Remove Emoji from Test Output

**Issue:** Test output uses emoji characters which may not render correctly on all terminals.

**Location:** `/Users/Shared/Projects/lean-workspace/data/reactive/ReactiveTests/Main.lean` (lines 18-19)

**Action Required:** Replace emoji with ASCII alternatives or make them configurable.

**Estimated Effort:** Small

---

## API Enhancements

### [Priority: Medium] Fluent/Chainable Event Combinators

**Current State:** Event combinators require separate function calls with explicit node IDs:
```lean
let mapped ← Event.map f event nodeId1
let filtered ← Event.filter p mapped nodeId2
```

**Proposed Change:** Add extension methods or a builder pattern for fluent chaining:
```lean
let result ← event.mapM f >>= filterM p
```

**Benefits:** More readable and composable code

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Combinators/Event.lean`

**Estimated Effort:** Medium

---

### [Priority: Medium] RecursiveDo / mfix Support

**Description:** Add support for defining mutually recursive events/dynamics using a fixed-point combinator.

**Rationale:** Many reactive UIs require circular dependencies (e.g., a button that disables based on a counter that the button updates). Currently this requires manual delay or workarounds.

**Affected Files:**
- `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Host/Spider.lean`
- New module for recursive binding support

**Estimated Effort:** Large

---

### [Priority: Low] Integration Helpers for Common Patterns

**Description:** Add ready-made helpers for common integration scenarios:
1. `fromIO : IO (Option a) -> SpiderM (Event Spider a)` - poll-based event source
2. `toCallback : Event t a -> (a -> IO Unit) -> SpiderM Unit` - export event as callback
3. `fromChannel : Conduit.Channel a -> SpiderM (Event Spider a)` - conduit integration

**Rationale:** Reduce boilerplate when integrating reactive networks with external systems.

**Affected Files:**
- New file `/Users/Shared/Projects/lean-workspace/data/reactive/Reactive/Integration.lean`

**Estimated Effort:** Small

---
