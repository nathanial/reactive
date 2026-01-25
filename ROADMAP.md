# Roadmap

This document outlines potential improvements, new features, and cleanup tasks for the Reactive FRP library.

---

## Recently Completed

### [DONE] Remove Dynamic.new Public API

Removed the public `Dynamic.new` function that returned a setter, which enabled dangerous anti-patterns like subscribe/sample/set.

**Changes:**
- Removed `Dynamic.new` entirely from public API
- Made `Dynamic.newWithId` `protected` (requires full namespace `Reactive.Dynamic.newWithId`)
- Changed `Dynamic.mk` constructor from `private` to `protected`
- Added private `createDynamic` helper in Spider.lean for internal use
- Updated internal combinators (`switchDynamicWithId`) to use protected function

**Rationale:** The setter returned by `Dynamic.new` allowed imperative patterns that bypass the FRP model and can cause crashes when sampling and setting the same Dynamic during propagation. Users should create Dynamics via:
- `holdDyn` - hold most recent event value
- `foldDyn` - fold over event occurrences
- `Dynamic.map`, `zipWith` - derive from other dynamics

**Files Modified:**
- `Reactive/Core/Dynamic.lean` (removed new, protected newWithId)
- `Reactive/Host/Spider.lean` (added private createDynamic)
- `Reactive/Combinators/Switch.lean` (use protected newWithId)
- `ReactiveTests/DynamicTests.lean` (removed tests, updated others)

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
- Added `Adjustable Spider SpiderM` instance with `runWithReplace`
- Added convenience function `runWithReplaceM` to avoid universe polymorphism issues
- Added `runWithReplaceRequester` for computations that produce their own replacement event
- Added `traverseDynList` for traversing dynamic lists with automatic rebuilding on changes

**Key Design Decisions:**
- `runWithReplace : m a → Event t (m a) → m (a × Event t a)` - takes replacement event as input (practical) rather than having computation produce it
- Old subscriptions are not explicitly cleaned up (rely on GC)

**Files Modified:**
- `Reactive/Class/Adjustable.lean` (updated signature)
- `Reactive/Host/Spider.lean` (Adjustable instance + helpers)
- `ReactiveTests/AdjustableTests.lean` (new test file with 5 tests)
- `ReactiveTests/Main.lean` (import AdjustableTests)

### [DONE] Scope-Based Subscription Management

Implemented `SubscriptionScope` for automatic subscription cleanup:

**Core Features:**
- `SubscriptionScope.new` - Create a new scope
- `SubscriptionScope.child` - Create child scope (auto-disposed with parent)
- `SubscriptionScope.register` - Register unsubscribe action
- `SubscriptionScope.dispose` - Run all unsubscribe actions

**SpiderM Integration:**
- `SpiderEnv.currentScope` - Current scope in the monad environment
- `SpiderM.getScope` - Access current scope
- `SpiderM.withScope` - Run with child scope (returns scope for manual disposal)
- `SpiderM.withAutoDisposeScope` - Run with child scope that auto-disposes
- `Event.subscribeM` - Subscribe with automatic scope registration
- `Event.subscribeScoped` - IO-based scoped subscription

**Updated Combinators:**
All SpiderM combinators now register subscriptions with the current scope:
- Event: `mapM`, `filterM`, `mapMaybeM`, `mergeM`, `tagM`, `attachM`, `attachWithM`, `gateM`, `mergeListM`, `leftmostM`, `fanEitherM`, `delayFrameM`, `takeNM`, `dropNM`, `accumulateM`, `delayDurationM`, `debounceM`, `throttleM`
- Dynamic: `mapM`, `zipWithM`, `zipWith3M`
- MonadHold: `hold`, `holdDyn`, `foldDyn`, `foldDynM`
- Adjustable: `runWithReplace`, `runWithReplaceM`, `runWithReplaceRequester`, `traverseDynList`

**Automatic Cleanup:**
`runSpider` now disposes the root scope when the network terminates, automatically cleaning up all registered subscriptions.

**Files Modified:**
- `Reactive/Core/Scope.lean` (new - SubscriptionScope type)
- `Reactive/Core/Event.lean` (subscribeScoped)
- `Reactive/Core.lean` (import Scope)
- `Reactive/Host/Spider.lean` (scope in SpiderEnv, all combinator updates)
- `ReactiveTests/ScopeTests.lean` (new - 16 tests)
- `ReactiveTests/Main.lean` (import ScopeTests)

### [DONE] Behavior.hold and foldB Combinators

Added pure IO-based combinators for creating behaviors from events:
- `Behavior.hold` - Create a behavior holding the most recent event value
- `Behavior.foldB` - Create a behavior by folding over event occurrences
- `Behavior.holdM` - SpiderM version with scope registration
- `Behavior.foldBM` - SpiderM version with scope registration

### [DONE] Consolidate Event Derivation Pattern

Extracted `Event.deriveWith` helper function to reduce code duplication in:
- `Event.map`
- `Event.filter`
- `Event.mapMaybe`

### [DONE] Improve Documentation

Added comprehensive docstrings with examples to core Event functions:
- `Subscriber` type alias
- `Event.never`
- `Event.newTrigger`
- `Event.subscribe`
- `Event.subscribeScoped`
- `Event.map`, `filter`, `mapMaybe`, `merge`

### [DONE] RecursiveDo / mfix Support

Implemented fixed-point combinators enabling circular event/dynamic dependencies:

**Core Combinators:**
- `SpiderM.fixDynM` - Create self-referential dynamics via lazy behavior accessor
- `SpiderM.fixDyn2M` - Create mutually recursive pairs of dynamics
- `SpiderM.fixEventM` - Create self-referential events via lazy IO accessor

**Design Approach:**
- Pass `Behavior` (lazy accessor) instead of Dynamic to the recursive function
- Behaviors are only sampled in event handlers, which run after network construction
- Uses `IO.Ref (Option (Dynamic t a))` as placeholder filled after `f` completes
- Requires `Inhabited a` for default value before wiring

**Example - Counter that disables at maxValue:**
```lean
fixDynM fun counterBehavior => do
  let (clicks, fire) ← newTriggerEvent
  let gateBehavior := counterBehavior.map (fun c => decide (c < maxValue))
  let gatedClicks ← Event.gateM gateBehavior clicks
  foldDyn (fun _ n => n + 1) 0 gatedClicks
```

**Files Modified:**
- `Reactive/Host/Spider.lean` (fixDynM, fixDyn2M, fixEventM in SpiderM namespace)
- `ReactiveTests/RecursiveTests.lean` (new test file with 4 tests)
- `ReactiveTests/Main.lean` (import RecursiveTests)

### [DONE] Type-Safe Timeline Separation

Implemented compile-time enforcement preventing mixing events from different timelines:

**Core Change:**
- Added `TimelineCtx (t : Type) [Timeline t]` evidence type with private constructor
- Only host implementations (e.g., SpiderM) can create `TimelineCtx`
- Event/Dynamic creation functions now require `TimelineCtx` parameter

**API Split Pattern:**
- `functionWithId` variants: Take explicit `NodeId`, have `[Timeline t]` constraint
- `function` variants: Take `TimelineCtx t`, auto-generate NodeId

**SpiderM Integration:**
- `SpiderEnv.timelineCtx : TimelineCtx Spider` - Provides context for combinators
- `SpiderM.getTimelineCtx` - Access the timeline context
- All `*M` combinators use `*WithId` functions internally

**Files Modified:**
- `Reactive/Core/Types.lean` (TimelineCtx type)
- `Reactive/Core/Event.lean` (newNode/newNodeWithId, newTrigger/newTriggerWithId, map/mapWithId, etc.)
- `Reactive/Core/Dynamic.lean` (new/newWithId, map/mapWithId, hold/holdWithId, etc.)
- `Reactive/Combinators/Event.lean` (all combinators split)
- `Reactive/Combinators/Dynamic.lean` (all combinators split)
- `Reactive/Combinators/Switch.lean` (all combinators split)
- `Reactive/Host/Spider.lean` (timelineCtx in SpiderEnv)

---

## Known Usability Issues

These issues were identified during real-world usage in the afferent-demos ReactiveShowcase application.

### [ADDRESSED] subscribe/sample/set Pattern is Dangerous

**Problem:** Sampling and updating the same Dynamic inside a subscription callback can cause crashes or unexpected behavior.

```lean
-- This pattern crashed with multiple buttons:
for btn in buttons do
  let _ ← btn.onClick.subscribe fun _ => do
    let n ← counter.sample    -- sampling inside subscribe
    setCounter (n + 1)        -- then setting

-- Correct pattern - use foldDyn:
let allClicks ← Event.mergeM btn1.onClick btn2.onClick
let counter ← foldDyn (fun _ n => n + 1) 0 allClicks
```

**Resolution:** Removed `Dynamic.new` from the public API. Users can no longer create Dynamics with exposed setters, forcing use of proper FRP patterns like `holdDyn` and `foldDyn`. See "Remove Dynamic.new Public API" in Recently Completed.

---

### [Issue] No Easy Way to Merge Multiple Events

**Problem:** Merging more than 2 events requires chaining `mergeM` calls:

```lean
-- Current (verbose):
let clicks12 ← Event.mergeM e1 e2
let clicks123 ← Event.mergeM clicks12 e3
let allClicks ← Event.mergeM clicks123 e4

-- Desired:
let allClicks ← Event.mergeMany #[e1, e2, e3, e4]
```

**Workaround:** Chain `mergeM` calls manually.

**Proposed Fix:** Add `Event.mergeManyM : Array (Event t a) → SpiderM (Event t a)` combinator.

---

### [Issue] Circular Dependencies for Focus-like Patterns

**Problem:** Proper FRP modeling of shared focus state requires circular dependencies:

1. Components need to expose `onFocus : Event`
2. App merges focus events: `focusedInput ← hold none (merge allFocusEvents)`
3. Components need `focusedInput : Dynamic` to know when to handle keyboard

This creates a circular dependency: components need `focusedInput` which depends on component events.

**Current Workaround:** Use imperative `setFocusedInput` callback passed to components, with pragmatic `subscribe` usage.

**Potential Fix:** The library has `fixDynM` for self-referential dynamics, but extending this pattern to multi-component scenarios is non-trivial. Consider adding `RecursiveDo`-style syntax support.

---

### [Issue] BEq Constraints Proliferate After Deduplication Fix

**Problem:** The fix for Dynamic.mapM deduplication (only firing `.updated` when value actually changes) requires `BEq` constraints. These constraints cascade through the codebase.

```lean
-- Now requires BEq:
def mapM [BEq b] (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b)
```

**Impact:** Adding `BEq` constraints to complex types can be tedious. Some types may not have meaningful equality.

**Workaround:** Derive `BEq` for structures, or use types with existing `BEq` instances.

**Potential Fix:** Consider providing non-deduplicating variants (`mapM'` without dedup) for cases where it's not needed or `BEq` is unavailable.

---

### [Issue] Some subscribe Usage is Unavoidable

**Problem:** For complex interactive widgets that need to both read state AND update it in response to events (like TextInput focus management), pure FRP patterns are awkward or impossible without circular dependency support.

**Affected Patterns:**
- Focus clearing when clicking outside text inputs
- Keyboard routing based on current focus state
- Complex state machines with multiple inputs

**Current Approach:** Accept pragmatic `subscribe` usage for these cases, marked with TODO comments for future refactoring.

---

## Feature Proposals

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

### [DONE] Behavior from Dynamic Event

Implemented as part of "Behavior.hold and foldB Combinators" in Recently Completed section.

Available functions:
- `Behavior.hold` - Pure IO version
- `Behavior.foldB` - Pure IO version with fold
- `Behavior.holdM` - SpiderM version with scope registration
- `Behavior.foldBM` - SpiderM version with fold and scope registration

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

### [DONE] Error Handling in Event Propagation

Added configurable error handling for subscriber callbacks that throw exceptions:

**Types:**
- `PropagationErrorHandler`: `IO.Error → IO Bool` (return true to continue)
- `defaultErrorHandler`: Logs to stderr and continues propagation
- `strictErrorHandler`: Re-raises first error, stopping propagation

**SpiderM API:**
- `SpiderM.getErrorHandler` / `SpiderM.setErrorHandler`
- `runSpiderWithErrorHandler network handler`

**Implementation:**
- `SpiderEnv` now contains an error handler ref
- `drainQueue` wraps `pending.fire` in try-catch
- Default behavior: log errors but continue propagating to other subscribers

**Files Modified:**
- `Reactive/Host/Spider.lean`

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

### [DONE] Consistent liftM Usage Pattern

All test code and examples now use the consistent `SpiderM.liftIO` pattern instead of the verbose `liftM (m := IO)` syntax.

**Completed:**
1. ✓ Added `SpiderM.liftIO` convenience function
2. ✓ Updated EventTests.lean to use `SpiderM.liftIO`
3. ✓ Updated DynamicTests.lean to use `SpiderM.liftIO`
4. ✓ Updated README.md examples to use `SpiderM.liftIO`
5. ✓ Updated Reactive.lean documentation to use `SpiderM.liftIO`

**Preferred Pattern:**
```lean
-- Use this:
SpiderM.liftIO <| someIOAction

-- Instead of:
liftM (m := IO) <| someIOAction
```

---

### [DONE] Type-Safe Timeline Separation

Implemented. See "Type-Safe Timeline Separation" in Recently Completed section.

---

### [CLOSED - Won't Fix] Replace IO.Ref with More Efficient Mutable State

**Decision:** Current `IO.Ref` performance is sufficient. Benchmarks show good results:
- 100 IO.Ref ops: ~2000ns
- 1000 subscribers x 100 fires: 5ms
- 1000-deep chain x 100 fires: 45000ns

The architectural changes required to switch to `ST.Ref` would be significant (different monad structure) and the performance gains are uncertain given Lean's already-optimized `IO.Ref` implementation.

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

### [DONE] Consolidate Event.map Patterns

Already implemented. The `deriveWith` and `deriveWithId` helper functions exist in `Event.lean` (lines 204-219) and are used by `map`, `filter`, and `mapMaybe`.

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

### [DONE] Fluent/Chainable Event Combinators

Added fluent combinators with event-first argument order for easier chaining:

**Event Fluent Combinators:**
```lean
-- Chain with monadic bind
Event.map' event (· * 2) >>= (Event.filter' · (· > 3))

-- Available: map', filter', mapMaybe', merge', tag', attach',
-- attachWith', gate', take', drop', scan', delayFrame',
-- delay', debounce', throttle', fanEither'
```

**Dynamic Fluent Combinators:**
```lean
Dynamic.map' dynA f >>= (Dynamic.zipWith' · g dynB)

-- Available: map', zipWith', zip', zipWith3', ap'
```

**Note:** Dot notation (`event.map' f`) doesn't work due to Lean 4's namespace resolution - the type is `Reactive.Event` but combinators are in `Reactive.Host.Event`. Use explicit form `Event.map' event f` instead.

**Files Modified:**
- `Reactive/Host/Spider.lean` (fluent combinators for Event and Dynamic)
- `ReactiveTests/EventTests.lean` (3 new tests)

---

### [DONE] Integration Helpers for Common Patterns

Added helpers for integrating reactive networks with external systems:

**Functions:**
- `fromIO : IO (Option a) → SpiderM (Event Spider a × IO Unit)` - Poll-based event source
- `toCallback : Event Spider a → (a → IO Unit) → SpiderM Unit` - Export event as callback
- `fromRef : a → SpiderM (Event Spider a × (a → IO Unit) × IO.Ref a)` - Event from mutable ref
- `fromRefWithBehavior : a → SpiderM (Event Spider a × Behavior Spider a × (a → IO Unit))` - Event + Behavior from ref

**Note:** `fromChannel` not implemented (Conduit not a dependency)

**Files Modified:**
- `Reactive/Host/Spider.lean`

---
