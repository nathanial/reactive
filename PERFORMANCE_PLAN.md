# Reactive Performance Optimization Plan

This document outlines performance bottlenecks identified in the Reactive FRP library and the plan to address them. The goal is to achieve 10x+ performance improvements for use cases like video games, desktop GUIs, and game simulations.

## Current Performance Baseline

From performance tests (January 2025):

| Test | Configuration | Time |
|------|--------------|------|
| Wide fan-out | 1000 subscribers × 100 fires | 3ms |
| Deep chain | 1000 map operations × 100 fires | 2ms |
| Diamond pattern | 100 branches × 100 fires | **18ms** |
| Diamond pattern | 200 branches × 50 fires | **32ms** |
| Mixed network | 50 chains × 100 depth × 20 subs × 100 fires | 14ms |
| Switch combinator | 500 cycles | 455μs |

**Key observation**: Diamond patterns (fan-out + merge) are 10x slower than other operations.

---

## Critical Bottlenecks

### 1. MergeList Uses O(n²) List Append

**Status**: DONE (January 2025)

**Location**: `Host/Spider.lean:773`, `Combinators/Event.lean:89`

**Problem**:
```lean
bufferRef.modify (· ++ [a])  -- List append is O(n)!
```

For N events firing simultaneously:
- 1st event: O(0) to append to empty list
- 2nd event: O(1) to append to 1-element list
- Nth event: O(N-1) to append to (N-1)-element list
- **Total**: 0 + 1 + 2 + ... + (N-1) = O(N²)

For 100 branches × 100 fires = ~500,000 list copy operations.

**Solution**: Use `Array.push` which is O(1) amortized:
```lean
bufferRef.modify (·.push a)
```

**Actual improvement**: 1.6-1.8x for diamond/merge patterns (18ms→11ms, 32ms→18ms).

Note: The improvement is less than initially estimated because:
1. The propagation queue and subscriber notification still dominate for large branch counts
2. The `toList` conversion at flush time is still O(n), though only once per batch
3. For smaller branch counts, the O(n²) overhead was less severe

---

### 2. Scope Prepend is O(n)

**Status**: DONE (January 2025)

**Location**: `Core/Scope.lean:57`

**Problem**:
```lean
parent.subscriptions.modify fun arr => #[disposeChild] ++ arr
```

Array prepend copies the entire array. For deep scope hierarchies (common in component trees), this becomes O(n²).

**Solution**: Append to end instead of prepend:
```lean
parent.subscriptions.modify (·.push disposeChild)
```

Order doesn't matter for cleanup - all subscriptions get disposed regardless of order.

**Actual improvement**: O(n) → O(1) per child scope creation. Benefits deep component hierarchies but doesn't affect the diamond pattern benchmarks (which don't create deep scope trees).

---

### 3. Unsubscribe Filters Entire Array

**Status**: Not started

**Location**: `Core/Event.lean:131-132`

**Problem**:
```lean
node.subscribers.modify fun subs => subs.filter (·.1 != subId)
```

Every unsubscribe scans and copies the entire subscriber array O(n).

**Solution**: Lazy deletion with periodic compaction:
```lean
-- Mark as deleted (O(1))
node.subscribers.modify fun subs =>
  subs.map fun (id, cb) => if id == subId then (id, none) else (id, some cb)

-- Skip deleted during fire
for (_, callback?) in subs do
  if let some callback := callback? then
    callback value

-- Compact periodically when deleted count exceeds threshold
```

**Expected improvement**: 2-3x for dynamic graphs with frequent subscribe/unsubscribe.

---

## Medium-Term Optimizations

### 4. Object Pooling for EventNodes

**Status**: Not started

**Problem**: Each combinator allocates multiple `IO.Ref`s:
- `subscribers : IO.Ref (Array ...)`
- `mapConnect : IO.Ref (Option ...)`
- `upstreamUnsub : IO.Ref (Option ...)`
- `nextSubId : IO.Ref Nat`

For games creating/destroying reactive nodes per frame, allocation pressure adds up.

**Solution**:
- Pre-allocated node pools
- Recycling EventNode structures
- Bulk allocation of Refs

---

### 5. Batched Subscriber Notification

**Status**: Not started

**Problem**: Currently fires to each subscriber individually in a loop.

**Solution**: For subscribers that can process batches:
- Accumulate values in array
- Single notification with all values
- Let subscriber process in bulk (SIMD-friendly)

---

### 6. Mutable Propagation Queue

**Status**: Not started

**Location**: `Core/Types.lean:180-181`

**Problem**: Binary heap creates new arrays on each insert due to functional style.

**Solution**:
- Pre-sized mutable array with capacity tracking
- In-place heap operations within IO context
- Avoid array copies during high-frequency frames

---

## Architecture Changes for 10x+ Gains

### 7. Push-Pull Hybrid with Dirty Flags

**Status**: Not started

**Current model**: Pure push - every change propagates immediately through the graph.

**Problem**: For games running at 60fps, many intermediate values are computed but never read.

**Solution**:
- Mark nodes dirty on upstream change (O(1))
- Only compute values when sampled
- Batch all updates per game frame
- Skip computation for unobserved branches

This is a significant architectural change but could provide 10x+ improvement for sparse observation patterns.

---

### 8. Arena Allocation

**Status**: Not started

**Problem**: GC pressure from per-frame allocations.

**Solution**:
- Allocate all frame-local structures from a per-frame arena
- Reset arena at frame end (single O(1) operation)
- Eliminates GC pause spikes

Requires Lean FFI to implement custom allocator.

---

### 9. Compiled Reactive Graphs

**Status**: Not started

**Problem**: Runtime overhead from dynamic graph traversal:
- Hash/array lookups for subscribers
- Dynamic dispatch through closures
- Queue operations for propagation

**Solution**: For static topologies (common in games), compile the dependency graph at initialization:
- Generate direct function call chains
- Inline simple combinators (map, filter)
- Eliminate queue overhead for known topologies

This is the most complex optimization but could provide order-of-magnitude improvements for static graphs.

---

## Implementation Priority

### Phase 1: Quick Wins (Days)

| Change | File | Effort | Impact |
|--------|------|--------|--------|
| Array buffer in mergeList | `Spider.lean`, `Event.lean` | 1 hour | **1.6-1.8x** for merges (DONE) |
| Append instead of prepend | `Scope.lean` | 30 min | O(n)→O(1) for scope creation (DONE) |
| Lazy unsubscribe | `Event.lean` | 2-3 hours | 2-3x for dynamic graphs |

### Phase 2: Medium-Term (Weeks)

| Change | Effort | Impact |
|--------|--------|--------|
| Object pooling | 1-2 days | 2-3x allocation reduction |
| Mutable propagation queue | 1 day | 2x for high-frequency events |
| Batched notifications | 1 day | Variable, depends on use case |

### Phase 3: Architecture (Months)

| Change | Effort | Impact |
|--------|--------|--------|
| Push-pull hybrid | 1-2 weeks | 10x+ for sparse observation |
| Arena allocation | 1 week | Eliminates GC pauses |
| Compiled graphs | 2-4 weeks | 10x+ for static topologies |

---

## Benchmarking

After each optimization, re-run the performance test suite:

```bash
lake build reactive_tests && .lake/build/bin/reactive_tests --filter "perf"
```

Track improvements in this table:

| Optimization | Diamond 100×100 | Diamond 200×50 | Speedup | Date |
|--------------|-----------------|----------------|---------|------|
| Baseline | 18ms | 32ms | - | 2025-01-12 |
| mergeList Array buffer | 11ms | 18ms | **1.6-1.8x** | 2025-01-12 |
| Scope append | 11ms | 18ms | N/A (different workload) | 2025-01-12 |
| Lazy unsubscribe | TBD | TBD | TBD | TBD |

---

## Notes

- All optimizations should maintain the library's glitch-free semantics
- Frame-based batching must be preserved for correctness
- Height-ordered propagation ensures deterministic behavior
- Test thoroughly after each change - FRP bugs can be subtle
