import Crucible
import Reactive

/-!
# Frame Semantics Tests

This module contains tests that document and validate the frame-based propagation
semantics of the Reactive FRP library. These tests serve as both executable
specifications and educational documentation.

## What Are Frames?

A **frame** is a propagation cycle where all events logically occur "at the same
instant." The Reactive library uses frames to ensure **glitch-free propagation**:
intermediate states during event propagation are never observable.

## Key Concepts

1. **Frame entry**: When a trigger fires outside a frame, it starts a new frame
2. **Frame nesting**: When a trigger fires inside a frame, it enqueues without starting a new drain
3. **Queue draining**: Events are processed in **height order** (topological order based on
   dependency depth—events closer to sources fire before derived events) after callbacks complete
4. **Glitch freedom**: You cannot sample intermediate propagation states
5. **Framed triggers**: `newTriggerEvent` returns a trigger that automatically wraps firing
   with frame logic—this is why triggers behave consistently whether called from user code
   or internal combinators

## Why This Matters

Code that works at the top level may behave differently inside event callbacks,
replacement computations, or other contexts where a frame is already active.
Understanding frame semantics is essential for writing correct FRP code.
-/

namespace ReactiveTests.FrameSemanticsTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Frame Semantics Tests"

/-!
## Test: Frame Semantics in Replacement Computations

This test demonstrates a subtle but important aspect of FRP frame semantics:
**triggers behave differently depending on whether code runs inside or outside
a propagation frame**.

### What This Test Validates

1. FRP combinators (`newTriggerEvent`, `foldDyn`) can be used inside replacement
   computations without crashing or corrupting state.

2. The return value semantics are consistent: both initial and replacement
   computations return the same computed value for the same input.

3. Understanding that you cannot observe synchronous propagation results
   when running inside an existing frame.

### Background: Frame-Based Propagation

The Reactive library uses **frames** to ensure glitch-free propagation:

- A **frame** is a propagation cycle where all events logically occur
  "at the same instant"
- Within a frame, you cannot observe intermediate states
- Triggers from `newTriggerEvent` are wrapped with `withFrame`:
  - If NOT in a frame: starts frame → runs action → drains queue → ends frame
  - If ALREADY in a frame: just runs action (enqueues, no immediate drain)

### Why This Matters

Consider a computation that:
1. Creates an event and a `foldDyn` that accumulates values
2. Fires triggers to that event
3. Samples the dynamic

**Intuition says**: sample should return the accumulated value.
**Reality depends on context**:
- Outside a frame: each trigger gets its own frame, propagates, then returns.
  Sampling sees the accumulated result.
- Inside a frame: triggers enqueue to the current frame's queue. The queue
  drains AFTER our computation returns. Sampling sees the initial value.

### The Test Design

We intentionally return `multiplier` directly rather than sampling the dynamic.
This makes the test deterministic regardless of frame context, while still
exercising the FRP combinator machinery.

### What Would Be Wrong Behavior

- If we returned `sample dyn.current` and expected it to equal `multiplier * 3`
  (the sum of `multiplier * 1` and `multiplier * 2`), the test would:
  - PASS for the initial computation (runs outside frame)
  - FAIL for the replacement computation (runs inside triggerReplace's frame)

This asymmetry would indicate either:
1. A misunderstanding of frame semantics (user error), or
2. Inconsistent frame handling (library bug)

The current test design avoids this trap by not relying on synchronous propagation.

### Execution Trace

> **Note**: The trace below uses internal implementation concepts (`withFrame`, `rawTrigger`,
> `drainQueue`, `inFrame`, etc.) to explain the behavior. These are not user-facing APIs—they're
> the underlying mechanisms that make frame semantics work. Users interact with `newTriggerEvent`,
> `foldDyn`, `sample`, etc., and the frame logic happens automatically.

**Initial computation** (`computeWithState 5`):
```
runWithReplaceM calls initial.run env
  → NOT in a frame (no event has fired yet)
  → newTriggerEvent creates (evt, framedTrigger)
  → foldDyn creates dynamic (valueRef = 0, subscribes to evt)
  → trigger 5:
      withFrame sees NOT in frame
      → starts NEW frame (inFrame = true)
      → rawTrigger 5 → fire sees inFrame, ENQUEUES
      → drainQueue → pops, fires → foldDyn callback → valueRef = 5
      → frame ENDS (inFrame = false)
  → trigger 10:
      withFrame sees NOT in frame (previous frame ended!)
      → starts NEW frame
      → ... valueRef = 15
      → frame ENDS
  → pure 5 returns 5
```

**Replacement computation** (`computeWithState 10`):
```
triggerReplace (computeWithState 10)
  → triggerReplace IS a framedTrigger (from newTriggerEvent)
  → withFrame sees NOT in frame, starts frame
  → rawTrigger fires replaceEvent → ENQUEUES
  → drainQueue starts processing...
      → pops replaceEvent fire
      → fires it → subscription callback runs:
          → (computeWithState 10).run env
          → STILL IN FRAME (drainQueue hasn't finished!)
          → newTriggerEvent creates (evt2, framedTrigger2)
          → foldDyn creates dynamic (valueRef2 = 0)
          → trigger 10:
              withFrame sees ALREADY in frame
              → just runs rawTrigger (NO new drain!)
              → rawTrigger 10 → fire sees inFrame, ENQUEUES
          → trigger 20:
              → same, ENQUEUES
          → pure 10 returns 10
          → (if we had done `sample dyn.current` here, it would return 0!)
      → subscription callback returns 10
      → drainQueue continues...
      → pops evt2 fires (10, 20), processes them
      → valueRef2 becomes 30 (but we already returned!)
  → frame ENDS
```

### Key Insight

The initial computation's triggers each get their own frame because each
`trigger` call finds `inFrame = false`. But the replacement computation's
triggers find `inFrame = true` (we're inside `triggerReplace`'s frame),
so they enqueue without draining.

This is **correct glitch-free FRP semantics**, not a bug. The FRP
infrastructure created inside replacements works correctly—you just
can't observe its synchronous effects within the same frame.
-/
test "replacement computations run inside caller's frame" := do
  let result ← runSpider do
    -- newTriggerEvent returns a "framed trigger": calling triggerReplace automatically
    -- wraps the fire in frame logic (starts frame if needed, drains queue after).
    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- A computation that creates FRP infrastructure and returns a value.
    -- We deliberately return `multiplier` directly rather than sampling the
    -- dynamic, because sampling would give different results depending on
    -- whether we're inside a frame (replacement) or outside (initial).
    let computeWithState : Nat → SpiderM Nat := fun multiplier => do
      -- Create fresh event and dynamic for this computation.
      -- Note: `trigger` is also a framed trigger, so calling it will start/join a frame.
      let (evt, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
      let _dyn ← foldDyn (fun x acc => acc + x) 0 evt

      -- Fire values to the event. These will:
      -- - Propagate immediately if we're outside a frame (initial computation)
      -- - Enqueue for later if we're inside a frame (replacement computation)
      -- Either way, the FRP infrastructure is correctly wired up.
      trigger (multiplier * 1)
      trigger (multiplier * 2)

      -- Return the multiplier directly. We prefix `dyn` with underscore to
      -- acknowledge we're not using it for the return value—this is intentional.
      -- See the documentation above for why sampling would be problematic.
      pure multiplier

    let (initial, resultEvent) ← SpiderM.runWithReplaceM (computeWithState 5) replaceEvent

    -- Collect all results: initial value plus any replacement values
    let resultsRef ← SpiderM.liftIO <| IO.mkRef [initial]
    let _ ← resultEvent.subscribe fun n =>
      resultsRef.modify (· ++ [n])

    -- Trigger a replacement. This fires replaceEvent, which:
    -- 1. Starts a new frame (since we're not currently in one)
    -- 2. Runs computeWithState 10 inside that frame
    -- 3. The replacement's triggers enqueue (we're in a frame)
    -- 4. Returns 10 (the multiplier, not a sampled value)
    triggerReplace (computeWithState 10)

    SpiderM.liftIO resultsRef.get

  -- Both computations return their multiplier directly: [5, 10]
  -- This is the expected and correct behavior.
  --
  -- INCORRECT expectation would be [15, 30] if we expected `sample dyn.current`
  -- to return accumulated values. That would fail because:
  -- - Initial: 5 + 10 = 15 ✓ (triggers propagate, each in own frame)
  -- - Replacement: would get 0, not 30 ✗ (triggers enqueue, sample sees initial)
  shouldBe result [5, 10]

/-!
## Test: Demonstrating the Frame Nesting Asymmetry

This test explicitly demonstrates that the **exact same code** produces different
results depending on frame context. This is not a bug—it's the expected behavior
of glitch-free FRP propagation.

We use `runWithReplaceM` to run the same `triggerAndSample` function in two contexts:
1. As the initial computation (outside any frame) → trigger propagates, sample sees 42
2. As the replacement computation (inside triggerReplace's frame) → trigger enqueues, sample sees 0

### Why `runWithReplaceM`?

We can't just call `triggerAndSample.run env` from an IO callback because:
- IO callbacks don't have access to the outer SpiderEnv
- Creating a new SpiderEnv would give us a fresh, independent environment
- A fresh environment is NOT "inside" the outer frame—it has its own frame state

`runWithReplaceM` properly shares the SpiderEnv, so the replacement computation
runs within the triggering event's frame.
-/
test "same code behaves differently inside vs outside frame" := do
  let result ← runSpider do
    -- A function that creates a dynamic, fires a trigger, and samples.
    -- THE SAME FUNCTION will produce DIFFERENT results depending on context!
    let triggerAndSample : SpiderM Nat := do
      let (evt, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
      let dyn ← foldDyn (fun x acc => acc + x) 0 evt
      trigger 42
      sample dyn.current

    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- Run as initial computation (outside any frame)
    let (outsideFrame, resultEvent) ← SpiderM.runWithReplaceM triggerAndSample replaceEvent

    -- Collect replacement results
    let insideFrameRef ← SpiderM.liftIO <| IO.mkRef 0
    let _ ← resultEvent.subscribe fun n =>
      insideFrameRef.set n

    -- Run as replacement (inside triggerReplace's frame)
    triggerReplace triggerAndSample

    let insideFrame ← SpiderM.liftIO insideFrameRef.get

    pure (outsideFrame, insideFrame)

  -- Outside frame: trigger propagates synchronously, sample sees 42
  -- Inside frame: trigger enqueues for later, sample sees initial value 0
  --
  -- THE SAME FUNCTION produces DIFFERENT RESULTS based solely on whether
  -- it runs inside or outside an existing propagation frame!
  shouldBe result (42, 0)

/-!
## Test: Replacement Tears Down Old Network, New Network Works

This test verifies two important properties:

1. **Old network disposal**: When a replacement fires, the old computation's
   subscriptions are disposed. The old network no longer receives events.

2. **New network works**: The replacement computation's FRP infrastructure
   is correctly wired up and works normally after the frame completes.

This is the correct Reflex-style replacement semantic: the old network is
torn down, not left running alongside the new one.
-/
test "replacement tears down old network, new network works" := do
  let result ← runSpider do
    -- External event we'll use to test which dynamics are still active
    let (externalEvent, fireExternal) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Track values from ALL dynamics (to verify old one is disposed)
    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- Computation that creates a dynamic subscribing to the external event
    let createDynamicForExternal : Nat → SpiderM Nat := fun initialValue => do
      let dyn ← foldDyn (fun x acc => acc + x) initialValue externalEvent
      -- Subscribe to observe updates (this subscription will be disposed on replacement)
      let _ ← dyn.updated.subscribe fun val =>
        valuesRef.modify (· ++ [val])
      pure initialValue

    let (initial, _) ← SpiderM.runWithReplaceM (createDynamicForExternal 100) replaceEvent

    -- Trigger replacement - OLD network (100) is DISPOSED, new one (200) created
    triggerReplace (createDynamicForExternal 200)

    -- Fire external event - ONLY the replacement dynamic should receive it
    -- The initial dynamic's subscription was disposed when replacement fired
    fireExternal 50

    let values ← SpiderM.liftIO valuesRef.get
    pure (initial, values)

  -- initial = 100 (returned directly from initial computation)
  --
  -- After replacement:
  --   First dynamic (100): DISPOSED - its subscription no longer fires
  --   Second dynamic (200): ACTIVE - will receive events
  --
  -- After fireExternal 50:
  --   Only second dynamic receives it: 200 + 50 = 250
  --
  -- values = [250] (NOT [150, 250] - the old network was torn down!)
  shouldBe result (100, [250])

/-!
## Test: Full Replacement Lifecycle

This test exercises the complete replacement lifecycle with multiple replacements,
verifying at each stage that:
1. The current network receives events correctly
2. After replacement, the old network is disposed (no longer receives events)
3. The new network takes over and works correctly

We create a sequence: initial → replacement1 → replacement2, firing events
at each stage to verify proper disposal and handoff.
-/
test "full replacement lifecycle with multiple replacements" := do
  let result ← runSpider do
    -- External event that all networks will subscribe to
    let (externalEvent, fireExternal) ← newTriggerEvent (t := Spider) (a := Nat)

    -- Track ALL values received by ALL networks (with labels for debugging)
    let logRef ← SpiderM.liftIO <| IO.mkRef ([] : List (String × Nat))

    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM String)

    -- Factory that creates a labeled network
    let createNetwork : String → Nat → SpiderM String := fun label initialValue => do
      let dyn ← foldDyn (fun x acc => acc + x) initialValue externalEvent
      -- Subscribe to log all updates with this network's label
      let _ ← dyn.updated.subscribe fun val =>
        logRef.modify (· ++ [(label, val)])
      pure label

    -- === PHASE 1: Initial network "A" with initial value 100 ===
    let (initialLabel, _) ← SpiderM.runWithReplaceM (createNetwork "A" 100) replaceEvent

    -- Fire some events - network A should receive them
    fireExternal 10  -- A: 100 + 10 = 110
    fireExternal 20  -- A: 110 + 20 = 130

    -- === PHASE 2: Replace with network "B" (initial value 200) ===
    -- Network A should be disposed after this
    triggerReplace (createNetwork "B" 200)

    -- Fire events - ONLY network B should receive them (A is disposed)
    fireExternal 5   -- B: 200 + 5 = 205
    fireExternal 15  -- B: 205 + 15 = 220

    -- === PHASE 3: Replace with network "C" (initial value 300) ===
    -- Network B should be disposed after this
    triggerReplace (createNetwork "C" 300)

    -- Fire events - ONLY network C should receive them (A and B are disposed)
    fireExternal 1   -- C: 300 + 1 = 301
    fireExternal 2   -- C: 301 + 2 = 303
    fireExternal 3   -- C: 303 + 3 = 306

    let log ← SpiderM.liftIO logRef.get
    pure (initialLabel, log)

  -- Verify the complete event log:
  -- Phase 1: A receives events at 110, 130
  -- Phase 2: Only B receives events at 205, 220 (A is gone)
  -- Phase 3: Only C receives events at 301, 303, 306 (A and B are gone)
  let expectedLog := [
    ("A", 110), ("A", 130),           -- Phase 1: A active
    ("B", 205), ("B", 220),           -- Phase 2: B active, A disposed
    ("C", 301), ("C", 303), ("C", 306) -- Phase 3: C active, B disposed
  ]
  shouldBe result ("A", expectedLog)

/-!
## Test: Replacement Disposes Nested Infrastructure

This test verifies that replacement properly disposes deeply nested FRP
infrastructure, not just top-level subscriptions. The initial network creates
multiple interconnected dynamics and events; all should be cleaned up on
replacement.
-/
test "replacement disposes nested infrastructure" := do
  let result ← runSpider do
    let (externalEvent, fireExternal) ← newTriggerEvent (t := Spider) (a := Nat)
    let logRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let (replaceEvent, triggerReplace) ← newTriggerEvent (t := Spider) (a := SpiderM Nat)

    -- Create a network with nested/chained dynamics
    let createComplexNetwork : Nat → SpiderM Nat := fun multiplier => do
      -- First dynamic: accumulates raw values
      let dyn1 ← foldDyn (fun (x : Nat) (acc : Nat) => acc + x) 0 externalEvent

      -- Second dynamic: derived from first, applies multiplier
      let derived : Event Spider Nat ← Event.mapM (fun (v : Nat) => v * multiplier) dyn1.updated
      let dyn2 ← foldDyn (fun (x : Nat) (acc : Nat) => acc + x) 0 derived

      -- Subscribe to the derived dynamic's updates
      let _ ← dyn2.updated.subscribe fun val =>
        logRef.modify (· ++ [val])

      pure multiplier

    -- Initial network with multiplier 2
    let (initial, _) ← SpiderM.runWithReplaceM (createComplexNetwork 2) replaceEvent

    -- Fire events - derived values are multiplied by 2
    fireExternal 10  -- dyn1: 10, dyn2: 10*2 = 20
    fireExternal 20  -- dyn1: 30, dyn2: 20 + 30*2 = 80 (wait, that's not right...)

    -- Actually: dyn1.updated fires with NEW accumulated value
    -- So: fire 10 -> dyn1 becomes 10, dyn1.updated fires 10, derived fires 20, dyn2 becomes 20
    --     fire 20 -> dyn1 becomes 30, dyn1.updated fires 30, derived fires 60, dyn2 becomes 80

    -- Replace with multiplier 10 - old network (including dyn1, dyn2, derived) disposed
    triggerReplace (createComplexNetwork 10)

    -- Fire events - new network uses multiplier 10
    fireExternal 5   -- dyn1: 5, derived: 50, dyn2: 50

    let log ← SpiderM.liftIO logRef.get
    pure (initial, log)

  -- Old network: 20, 80 (multiplier 2)
  -- New network: 50 (multiplier 10)
  -- Old network's subscription is disposed, so we don't see its values after replacement
  shouldBe result (2, [20, 80, 50])


end ReactiveTests.FrameSemanticsTests
