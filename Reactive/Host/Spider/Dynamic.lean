/-
  Reactive/Host/Spider/Dynamic.lean

  Dynamic SpiderM combinators for the Spider FRP runtime.
-/
import Reactive.Host.Spider.Core

namespace Reactive.Host

/-! ## Dynamic SpiderM Combinators

These provide ergonomic versions of Dynamic operations that auto-allocate NodeIds
and register subscriptions with the current scope for automatic cleanup.

Note: These use the existing IO-based Dynamic functions and wrap them to track
subscriptions. The subscriptions created internally by Dynamic.map etc. are
registered with the scope via a post-creation subscription to the updated event. -/

namespace Dynamic

/-- Map a function over a Dynamic, auto-allocating NodeId and registering with scope.
    Fires on every source update (no deduplication). Use `mapUniqM` for deduplication. -/
def mapM (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.mapM"
  let nodeId ← env.timelineCtx.freshNodeId
  -- Use raw map (no deduplication) - matches Reflex FRP semantics
  let result ← Dynamic.mapWithIdRaw f da nodeId
  -- Register a subscription to the source's updated event
  -- This tracks the subscription for cleanup
  let unsub ← Reactive.Event.subscribe da.updated fun _ => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Map a function over a Dynamic with deduplication.
    Only fires the change event when the mapped value actually changes.
    Use this when you want to avoid redundant updates. -/
def mapUniqM [BEq b] (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.mapUniqM"
  let nodeId ← env.timelineCtx.freshNodeId
  -- Use deduplicating map
  let result ← Dynamic.mapWithId f da nodeId
  -- Register a subscription to the source's updated event
  -- This tracks the subscription for cleanup
  let unsub ← Reactive.Event.subscribe da.updated fun _ => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Combine two Dynamics with a function, auto-allocating NodeId and registering with scope.
    Only fires the change event when the combined value actually changes. -/
def zipWithM [BEq c] (f : a → b → c) (da : Dynamic Spider a) (db : Dynamic Spider b)
    : SpiderM (Dynamic Spider c) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.zipWithM"
  let nodeId ← env.timelineCtx.freshNodeId
  -- Use existing IO-based zipWith
  let result ← Dynamic.zipWithId f da db nodeId
  -- Register subscriptions to track both sources
  let unsub1 ← Reactive.Event.subscribe da.updated fun _ => pure ()
  let unsub2 ← Reactive.Event.subscribe db.updated fun _ => pure ()
  env.currentScope.register unsub1
  env.currentScope.register unsub2
  env.decrementDepth
  pure result⟩

/-- Combine three Dynamics with a function, auto-allocating NodeIds and registering with scope.
    Only fires the change event when the combined value actually changes. -/
def zipWith3M [BEq a] [BEq b] [BEq d] (f : a → b → c → d)
    (da : Dynamic Spider a) (db : Dynamic Spider b) (dc : Dynamic Spider c)
    : SpiderM (Dynamic Spider d) := do
  -- Implemented using zipWithM which already handles scope registration
  let ab ← Dynamic.zipWithM Prod.mk da db
  Dynamic.zipWithM (fun (a, b) c => f a b c) ab dc

/-- Create a constant Dynamic that never changes.
    Uses TimelineCtx for type-safe event creation. -/
def pureM (x : a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  Dynamic.constant env.timelineCtx x⟩

/-- Applicative apply for Dynamics, auto-allocating NodeId and registering with scope. -/
def apM [BEq b] (df : Dynamic Spider (a → b)) (da : Dynamic Spider a)
    : SpiderM (Dynamic Spider b) :=
  Dynamic.zipWithM (fun f a => f a) df da

/-! ### Fluent Chainable Combinators for Dynamic

Extension methods enabling dot-notation chaining:
```lean
dynA.map' f |>.zipWith' g dynB
-- or with bind
dynA.map' f >>= (·.zipWith' g dynB)
``` -/

/-- Map a function over a Dynamic (fluent style).
    Enables: `dynamic.map' f` -/
def map' (da : Dynamic Spider a) (f : a → b) : SpiderM (Dynamic Spider b) :=
  mapM f da

/-- Map a function over a Dynamic with deduplication (fluent style).
    Enables: `dynamic.mapUniq' f` -/
def mapUniq' [BEq b] (da : Dynamic Spider a) (f : a → b) : SpiderM (Dynamic Spider b) :=
  mapUniqM f da

/-- Combine with another Dynamic (fluent style).
    Enables: `dynA.zipWith' f dynB` -/
def zipWith' [BEq c] (da : Dynamic Spider a) (f : a → b → c) (db : Dynamic Spider b)
    : SpiderM (Dynamic Spider c) :=
  zipWithM f da db

/-- Pair with another Dynamic (fluent style).
    Enables: `dynA.zip' dynB` -/
def zip' [BEq a] [BEq b] (da : Dynamic Spider a) (db : Dynamic Spider b) : SpiderM (Dynamic Spider (a × b)) :=
  zipWithM Prod.mk da db

/-- Combine with two other Dynamics (fluent style).
    Enables: `dynA.zipWith3' f dynB dynC` -/
def zipWith3' [BEq a] [BEq b] [BEq d] (da : Dynamic Spider a) (f : a → b → c → d)
    (db : Dynamic Spider b) (dc : Dynamic Spider c) : SpiderM (Dynamic Spider d) :=
  zipWith3M f da db dc

/-- Apply a Dynamic function (fluent style).
    Enables: `dynF.ap' dynA` -/
def ap' [BEq b] (df : Dynamic Spider (a → b)) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) :=
  apM df da

/-- Get an event that fires with (oldValue, newValue) pairs on each change.
    Auto-allocates NodeId and registers subscription with current scope.

    This is useful for detecting changes in a Dynamic's value, e.g., to
    determine when a component gains or loses focus.

    Example:
    ```
    let focusChanges ← Dynamic.changesM focusDynamic
    let gainFocus ← Event.filterM (fun (old, new) => !old && new) focusChanges
    ```
-/
def changesM (d : Dynamic Spider a) : SpiderM (Event Spider (a × a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.changesM"
  let result ← Dynamic.changesId d (← env.timelineCtx.freshNodeId)
  env.decrementDepth
  pure result⟩

/-- Deduplicate a Dynamic's updates.
    Only fires when the value actually changes.
    Subscribes within the current scope for cleanup. -/
def holdUniqDynM [BEq a] (d : Dynamic Spider a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.holdUniqDynM"
  let initial ← d.sample
  let currentRef ← IO.mkRef initial
  let (result, updateResult) ← createDynamic env.timelineCtx initial
  let unsub ← Reactive.Event.subscribe d.updated fun newVal => do
    let current ← currentRef.get
    if newVal != current then
      currentRef.set newVal
      updateResult newVal
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Switch/join a Dynamic of Dynamics into a single Dynamic.
    The result updates when either the outer changes or the current inner changes.
    All subscriptions are registered with the current scope. -/
def switchM (dd : Dynamic Spider (Dynamic Spider a)) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.switchM"
  let initialInner ← dd.sample
  let initialValue ← initialInner.sample
  let (result, updateResult) ← createDynamic env.timelineCtx initialValue
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  let subscribeToInner := fun (inner : Dynamic Spider a) => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe inner.updated fun newValue => updateResult newValue
    currentUnsubRef.set unsub
    let currentValue ← inner.sample
    updateResult currentValue

  let unsubInner ← Reactive.Event.subscribe initialInner.updated fun newValue => updateResult newValue
  currentUnsubRef.set unsubInner

  let unsubOuter ← Reactive.Event.subscribe dd.updated subscribeToInner

  -- Register both the outer subscription and a cleanup for the current inner
  env.currentScope.register unsubOuter
  env.currentScope.register do
    let unsub ← currentUnsubRef.get
    unsub

  env.decrementDepth
  pure result⟩

/-- Switch a Dynamic of Dynamics (fluent style).
    Enables: `dynOfDyn.switch'` -/
def switch' (dd : Dynamic Spider (Dynamic Spider a)) : SpiderM (Dynamic Spider a) :=
  switchM dd

end Dynamic

end Reactive.Host
