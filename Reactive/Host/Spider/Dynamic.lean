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

/-- Bind/flatMap for Dyn (Option a).
    When the outer dynamic is `none`, the result holds the default value.
    When the outer dynamic is `some v`, the result tracks the dynamic produced by `f v`.

    This is the general form that avoids intermediate `Dyn (Option (Dyn a))`.

    Example:
    ```
    -- Switch to streaming content when request is active, else show placeholder
    let display ← Dynamic.bindOptionM requestDyn (·.contentDyn) "Loading..."
    ``` -/
def bindOptionM (d : Dynamic Spider (Option a)) (f : a → Dynamic Spider b) (default : b)
    : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.bindOptionM"
  let initialOpt ← d.sample
  let initialValue ← match initialOpt with
    | some v => (f v).sample
    | none => pure default
  let (result, updateResult) ← createDynamic env.timelineCtx initialValue
  let currentUnsubRef ← IO.mkRef (pure () : IO Unit)

  let subscribeToInner := fun (inner : Dynamic Spider b) => do
    let oldUnsub ← currentUnsubRef.get
    oldUnsub
    let unsub ← Reactive.Event.subscribe inner.updated fun newValue => updateResult newValue
    currentUnsubRef.set unsub
    let currentValue ← inner.sample
    updateResult currentValue

  -- Initial subscription if some
  match initialOpt with
  | some v =>
    let inner := f v
    let unsubInner ← Reactive.Event.subscribe inner.updated fun newValue => updateResult newValue
    currentUnsubRef.set unsubInner
  | none => pure ()

  -- Outer subscription
  let unsubOuter ← Reactive.Event.subscribe d.updated fun opt => do
    match opt with
    | some v => subscribeToInner (f v)
    | none =>
      let oldUnsub ← currentUnsubRef.get
      oldUnsub
      currentUnsubRef.set (pure ())
      updateResult default

  env.currentScope.register unsubOuter
  env.currentScope.register do
    let unsub ← currentUnsubRef.get
    unsub

  env.decrementDepth
  pure result⟩

/-- Switch/join a Dynamic of Optional Dynamics.
    When the outer is `none`, the result holds the default value.
    When the outer is `some inner`, the result tracks the inner dynamic.

    Example:
    ```
    -- Track selected item's details, or show placeholder
    let details ← Dynamic.switchOptionM selectedItemDyn defaultDetails
    ``` -/
def switchOptionM (dd : Dynamic Spider (Option (Dynamic Spider a))) (default : a)
    : SpiderM (Dynamic Spider a) :=
  bindOptionM dd id default

/-- Bind/flatMap for optional dynamics (fluent style).
    Enables: `optDyn.bindOption' default f` -/
def bindOption' (d : Dynamic Spider (Option a)) (default : b) (f : a → Dynamic Spider b)
    : SpiderM (Dynamic Spider b) :=
  bindOptionM d f default

/-- Switch optional nested dynamic (fluent style).
    Enables: `optDynOfDyn.switchOption' default` -/
def switchOption' (dd : Dynamic Spider (Option (Dynamic Spider a))) (default : a)
    : SpiderM (Dynamic Spider a) :=
  switchOptionM dd default

/-- Deduplicate a Dynamic's updates using a custom comparison function.
    Only fires when `eq oldVal newVal` returns false.

    Example:
    ```
    -- Only fire when the name field changes
    let uniqByName ← Dynamic.uniqByM (fun a b => a.name == b.name) userDyn
    ``` -/
def uniqByM (eq : a → a → Bool) (d : Dynamic Spider a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.uniqByM"
  let initial ← d.sample
  let currentRef ← IO.mkRef initial
  let (result, updateResult) ← createDynamic env.timelineCtx initial
  let unsub ← Reactive.Event.subscribe d.updated fun newVal => do
    let current ← currentRef.get
    if !eq current newVal then
      currentRef.set newVal
      updateResult newVal
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Deduplicate using custom comparison (fluent style).
    Enables: `dynamic.uniqBy' eqFn` -/
def uniqBy' (d : Dynamic Spider a) (eq : a → a → Bool) : SpiderM (Dynamic Spider a) :=
  uniqByM eq d

/-- Fold over events but only update state when the function returns Some.
    This is useful when not every event should update the state.

    Example:
    ```
    -- Only count positive numbers
    let positiveCount ← foldDynMaybeM
      (fun n count => if n > 0 then some (count + 1) else none)
      0 numberEvent
    ``` -/
def foldDynMaybeM (f : a → b → Option b) (initial : b) (event : Event Spider a)
    : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.foldDynMaybeM"
  let currentRef ← IO.mkRef initial
  let (result, updateResult) ← createDynamic env.timelineCtx initial
  let unsub ← Reactive.Event.subscribe event fun a => do
    let current ← currentRef.get
    match f a current with
    | some newVal =>
      currentRef.set newVal
      updateResult newVal
    | none => pure ()
  env.currentScope.register unsub
  env.decrementDepth
  pure result⟩

/-- Fold with conditional updates (fluent style).
    Enables: `event.foldDynMaybe' f initial` -/
def foldDynMaybe' (event : Event Spider a) (f : a → b → Option b) (initial : b)
    : SpiderM (Dynamic Spider b) :=
  foldDynMaybeM f initial event

/-! ### Debugging Combinators -/

/-- Debug logging for Dynamic value changes. Prints each change with a label.
    Useful for debugging reactive networks.

    Example:
    ```
    let debuggedCounter ← Dynamic.traceM "counter" counterDyn
    -- Prints: [counter] <value> for each change
    ``` -/
def traceM (label : String) (d : Dynamic Spider a) [ToString a] : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.traceM"
  let initial ← d.sample
  IO.println s!"[{label}] initial: {initial}"
  let unsub ← Reactive.Event.subscribe d.updated fun newVal =>
    IO.println s!"[{label}] {newVal}"
  env.currentScope.register unsub
  env.decrementDepth
  pure d⟩

/-- Debug logging with custom formatter.

    Example:
    ```
    let debugged ← Dynamic.traceWithM "user" (fun u => u.name) userDyn
    ``` -/
def traceWithM (label : String) (f : a → String) (d : Dynamic Spider a) : SpiderM (Dynamic Spider a) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.traceWithM"
  let initial ← d.sample
  IO.println s!"[{label}] initial: {f initial}"
  let unsub ← Reactive.Event.subscribe d.updated fun newVal =>
    IO.println s!"[{label}] {f newVal}"
  env.currentScope.register unsub
  env.decrementDepth
  pure d⟩

/-- Trace Dynamic changes (fluent style).
    Enables: `dynamic.trace' "label"` -/
def trace' (d : Dynamic Spider a) (label : String) [ToString a] : SpiderM (Dynamic Spider a) :=
  traceM label d

/-- Trace with custom formatter (fluent style).
    Enables: `dynamic.traceWith' "label" formatter` -/
def traceWith' (d : Dynamic Spider a) (label : String) (f : a → String) : SpiderM (Dynamic Spider a) :=
  traceWithM label f d

/-! ### Memoization Combinators -/

/-- Memoize a mapped computation over a Dynamic.
    Caches the result of `f` and only recomputes when the input value changes.
    Uses BEq on the input type to detect when recomputation is needed.

    This is useful when `f` is expensive and you want to avoid redundant
    recomputation when the Dynamic's value hasn't changed.

    Unlike `mapUniqM` which deduplicates by output, this deduplicates by input,
    avoiding the computation entirely when the input is unchanged.

    Example:
    ```
    -- Expensive parsing only when text changes
    let parsedDyn ← Dynamic.memoizeM parseDocument textDyn
    ``` -/
def memoizeM [BEq a] (f : a → b) (da : Dynamic Spider a) : SpiderM (Dynamic Spider b) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.memoizeM"
  let initial ← da.sample
  let initialResult := f initial

  -- Cache both input and output
  let cachedInputRef ← IO.mkRef initial
  let valueRef ← IO.mkRef initialResult
  let (changeEvent, trigger) ← Event.newTrigger env.timelineCtx

  let unsub ← Reactive.Event.subscribe da.updated fun newInput => do
    let cachedInput ← cachedInputRef.get
    if newInput != cachedInput then
      -- Input changed, recompute
      let newResult := f newInput
      cachedInputRef.set newInput
      valueRef.set newResult
      trigger newResult
    -- else: input unchanged, skip computation and don't fire

  env.currentScope.register unsub
  env.decrementDepth
  pure ⟨valueRef, changeEvent, trigger⟩⟩

/-- Memoize a computation (fluent style).
    Enables: `dynamic.memoize' expensiveComputation` -/
def memoize' [BEq a] (da : Dynamic Spider a) (f : a → b) : SpiderM (Dynamic Spider b) :=
  memoizeM f da

/-! ### Collection Combinators -/

/-- Convert a List of Dynamics into a Dynamic of List.
    The resulting dynamic updates whenever any input dynamic changes.

    Essential for working with dynamic collections where each element is reactive.

    Example:
    ```
    let dynamics : List (Dynamic Spider Nat) := [counterA, counterB, counterC]
    let allCounters ← Dynamic.sequenceM dynamics
    -- allCounters : Dynamic Spider (List Nat)
    -- Updates whenever any counter changes
    ``` -/
def sequenceM (dynamics : List (Dynamic Spider a)) : SpiderM (Dynamic Spider (List a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.sequenceM"
  -- Sample all initial values
  let initial ← dynamics.mapM (·.sample)
  let (result, updateResult) ← createDynamic env.timelineCtx initial

  -- Helper to resample all dynamics and update the result
  let resampleAll : IO Unit := do
    let values ← dynamics.mapM (·.sample)
    updateResult values

  -- Subscribe to each dynamic's updates
  for d in dynamics do
    let unsub ← Reactive.Event.subscribe d.updated fun _ => resampleAll
    env.currentScope.register unsub

  env.decrementDepth
  pure result⟩

/-- Convert an Array of Dynamics into a Dynamic of Array.
    The resulting dynamic updates whenever any input dynamic changes. -/
def sequenceArrayM (dynamics : Array (Dynamic Spider a)) : SpiderM (Dynamic Spider (Array a)) := ⟨fun env => do
  let _ ← env.incrementDepth "Dynamic.sequenceArrayM"
  -- Sample all initial values
  let initial ← dynamics.mapM (·.sample)
  let (result, updateResult) ← createDynamic env.timelineCtx initial

  -- Helper to resample all dynamics and update the result
  let resampleAll : IO Unit := do
    let values ← dynamics.mapM (·.sample)
    updateResult values

  -- Subscribe to each dynamic's updates
  for d in dynamics do
    let unsub ← Reactive.Event.subscribe d.updated fun _ => resampleAll
    env.currentScope.register unsub

  env.decrementDepth
  pure result⟩

/-- Traverse a List with a Dynamic-producing function, collecting the results
    into a Dynamic of List.

    Equivalent to `Dynamic.sequenceM (values.map f)`. -/
def traverseM (f : a → Dynamic Spider b) (values : List a) : SpiderM (Dynamic Spider (List b)) :=
  sequenceM (values.map f)

end Dynamic

end Reactive.Host
