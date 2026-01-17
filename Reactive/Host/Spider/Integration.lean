/-
  Reactive/Host/Spider/Integration.lean

  Integration helpers for the Spider FRP runtime.
-/
import Reactive.Host.Spider.Core
import Reactive.Host.Spider.Event

namespace Reactive.Host

/-! ## Adjustable Helpers

Additional combinators for higher-order FRP patterns. -/

/-- Run a computation that can request its own replacement.
    The computation returns both a result and an event carrying replacement computations.
    This is useful for self-replacing widgets or state machines.
    Subscription is registered with current scope. -/
def runWithReplaceRequester (computation : SpiderM (a × Event Spider (SpiderM a)))
    : SpiderM (a × Event Spider a) := ⟨fun env => do
  let (resultEvent, fireResult) ← Event.newTrigger env.timelineCtx

  -- Run the initial computation
  let (initialResult, selfReplaceEvent) ← computation.run env

  -- Subscribe to the replacement event
  let unsub ← Reactive.Event.subscribe selfReplaceEvent fun replacementM => do
    let newResult ← replacementM.run env
    fireResult newResult
  env.currentScope.register unsub

  pure (initialResult, resultEvent)
⟩

/-- Traverse a dynamic list, rebuilding results when the list changes.
    Returns a Dynamic of results that updates whenever the input list changes.

    Note: This rebuilds all results on each change. For incremental updates,
    a more sophisticated implementation would be needed.
    Subscription is registered with current scope. -/
def traverseDynList (f : a → SpiderM b) (dynList : Dynamic Spider (List a))
    : SpiderM (Dynamic Spider (List b)) := ⟨fun env => do
  -- Get initial list and compute initial results
  let initialList ← dynList.sample
  let initialResults ← initialList.mapM fun a => (f a).run env

  -- Create result dynamic
  let (resultDyn, updateResult) ← createDynamic env.timelineCtx initialResults

  -- Subscribe to list changes and rebuild results
  let unsub ← Reactive.Event.subscribe dynList.updated fun newList => do
    let newResults ← newList.mapM fun a => (f a).run env
    updateResult newResults
  env.currentScope.register unsub

  pure resultDyn
⟩

/-! ## Integration Helpers

Common patterns for integrating reactive networks with external systems. -/

/-- Create a poll-based event source.
    The provided IO action is polled repeatedly. When it returns `some value`,
    the event fires with that value. When it returns `none`, no event fires.

    Use with `runSpiderLoop` for continuous polling, or call the returned
    poll action manually.

    Example:
    ```
    -- Create event that fires when stdin has input
    let (inputEvent, pollInput) ← fromIO do
      if ← IO.getStdin.anyAvailable then
        some <$> IO.getStdin.getLine
      else
        pure none
    ```
-/
def fromIO (poll : IO (Option a)) : SpiderM (Event Spider a × IO Unit) := do
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let pollAction : IO Unit := do
    match ← poll with
    | some value => fire value
    | none => pure ()
  pure (event, pollAction)

/-- Export an event as a callback.
    Subscribes to the event and calls the provided callback whenever it fires.
    The subscription is registered with the current scope for automatic cleanup.

    Example:
    ```
    -- Forward clicks to an external system
    toCallback clickEvent fun pos =>
      ExternalUI.handleClick pos
    ```
-/
def toCallback (event : Event Spider a) (callback : a → IO Unit) : SpiderM Unit := do
  let _ ← Event.subscribeM event callback
  pure ()

/-- Run IO actions from an event and return an event of results.
    This is the core pattern for performing effects in response to FRP events.

    The IO action is executed synchronously when the source event fires,
    and the resulting event fires with the action's result.
    Subscription is registered with current scope for automatic cleanup.

    Example:
    ```
    -- Save to file whenever document changes
    let saveResults ← performEvent (saveEvent.map' fun doc => saveDocument doc)
    -- saveResults fires with the save success/failure after each save
    ```
-/
def performEvent (event : Event Spider (IO a)) : SpiderM (Event Spider a) := ⟨fun env => do
  let nodeId ← env.timelineCtx.freshNodeId
  let derived ← Event.newNodeWithId nodeId event.height.inc
  let unsub ← Reactive.Event.subscribe event fun action => do
    let result ← action
    derived.fire result
  env.currentScope.register unsub
  pure derived⟩

/-- Run IO actions from an event, discarding results.
    Use this when you only care about the side effect, not the result.
    Subscription is registered with current scope for automatic cleanup.

    Example:
    ```
    -- Log every button click
    let logEvents ← clickEvent.map' fun _ => IO.println "Button clicked!"
    performEvent_ logEvents
    ```
-/
def performEvent_ (event : Event Spider (IO Unit)) : SpiderM Unit := ⟨fun env => do
  let unsub ← Reactive.Event.subscribe event fun action => action
  env.currentScope.register unsub⟩

/-- Create an event from an IO.Ref.
    Returns an event that fires whenever the ref is modified, plus a function
    to trigger updates. The event fires with the new value after modification.

    Example:
    ```
    let (stateEvent, updateState) ← fromRef initialState
    -- Later:
    updateState (· + 1)  -- Fires stateEvent with new value
    ```
-/
def fromRef (initial : a) : SpiderM (Event Spider a × (a → IO Unit) × IO.Ref a) := do
  let ref ← SpiderM.liftIO <| IO.mkRef initial
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let update := fun newValue => do
    ref.set newValue
    fire newValue
  pure (event, update, ref)

/-- Create an event and behavior pair from a mutable ref.
    The behavior always samples the current ref value.
    The event fires when update is called.

    This is similar to `holdDyn` but gives you direct control over when
    updates happen via the returned update function.
-/
def fromRefWithBehavior (initial : a) : SpiderM (Event Spider a × Behavior Spider a × (a → IO Unit)) := do
  let ref ← SpiderM.liftIO <| IO.mkRef initial
  let (event, fire) ← newTriggerEvent (t := Spider) (a := a)
  let behavior := Behavior.fromSample ref.get
  let update := fun newValue => do
    ref.set newValue
    fire newValue
  pure (event, behavior, update)

/-- Run a Spider network and return the result -/
def runSpider (network : SpiderM a) : IO a :=
  SpiderM.runFresh network

/-- Run a Spider network with a custom error handler.
    - `defaultErrorHandler`: logs errors and continues (default for runSpider)
    - `strictErrorHandler`: re-raises first error, stopping propagation -/
def runSpiderWithErrorHandler (network : SpiderM a)
    (errorHandler : PropagationErrorHandler) : IO a :=
  SpiderM.runFresh network errorHandler

/-- Run a Spider network with an event loop.

    The eventSource function is called repeatedly to get external events.
    It should return:
    - `some action` to fire an event (action is the trigger)
    - `none` when there are no more events

    The loop runs until shouldQuit returns true. -/
partial def runSpiderLoop (network : SpiderM a) (eventSource : IO (Option (IO Unit)))
    (shouldQuit : IO Bool) : IO a := do
  let result ← runSpider network

  -- Simple event loop
  let rec loop : IO Unit := do
    if ← shouldQuit then
      pure ()
    else
      match ← eventSource with
      | some action => do
          action
          loop
      | none => do
          -- Small delay to avoid busy-waiting
          IO.sleep 10
          loop

  loop
  pure result

end Reactive.Host
