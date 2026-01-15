/-
  Reactive/Core/Scope.lean

  Subscription scope for automatic cleanup of subscriptions.
  Scopes form a tree: disposing a parent disposes all children.
-/

namespace Reactive

/-- A subscription scope that tracks subscriptions for automatic cleanup.
    Scopes form a tree: disposing a parent disposes all children.
    When a scope is disposed, all registered unsubscribe actions are called.

    Implementation note: Children are tracked as disposal callbacks,
    avoiding recursive type issues. -/
structure SubscriptionScope where
  private mk ::
  /-- Registered unsubscribe actions (includes child disposal) -/
  private subscriptions : IO.Ref (Array (IO Unit))
  /-- Whether this scope has been disposed -/
  private disposed : IO.Ref Bool

namespace SubscriptionScope

/-- Create a new root subscription scope -/
def new : IO SubscriptionScope := do
  let subscriptions ← IO.mkRef #[]
  let disposed ← IO.mkRef false
  pure ⟨subscriptions, disposed⟩

/-- Create a child scope. The child is automatically disposed when the parent is disposed. -/
def child (parent : SubscriptionScope) : IO SubscriptionScope := do
  let isDisposed ← parent.disposed.get
  if isDisposed then
    -- Parent already disposed, return a pre-disposed scope
    let subscriptions ← IO.mkRef #[]
    let disposed ← IO.mkRef true
    pure ⟨subscriptions, disposed⟩
  else
    -- Create child scope
    let childSubscriptions ← IO.mkRef #[]
    let childDisposed ← IO.mkRef false
    let childScope : SubscriptionScope := ⟨childSubscriptions, childDisposed⟩

    -- Register child's disposal with parent
    -- We use a helper to dispose child when parent disposes
    let disposeChild : IO Unit := do
      -- Only dispose if not already disposed
      let alreadyDisposed ← childDisposed.modifyGet fun d => (d, true)
      if !alreadyDisposed then
        let subs ← childSubscriptions.modifyGet fun s => (s, #[])
        for unsub in subs do
          unsub

    -- Add child disposal to parent's subscriptions
    -- Using push (O(1)) instead of prepend (O(n)) for performance
    -- Order doesn't affect correctness - all subscriptions get disposed regardless
    parent.subscriptions.modify (·.push disposeChild)

    pure childScope

/-- Register an unsubscribe action with this scope.
    The action will be called when the scope is disposed.
    If the scope is already disposed, the action is called immediately. -/
def register (scope : SubscriptionScope) (unsubscribe : IO Unit) : IO Unit := do
  let isDisposed ← scope.disposed.get
  if isDisposed then
    -- Already disposed, run cleanup immediately
    unsubscribe
  else
    scope.subscriptions.modify (·.push unsubscribe)

/-- Dispose this scope and all children, running all unsubscribe actions.
    Safe to call multiple times (no-op after first call). -/
def dispose (scope : SubscriptionScope) : IO Unit := do
  let alreadyDisposed ← scope.disposed.modifyGet fun d => (d, true)
  if alreadyDisposed then
    return ()

  -- Get all subscriptions and clear
  let subscriptions ← scope.subscriptions.modifyGet fun subs => (subs, #[])

  -- Run all unsubscribe actions
  for unsub in subscriptions do
    unsub

/-- Check if scope has been disposed -/
def isDisposed (scope : SubscriptionScope) : IO Bool :=
  scope.disposed.get

/-- Get the number of registered subscriptions (for testing) -/
def subscriptionCount (scope : SubscriptionScope) : IO Nat := do
  let subs ← scope.subscriptions.get
  pure subs.size

/-- Check if the scope has no registered subscriptions.
    More efficient than `subscriptionCount == 0` for auto-detection. -/
def isEmpty (scope : SubscriptionScope) : IO Bool := do
  let subs ← scope.subscriptions.get
  pure subs.isEmpty

/-- Clear all subscriptions from this scope, running their cleanup actions,
    but keep the scope alive for reuse. Unlike dispose, this allows the scope
    to be reused for new subscriptions without creating a new child scope
    (which would leak entries in the parent's subscriptions array). -/
def clear (scope : SubscriptionScope) : IO Unit := do
  let isDisposed ← scope.disposed.get
  if isDisposed then
    return ()
  -- Get all subscriptions and clear
  let subscriptions ← scope.subscriptions.modifyGet fun subs => (subs, #[])
  -- Run all unsubscribe actions
  for unsub in subscriptions do
    unsub

end SubscriptionScope

end Reactive
