/-
  Reactive/Core/AsyncState.lean

  AsyncState type for representing async resource loading states.
  Used by async combinators to track pending/loading/ready/error states.
-/

namespace Reactive

/-- Represents the state of an async resource.
    - `pending`: Initial state, operation not yet started
    - `loading`: Operation is in progress
    - `ready`: Operation completed successfully with a value
    - `error`: Operation failed with an error -/
inductive AsyncState (e a : Type) where
  | pending
  | loading
  | ready : a → AsyncState e a
  | error : e → AsyncState e a
  deriving Repr, BEq

namespace AsyncState

instance : Inhabited (AsyncState e a) where
  default := .pending

/-- Map a function over the success value -/
def map (f : a → b) : AsyncState e a → AsyncState e b
  | .pending => .pending
  | .loading => .loading
  | .ready v => .ready (f v)
  | .error err => .error err

/-- Map a function over the error value -/
def mapError (f : e → e') : AsyncState e a → AsyncState e' a
  | .pending => .pending
  | .loading => .loading
  | .ready v => .ready v
  | .error err => .error (f err)

/-- Flat map over the success value -/
def bind (f : a → AsyncState e b) : AsyncState e a → AsyncState e b
  | .pending => .pending
  | .loading => .loading
  | .ready v => f v
  | .error err => .error err

instance : Functor (AsyncState e) where
  map := map

instance : Monad (AsyncState e) where
  pure := .ready
  bind x f := x.bind f

/-- Extract the success value if ready, otherwise none -/
def toOption : AsyncState e a → Option a
  | .ready v => some v
  | _ => none

/-- Extract the error if present, otherwise none -/
def toError : AsyncState e a → Option e
  | .error err => some err
  | _ => none

/-- Check if the state is pending -/
def isPending : AsyncState e a → Bool
  | .pending => true
  | _ => false

/-- Check if the state is loading -/
def isLoading : AsyncState e a → Bool
  | .loading => true
  | _ => false

/-- Check if the state is ready with a value -/
def isReady : AsyncState e a → Bool
  | .ready _ => true
  | _ => false

/-- Check if the state is an error -/
def isError : AsyncState e a → Bool
  | .error _ => true
  | _ => false

/-- Check if the state is terminal (ready or error) -/
def isTerminal : AsyncState e a → Bool
  | .ready _ => true
  | .error _ => true
  | _ => false

/-- Get the success value or a default -/
def getOrElse (default : a) : AsyncState e a → a
  | .ready v => v
  | _ => default

/-- Convert to Except if terminal, otherwise none -/
def toExcept : AsyncState e a → Option (Except e a)
  | .ready v => some (.ok v)
  | .error err => some (.error err)
  | _ => none

end AsyncState

end Reactive
