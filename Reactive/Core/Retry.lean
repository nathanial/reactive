/-
  Reactive/Core/Retry.lean

  Pure retry state machine for exponential backoff retry logic.
  Used by async combinators that need retry behavior.
-/

namespace Reactive

/-- Configuration for retry behavior -/
structure RetryConfig where
  /-- Maximum number of retries before giving up (0 means no retries) -/
  maxRetries : Nat := 3
  /-- Base delay in milliseconds for exponential backoff -/
  baseDelayMs : Nat := 1000
  /-- Maximum delay cap in milliseconds -/
  maxDelayMs : Nat := 30000
  deriving Repr, BEq, Inhabited

namespace RetryConfig

/-- Default retry configuration: 3 retries, 1s base, 30s max -/
def default : RetryConfig := {}

/-- No retries - fail immediately -/
def noRetry : RetryConfig := { maxRetries := 0 }

/-- Fast retry for tests: 3 retries, 10ms base, 100ms max -/
def fast : RetryConfig := { maxRetries := 3, baseDelayMs := 10, maxDelayMs := 100 }

end RetryConfig

/-- State of a retry operation -/
structure RetryState where
  /-- Number of retries attempted so far -/
  retryCount : Nat := 0
  /-- Timestamp of last attempt in milliseconds (monotonic) -/
  lastAttemptTime : Nat := 0
  /-- Error message from last failed attempt -/
  lastError : Option String := none
  deriving Repr, BEq, Inhabited

namespace RetryState

/-- Initial state with no retries -/
def initial : RetryState := {}

/-- Calculate backoff delay in milliseconds for current retry count.
    Uses exponential backoff: baseDelay * 2^retryCount, capped at maxDelay. -/
def backoffDelayMs (config : RetryConfig) (s : RetryState) : Nat :=
  let exponentialDelay := config.baseDelayMs * (2 ^ s.retryCount)
  min exponentialDelay config.maxDelayMs

/-- Check if retries are exhausted based on config -/
def isExhausted (config : RetryConfig) (s : RetryState) : Bool :=
  s.retryCount >= config.maxRetries

/-- Check if more retries are available -/
def canRetry (config : RetryConfig) (s : RetryState) : Bool :=
  s.retryCount < config.maxRetries

/-- Create initial failure state from first error -/
def initialFailure (now : Nat) (msg : String) : RetryState :=
  { retryCount := 0
  , lastAttemptTime := now
  , lastError := some msg }

/-- Record a retry failure, incrementing retry count -/
def recordRetryFailure (s : RetryState) (now : Nat) (msg : String) : RetryState :=
  { retryCount := s.retryCount + 1
  , lastAttemptTime := now
  , lastError := some msg }

/-- Record a successful retry, clearing error state -/
def recordSuccess (s : RetryState) (now : Nat) : RetryState :=
  { s with lastAttemptTime := now, lastError := none }

/-- Get a human-readable description of retry status -/
def describe (config : RetryConfig) (s : RetryState) : String :=
  if s.isExhausted config then
    s!"Exhausted after {s.retryCount} retries"
  else if s.retryCount == 0 then
    "Initial attempt"
  else
    s!"Retry {s.retryCount}/{config.maxRetries}"

end RetryState

end Reactive
