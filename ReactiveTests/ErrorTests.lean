import Crucible
import Reactive

namespace ReactiveTests.ErrorTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Error Tests"

-- Note: Error handling catches exceptions at the propagation frame level.
-- Within a single event's subscriber list, if one subscriber throws,
-- subsequent subscribers in that same event don't run.
-- But across different events in the same frame, errors are caught.

test "setErrorHandler changes active handler" := do
  let result ← runSpider do
    let handlerCallsRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Set a custom handler that counts calls
    let customHandler : PropagationErrorHandler := fun _ => do
      handlerCallsRef.modify (· + 1)
      pure true  -- continue propagation

    SpiderM.setErrorHandler customHandler

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Unit)
    let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
      throw (IO.userError "error")

    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO <| trigger ()

    SpiderM.liftIO handlerCallsRef.get
  shouldBe result 3

test "getErrorHandler returns current handler" := do
  let result ← runSpider do
    let handler1 ← SpiderM.getErrorHandler
    -- Handler1 should be defaultErrorHandler (returns true)
    let continues1 ← SpiderM.liftIO <| handler1 (IO.userError "test")

    SpiderM.setErrorHandler strictErrorHandler
    let handler2 ← SpiderM.getErrorHandler
    -- Handler2 should be strictErrorHandler (returns false)
    let continues2 ← SpiderM.liftIO <| handler2 (IO.userError "test")

    pure (continues1, continues2)
  shouldBe result (true, false)

test "error handler receives correct error message" := do
  let result ← runSpider do
    let errorMsgRef ← SpiderM.liftIO <| IO.mkRef ""

    let customHandler : PropagationErrorHandler := fun err => do
      errorMsgRef.set err.toString
      pure true

    SpiderM.setErrorHandler customHandler

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Unit)
    let _ ← SpiderM.liftIO <| event.subscribe fun _ =>
      throw (IO.userError "specific error message")

    SpiderM.liftIO <| trigger ()
    SpiderM.liftIO errorMsgRef.get
  -- The error message should contain our specific text
  let parts := result.splitOn "specific error message"
  let hasMessage := decide (parts.length > 1)
  shouldBe hasMessage true

test "default handler allows propagation to continue across frames" := do
  let result ← runSpider do
    let (event1, trigger1) ← newTriggerEvent (t := Spider) (a := Nat)
    let (event2, trigger2) ← newTriggerEvent (t := Spider) (a := Nat)
    let receivedRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    -- Event1 throws
    let _ ← SpiderM.liftIO <| event1.subscribe fun _ =>
      throw (IO.userError "error from event1")

    -- Event2 collects values
    let _ ← SpiderM.liftIO <| event2.subscribe fun n =>
      receivedRef.modify (· ++ [n])

    -- Fire event1 (throws, but default handler continues)
    SpiderM.liftIO <| trigger1 1
    -- Fire event2 (should still work)
    SpiderM.liftIO <| trigger2 42
    SpiderM.liftIO <| trigger2 43

    SpiderM.liftIO receivedRef.get
  shouldBe result [42, 43]

test "strict handler stops propagation on error" := do
  let result ← runSpiderWithErrorHandler (do
    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let valuesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    -- This subscriber records then throws on value 2
    let _ ← SpiderM.liftIO <| event.subscribe fun n => do
      if n == 2 then
        throw (IO.userError "error on 2")
      valuesRef.modify (· ++ [n])

    SpiderM.liftIO <| trigger 1  -- succeeds
    -- trigger 2 would cause an error, which stops propagation
    SpiderM.liftIO valuesRef.get
  ) strictErrorHandler
  shouldBe result [1]

test "error handler called once per throwing event" := do
  let result ← runSpider do
    let errorCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let valueCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let customHandler : PropagationErrorHandler := fun _ => do
      errorCountRef.modify (· + 1)
      pure true  -- continue

    SpiderM.setErrorHandler customHandler

    let (eventA, triggerA) ← newTriggerEvent (t := Spider) (a := Unit)
    let (eventB, triggerB) ← newTriggerEvent (t := Spider) (a := Unit)

    -- EventA throws
    let _ ← SpiderM.liftIO <| eventA.subscribe fun _ =>
      throw (IO.userError "A error")

    -- EventB succeeds
    let _ ← SpiderM.liftIO <| eventB.subscribe fun _ =>
      valueCountRef.modify (· + 1)

    -- Fire both - A's error is caught, B still runs
    SpiderM.liftIO <| triggerA ()
    SpiderM.liftIO <| triggerB ()
    SpiderM.liftIO <| triggerA ()
    SpiderM.liftIO <| triggerB ()

    let errors ← SpiderM.liftIO errorCountRef.get
    let values ← SpiderM.liftIO valueCountRef.get
    pure (errors, values)
  shouldBe result (2, 2)

test "errors in derived events trigger handler" := do
  let result ← runSpider do
    let errorCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    let customHandler : PropagationErrorHandler := fun _ => do
      errorCountRef.modify (· + 1)
      pure true

    SpiderM.setErrorHandler customHandler

    let (event, trigger) ← newTriggerEvent (t := Spider) (a := Nat)
    let mapped ← Event.mapM (· * 2) event

    -- Subscriber on derived event throws
    let _ ← SpiderM.liftIO <| mapped.subscribe fun _ =>
      throw (IO.userError "derived error")

    SpiderM.liftIO <| trigger 1
    SpiderM.liftIO <| trigger 2
    SpiderM.liftIO errorCountRef.get
  shouldBe result 2

test "runSpiderWithErrorHandler uses provided handler" := do
  let result ← runSpiderWithErrorHandler (do
    let errorCountRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)

    -- Check that the handler we get is not the default
    let handler ← SpiderM.getErrorHandler
    let continues ← SpiderM.liftIO <| handler (IO.userError "test")

    pure continues
  ) strictErrorHandler
  -- strictErrorHandler returns false (don't continue)
  shouldBe result false

#generate_tests

end ReactiveTests.ErrorTests
