/-
  Reactive - A Reflex-style FRP library for Lean 4

  This library provides Functional Reactive Programming primitives:
  - Event: Discrete occurrences over time
  - Behavior: Time-varying values
  - Dynamic: Behaviors with change notifications

  Example usage:

  ```lean
  import Reactive

  open Reactive
  open Reactive.Host

  def example : SpiderM Unit := do
    -- Create a triggerable event
    let (clickEvent, fireClick) ← newTriggerEvent

    -- Hold the click count
    let clickCount ← foldDyn (fun _ n => n + 1) 0 clickEvent

    -- Subscribe to changes
    let _ ← SpiderM.liftIO <| clickCount.updated.subscribe fun n =>
      IO.println s!"Click count: {n}"

    pure ()

  def main : IO Unit := do
    runSpider example
  ```

  ## Core Types

  - `Event t a`: Stream of discrete occurrences
  - `Behavior t a`: Time-varying value (can sample anytime)
  - `Dynamic t a`: Behavior with change Event

  ## Key Typeclasses

  - `MonadSample t m`: Can sample Behaviors
  - `MonadHold t m`: Can create Behaviors/Dynamics from Events
  - `TriggerEvent t m`: Can create external event triggers
  - `PostBuild t m`: Can run actions after network construction
  - `Adjustable t m`: Supports dynamic switching

  ## Host

  The `Spider` timeline provides an IO-based push runtime.
  Use `SpiderM` to build reactive networks and `runSpider` to execute them.
-/

import Reactive.Core
import Reactive.Class
import Reactive.Combinators
import Reactive.Host
