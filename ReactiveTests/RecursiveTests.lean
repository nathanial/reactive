import Crucible
import Reactive

namespace ReactiveTests.RecursiveTests

open Crucible
open Reactive
open Reactive.Host

testSuite "Recursive Tests"

test "fixDynM creates self-referential dynamic" := do
  let result ← runSpider do
    let fireRef ← SpiderM.liftIO <| IO.mkRef (fun () => pure () : Unit → IO Unit)

    let counter ← SpiderM.fixDynM fun counterBehavior => do
      let (clicks, fire) ← newTriggerEvent (t := Spider) (a := Unit)
      SpiderM.liftIO <| fireRef.set fire
      -- Filter based on counter value (circular ref!)
      -- Use gateM with a mapped behavior
      let gateBehavior := counterBehavior.map (fun c => decide (c < 3))
      let gated ← Event.gateM gateBehavior clicks
      foldDyn (fun _ n => n + 1) 0 gated

    let fire ← SpiderM.liftIO fireRef.get

    -- Fire 5 times, but only first 3 should count
    fire ()
    fire ()
    fire ()
    fire ()  -- Should be filtered (counter = 3)
    fire ()  -- Should be filtered

    sample counter.current

  shouldBe result 3

test "fixDyn2M creates mutually recursive dynamics" := do
  let result ← runSpider do
    let fireRef ← SpiderM.liftIO <| IO.mkRef (fun () => pure () : Unit → IO Unit)

    let (toggle, count) ← SpiderM.fixDyn2M fun toggleB countB => do
      let (event, fire) ← newTriggerEvent (t := Spider) (a := Unit)
      SpiderM.liftIO <| fireRef.set fire

      -- Toggle flips on each event when count is even
      let evenBehavior := countB.map (fun c => c % 2 == 0)
      let toggleFiltered ← Event.gateM evenBehavior event
      let toggle' ← foldDyn (fun _ b => !b) false toggleFiltered

      -- Count increments only when toggle is true
      let countFiltered ← Event.gateM toggleB event
      let count' ← foldDyn (fun _ n => n + 1) 0 countFiltered

      pure (toggle', count')

    -- Just verify it compiles and runs without infinite loop
    let t ← sample toggle.current
    let c ← sample count.current
    pure (t, c)

  shouldBe result (false, 0)

test "fixDynM behavior samples real dynamic after wiring" := do
  let result ← runSpider do
    let fireRef ← SpiderM.liftIO <| IO.mkRef (fun () => pure () : Unit → IO Unit)
    let sampledRef ← SpiderM.liftIO <| IO.mkRef ([] : List Nat)

    let counter ← SpiderM.fixDynM fun counterBehavior => do
      let (clicks, fire) ← newTriggerEvent (t := Spider) (a := Unit)
      SpiderM.liftIO <| fireRef.set fire

      let counter' ← foldDyn (fun _ n => n + 1) 0 clicks

      -- Subscribe to sample the behavior on each update
      let _ ← counter'.updated.subscribe fun _ => do
        let c ← counterBehavior.sample
        sampledRef.modify (· ++ [c])

      pure counter'

    let fire ← SpiderM.liftIO fireRef.get
    fire ()
    fire ()
    fire ()

    SpiderM.liftIO sampledRef.get

  -- Each sample sees the counter value at time of event
  shouldBe result [1, 2, 3]

test "fixDynM with initial value check" := do
  let result ← runSpider do
    let counter ← SpiderM.fixDynM fun _counterBehavior => do
      let (clicks, _) ← newTriggerEvent (t := Spider) (a := Unit)
      foldDyn (fun _ n => n + 1) 42 clicks  -- Start at 42

    sample counter.current

  shouldBe result 42


end ReactiveTests.RecursiveTests
