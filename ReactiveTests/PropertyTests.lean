/-
  ReactiveTests/PropertyTests.lean

  Property-based tests for the Reactive FRP library using Crucible's
  proptest infrastructure. Tests functor/applicative laws, event combinator
  properties, and accumulation correctness.
-/
import Crucible
import Reactive

namespace ReactiveTests.PropertyTests

open Crucible
open Crucible.Property
open Reactive
open Reactive.Host

/-! ## Helpers -/

/-- Run a SpiderM Bool action as IO Bool for property testing. -/
@[inline] private def runSpiderIO (action : SpiderM Bool) : IO Bool :=
  runSpider action

testSuite "Property Tests"

/-! ## Behavior Functor Laws -/

proptest "Behavior.map id = id" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x => runSpiderIO do
      let b : Behavior Spider Int := Behavior.constant x
      let mapped := Behavior.map id b
      let original ← sample (t := Spider) b
      let result ← sample (t := Spider) mapped
      pure (original == result)

proptest "Behavior.map (f ∘ g) = Behavior.map f ∘ Behavior.map g" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    let f := (· + 1 : Int → Int)
    let g := (· * 2 : Int → Int)
    runSpiderIO do
      let b : Behavior Spider Int := Behavior.constant x
      -- (f ∘ g) applied once
      let composed := Behavior.map (f ∘ g) b
      -- f . g applied in two steps
      let sequential := Behavior.map f (Behavior.map g b)
      let v1 ← sample (t := Spider) composed
      let v2 ← sample (t := Spider) sequential
      pure (v1 == v2)

/-! ## Behavior Applicative Laws -/

proptest "Behavior pure id <*> b = b (identity)" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x => runSpiderIO do
      let b : Behavior Spider Int := Behavior.constant x
      let applied := Behavior.ap (Behavior.pure id) b
      let v1 ← sample (t := Spider) applied
      let v2 ← sample (t := Spider) b
      pure (v1 == v2)

proptest "Behavior pure f <*> pure x = pure (f x) (homomorphism)" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    let f := (· + 42 : Int → Int)
    runSpiderIO do
      let applied : Behavior Spider Int := Behavior.ap (Behavior.pure f) (Behavior.pure x)
      let direct : Behavior Spider Int := Behavior.pure (f x)
      let v1 ← sample (t := Spider) applied
      let v2 ← sample (t := Spider) direct
      pure (v1 == v2)

/-! ## Behavior zipWith Properties -/

proptest "Behavior.zipWith is commutative for commutative operations" :=
  forAllIO (Gen.pair (Gen.chooseInt (-100) 100) (Gen.chooseInt (-100) 100)) fun (x, y) => runSpiderIO do
      let bx : Behavior Spider Int := Behavior.constant x
      let by_ : Behavior Spider Int := Behavior.constant y
      let sum1 := Behavior.zipWith (· + ·) bx by_
      let sum2 := Behavior.zipWith (· + ·) by_ bx
      let v1 ← sample (t := Spider) sum1
      let v2 ← sample (t := Spider) sum2
      pure (v1 == v2)

proptest "Behavior.constant always returns same value on multiple samples" :=
  forAllIO (Gen.chooseInt (-1000) 1000) fun x => runSpiderIO do
      let b : Behavior Spider Int := Behavior.constant x
      let v1 ← sample (t := Spider) b
      let v2 ← sample (t := Spider) b
      let v3 ← sample (t := Spider) b
      pure (v1 == x && v2 == x && v3 == x)

/-! ## Event Map Properties -/

proptest "Event.map id fires same value" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-100) 100)) fun events => runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let mapped ← SpiderM.liftIO <| Event.map ctx id evt

      let receivedOrig ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let receivedMap ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← evt.subscribe fun v => receivedOrig.modify (· ++ [v])
      let _ ← mapped.subscribe fun v => receivedMap.modify (· ++ [v])

      for e in events do fire e

      let orig ← SpiderM.liftIO receivedOrig.get
      let mapped' ← SpiderM.liftIO receivedMap.get
      pure (orig == mapped')

proptest "Event.map (f ∘ g) = Event.map f ∘ Event.map g" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    let f := (· + 1 : Int → Int)
    let g := (· * 2 : Int → Int)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      -- Composed
      let composed ← SpiderM.liftIO <| Event.map ctx (f ∘ g) evt
      -- Sequential
      let step1 ← SpiderM.liftIO <| Event.map ctx g evt
      let sequential ← SpiderM.liftIO <| Event.map ctx f step1

      let receivedComp ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let receivedSeq ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← composed.subscribe fun v => receivedComp.modify (· ++ [v])
      let _ ← sequential.subscribe fun v => receivedSeq.modify (· ++ [v])

      for e in events do fire e

      let comp ← SpiderM.liftIO receivedComp.get
      let seq ← SpiderM.liftIO receivedSeq.get
      pure (comp == seq)

/-! ## Event Filter Properties -/

proptest "Event.filter p then filter q = filter (p && q)" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    let p := (· > 0 : Int → Bool)
    let q := (· < 30 : Int → Bool)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      -- Combined filter
      let combined ← SpiderM.liftIO <| Event.filter ctx (fun x => p x && q x) evt
      -- Sequential filters
      let step1 ← SpiderM.liftIO <| Event.filter ctx p evt
      let sequential ← SpiderM.liftIO <| Event.filter ctx q step1

      let receivedComb ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let receivedSeq ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← combined.subscribe fun v => receivedComb.modify (· ++ [v])
      let _ ← sequential.subscribe fun v => receivedSeq.modify (· ++ [v])

      for e in events do fire e

      let comb ← SpiderM.liftIO receivedComb.get
      let seq ← SpiderM.liftIO receivedSeq.get
      pure (comb == seq)

proptest "Event.filter (const true) = id" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-100) 100)) fun events => runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      let filtered ← SpiderM.liftIO <| Event.filter ctx (fun _ => true) evt

      let receivedOrig ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let receivedFilt ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← evt.subscribe fun v => receivedOrig.modify (· ++ [v])
      let _ ← filtered.subscribe fun v => receivedFilt.modify (· ++ [v])

      for e in events do fire e

      let orig ← SpiderM.liftIO receivedOrig.get
      let filt ← SpiderM.liftIO receivedFilt.get
      pure (orig == filt)

proptest "Event.filter (const false) fires nothing" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-100) 100)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      let filtered ← SpiderM.liftIO <| Event.filter ctx (fun _ => false) evt

      let receivedFilt ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← filtered.subscribe fun v => receivedFilt.modify (· ++ [v])

      for e in events do fire e

      let filt ← SpiderM.liftIO receivedFilt.get
      pure filt.isEmpty

/-! ## Event Merge Properties -/

proptest "Event.merge is associative (multiset equality)" :=
  -- Tests that (e1 ⊕ e2) ⊕ e3 receives same values as e1 ⊕ (e2 ⊕ e3)
  -- Fires events sequentially in separate frames; checks multiset equality
  forAllIO (Gen.triple
    (Gen.listOf (Gen.chooseInt 1 10))
    (Gen.listOf (Gen.chooseInt 11 20))
    (Gen.listOf (Gen.chooseInt 21 30))) fun (evts1, evts2, evts3) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx

      let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Int)
      let (e3, fire3) ← newTriggerEvent (t := Spider) (a := Int)

      -- (e1 merge e2) merge e3
      let m12 ← SpiderM.liftIO <| Event.merge ctx e1 e2
      let left ← SpiderM.liftIO <| Event.merge ctx m12 e3

      -- e1 merge (e2 merge e3)
      let m23 ← SpiderM.liftIO <| Event.merge ctx e2 e3
      let right ← SpiderM.liftIO <| Event.merge ctx e1 m23

      let receivedLeft ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let receivedRight ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← left.subscribe fun v => receivedLeft.modify (· ++ [v])
      let _ ← right.subscribe fun v => receivedRight.modify (· ++ [v])

      -- Fire all events sequentially (separate frames)
      for e in evts1 do fire1 e
      for e in evts2 do fire2 e
      for e in evts3 do fire3 e

      let leftVals ← SpiderM.liftIO receivedLeft.get
      let rightVals ← SpiderM.liftIO receivedRight.get

      -- Both should receive the same multiset of values
      pure (leftVals.toArray.qsort (· < ·) == rightVals.toArray.qsort (· < ·))

proptest "Event.merge preserves exact order for sequential fires" :=
  -- Tests that merge preserves exact ordering when events fire in separate frames
  forAllIO (Gen.listOf (Gen.chooseInt 1 100)) fun values =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx

      let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let merged ← SpiderM.liftIO <| Event.merge ctx e1 e2

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← merged.subscribe fun v => received.modify (· ++ [v])

      -- Alternate firing between e1 and e2
      let mut i := 0
      for v in values do
        if i % 2 == 0 then fire1 v else fire2 v
        i := i + 1

      let actual ← SpiderM.liftIO received.get
      -- Order should match exact firing order
      pure (actual == values)

/-! ## foldDyn Accumulation Properties -/

proptest "foldDyn accumulates like List.foldl" :=
  forAllIO (Gen.pair
    (Gen.chooseInt (-100) 100)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (init, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (fun a acc => acc + a) init evt

      for e in events do fire e

      let result ← dyn.sample
      let expected := events.foldl (· + ·) init
      pure (result == expected)

proptest "foldDyn with multiplication accumulates correctly" :=
  -- Use bounded generator (max 5 events with values 1-3) to avoid overflow
  forAllIO (Gen.pair
    (Gen.chooseInt 1 5)
    (Gen.listOfN 5 (Gen.chooseInt 1 3))) fun (init, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (fun a acc => acc * a) init evt

      for e in events do fire e

      let result ← dyn.sample
      let expected := events.foldl (· * ·) init
      pure (result == expected)

proptest "foldDyn with id (replace) holds last value" :=
  forAllIO (Gen.pair
    (Gen.chooseInt (-100) 100)
    (Gen.listOf (Gen.chooseInt (-100) 100))) fun (init, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (fun a _ => a) init evt

      for e in events do fire e

      let result ← dyn.sample
      let expected := events.getLast?.getD init
      pure (result == expected)

/-! ## holdDyn Properties -/

proptest "holdDyn holds initial value when no events" :=
  forAllIO (Gen.chooseInt (-1000) 1000) fun init =>
    runSpiderIO do
      let (evt, _) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn init evt
      let result ← dyn.sample
      pure (result == init)

proptest "holdDyn holds last fired value" :=
  forAllIO (Gen.pair
    (Gen.chooseInt (-100) 100)
    (Gen.listOf1 (Gen.chooseInt (-100) 100))) fun (init, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn init evt

      for e in events do fire e

      let result ← dyn.sample
      let expected := events.getLast!
      pure (result == expected)

/-! ## Dynamic.updated Event Properties -/

proptest "Dynamic.updated fires for each event" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 100)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (init, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn init evt

      let updatesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← dyn.updated.subscribe fun v => updatesRef.modify (· ++ [v])

      for e in events do fire e

      let updates ← SpiderM.liftIO updatesRef.get
      pure (updates == events)

/-! ## Behavior Sample Consistency -/

proptest "Event.tag samples post-update value (glitch-free semantics)" :=
  -- Tests glitch-free propagation: Event.tag sees the foldDyn value AFTER
  -- the current event has been processed. This is the expected FRP semantics.
  -- If evt drives foldDyn, then Event.tag ctx dyn.current evt samples the
  -- NEW accumulated value, not the previous one.
  forAllIO (Gen.pair
    (Gen.chooseInt 0 100)
    (Gen.listOf1 (Gen.chooseInt 1 50))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (· + ·) init evt

      -- Event.tag samples dyn.current when evt fires
      let sampled ← SpiderM.liftIO <| Event.tag ctx dyn.current evt

      let sampledVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← sampled.subscribe fun v => sampledVals.modify (· ++ [v])

      -- Fire events and compute expected post-update values
      let mut expected : List Int := []
      let mut acc := init
      for e in events do
        acc := acc + e
        expected := expected ++ [acc]
        fire e

      let actual ← SpiderM.liftIO sampledVals.get
      pure (actual == expected)

/-! ## Behavior.zipWith Consistency -/

proptest "Behavior.zipWith samples both behaviors consistently" :=
  forAllIO (Gen.triple
    (Gen.chooseInt 0 50)
    (Gen.chooseInt 1 50)
    (Gen.listOf (Gen.chooseInt 1 10))) fun (init1, init2, events) =>
    runSpiderIO do
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn1 ← foldDyn (· + ·) init1 evt
      let dyn2 ← foldDyn (· * ·) init2 evt

      let zipped := Behavior.zipWith (· + ·) dyn1.current dyn2.current

      for e in events do fire e

      let v1 ← sample (t := Spider) dyn1.current
      let v2 ← sample (t := Spider) dyn2.current
      let vZip ← sample (t := Spider) zipped

      pure (vZip == v1 + v2)

/-! ## Event.mapMaybe Properties -/

proptest "Event.mapMaybe only passes Some values" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      -- Only pass positive values, doubled
      let mapped ← SpiderM.liftIO <| Event.mapMaybe ctx
        (fun x => if x > 0 then some (x * 2) else none) evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← mapped.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      let expected := events.filterMap (fun x => if x > 0 then some (x * 2) else none)
      pure (actual == expected)

end ReactiveTests.PropertyTests
