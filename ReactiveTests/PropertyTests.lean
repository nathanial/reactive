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

/-! ## Phase 1: Behavior Law Completions -/

/-! ### Applicative Laws (additional) -/

proptest "Behavior Applicative composition: pure (.) <*> u <*> v <*> w = u <*> (v <*> w)" :=
  forAllIO (Gen.chooseInt (-50) 50) fun x =>
    let u : Behavior Spider (Int → Int) := Behavior.pure (· + 10)
    let v : Behavior Spider (Int → Int) := Behavior.pure (· * 2)
    let w : Behavior Spider Int := Behavior.constant x
    runSpiderIO do
      -- pure (.) <*> u <*> v <*> w
      let compose : (Int → Int) → (Int → Int) → Int → Int := fun f g x => f (g x)
      let lhs := Behavior.ap (Behavior.ap (Behavior.ap (Behavior.pure compose) u) v) w
      -- u <*> (v <*> w)
      let rhs := Behavior.ap u (Behavior.ap v w)
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

proptest "Behavior Applicative interchange: u <*> pure y = pure ($ y) <*> u" :=
  forAllIO (Gen.chooseInt (-100) 100) fun y =>
    let u : Behavior Spider (Int → Int) := Behavior.pure (· * 3 + 5)
    runSpiderIO do
      -- u <*> pure y
      let lhs := Behavior.ap u (Behavior.pure y)
      -- pure ($ y) <*> u  means  pure (fun f => f y) <*> u
      let applyY : (Int → Int) → Int := fun f => f y
      let rhs := Behavior.ap (Behavior.pure applyY) u
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

/-! ### Monad Laws -/

proptest "Behavior Monad left identity: pure a >>= f = f a" :=
  forAllIO (Gen.chooseInt (-100) 100) fun a =>
    let f : Int → Behavior Spider Int := fun x => Behavior.constant (x * 2 + 1)
    runSpiderIO do
      let lhs := Behavior.bind (Behavior.pure a) f
      let rhs := f a
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

proptest "Behavior Monad right identity: m >>= pure = m" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x => runSpiderIO do
      let m : Behavior Spider Int := Behavior.constant x
      let lhs := Behavior.bind m Behavior.pure
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) m
      pure (v1 == v2)

proptest "Behavior Monad associativity: (m >>= f) >>= g = m >>= (fun x => f x >>= g)" :=
  forAllIO (Gen.chooseInt (-10) 10) fun x =>
    let m : Behavior Spider Int := Behavior.constant x
    let f : Int → Behavior Spider Int := fun n => Behavior.constant (n + 5)
    let g : Int → Behavior Spider Int := fun n => Behavior.constant (n * 2)
    runSpiderIO do
      let lhs := Behavior.bind (Behavior.bind m f) g
      let rhs := Behavior.bind m (fun x => Behavior.bind (f x) g)
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

proptest "Behavior.apply equals Behavior.ap" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    let bf : Behavior Spider (Int → Int) := Behavior.pure (· + 42)
    let ba : Behavior Spider Int := Behavior.constant x
    runSpiderIO do
      let v1 ← sample (t := Spider) (Behavior.apply bf ba)
      let v2 ← sample (t := Spider) (Behavior.ap bf ba)
      pure (v1 == v2)

/-! ## Phase 2: Behavior Combinators -/

/-! ### Zipping -/

proptest "Behavior.zipWith3 samples all three" :=
  forAllIO (Gen.triple
    (Gen.chooseInt (-50) 50)
    (Gen.chooseInt (-50) 50)
    (Gen.chooseInt (-50) 50)) fun (a, b, c) => runSpiderIO do
      let ba : Behavior Spider Int := Behavior.constant a
      let bb : Behavior Spider Int := Behavior.constant b
      let bc : Behavior Spider Int := Behavior.constant c
      let zipped := Behavior.zipWith3 (fun x y z => x + y + z) ba bb bc
      let result ← sample (t := Spider) zipped
      pure (result == a + b + c)

proptest "Behavior.zipWith4 samples all four" :=
  forAllIO (Gen.pair
    (Gen.pair (Gen.chooseInt (-25) 25) (Gen.chooseInt (-25) 25))
    (Gen.pair (Gen.chooseInt (-25) 25) (Gen.chooseInt (-25) 25))) fun ((a, b), (c, d)) => runSpiderIO do
      let ba : Behavior Spider Int := Behavior.constant a
      let bb : Behavior Spider Int := Behavior.constant b
      let bc : Behavior Spider Int := Behavior.constant c
      let bd : Behavior Spider Int := Behavior.constant d
      let zipped := Behavior.zipWith4 (fun w x y z => w + x + y + z) ba bb bc bd
      let result ← sample (t := Spider) zipped
      pure (result == a + b + c + d)

proptest "Behavior.zip equals zipWith Prod.mk" :=
  forAllIO (Gen.pair (Gen.chooseInt (-100) 100) (Gen.chooseInt (-100) 100)) fun (a, b) => runSpiderIO do
      let ba : Behavior Spider Int := Behavior.constant a
      let bb : Behavior Spider Int := Behavior.constant b
      let zipped := Behavior.zip ba bb
      let withMk := Behavior.zipWith Prod.mk ba bb
      let v1 ← sample (t := Spider) zipped
      let v2 ← sample (t := Spider) withMk
      pure (v1 == v2)

/-! ### Boolean Combinators -/

proptest "Behavior.not involution: not (not b) = b" :=
  forAllIO Gen.bool fun x => runSpiderIO do
      let b : Behavior Spider Bool := Behavior.constant x
      let notNot := Behavior.not (Behavior.not b)
      let v1 ← sample (t := Spider) notNot
      let v2 ← sample (t := Spider) b
      pure (v1 == v2)

proptest "Behavior.and commutativity" :=
  forAllIO (Gen.pair Gen.bool Gen.bool) fun (x, y) => runSpiderIO do
      let bx : Behavior Spider Bool := Behavior.constant x
      let by_ : Behavior Spider Bool := Behavior.constant y
      let and1 := Behavior.and bx by_
      let and2 := Behavior.and by_ bx
      let v1 ← sample (t := Spider) and1
      let v2 ← sample (t := Spider) and2
      pure (v1 == v2)

proptest "Behavior.or commutativity" :=
  forAllIO (Gen.pair Gen.bool Gen.bool) fun (x, y) => runSpiderIO do
      let bx : Behavior Spider Bool := Behavior.constant x
      let by_ : Behavior Spider Bool := Behavior.constant y
      let or1 := Behavior.or bx by_
      let or2 := Behavior.or by_ bx
      let v1 ← sample (t := Spider) or1
      let v2 ← sample (t := Spider) or2
      pure (v1 == v2)

proptest "De Morgan: not (a && b) = not a || not b" :=
  forAllIO (Gen.pair Gen.bool Gen.bool) fun (x, y) => runSpiderIO do
      let bx : Behavior Spider Bool := Behavior.constant x
      let by_ : Behavior Spider Bool := Behavior.constant y
      let lhs := Behavior.not (Behavior.and bx by_)
      let rhs := Behavior.or (Behavior.not bx) (Behavior.not by_)
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

proptest "De Morgan 2: not (a || b) = not a && not b" :=
  forAllIO (Gen.pair Gen.bool Gen.bool) fun (x, y) => runSpiderIO do
      let bx : Behavior Spider Bool := Behavior.constant x
      let by_ : Behavior Spider Bool := Behavior.constant y
      let lhs := Behavior.not (Behavior.or bx by_)
      let rhs := Behavior.and (Behavior.not bx) (Behavior.not by_)
      let v1 ← sample (t := Spider) lhs
      let v2 ← sample (t := Spider) rhs
      pure (v1 == v2)

proptest "Behavior.allTrue matches List.all" :=
  forAllIO (Gen.listOf Gen.bool) fun bools => runSpiderIO do
      let behaviors := bools.map (Behavior.constant (t := Spider))
      let allTrue := Behavior.allTrue behaviors
      let result ← sample (t := Spider) allTrue
      let expected := bools.all id
      pure (result == expected)

proptest "Behavior.anyTrue matches List.any" :=
  forAllIO (Gen.listOf Gen.bool) fun bools => runSpiderIO do
      let behaviors := bools.map (Behavior.constant (t := Spider))
      let anyTrue := Behavior.anyTrue behaviors
      let result ← sample (t := Spider) anyTrue
      let expected := bools.any id
      pure (result == expected)

/-! ### Switch/Join -/

proptest "Behavior.switch samples inner behavior" :=
  forAllIO (Gen.pair (Gen.chooseInt (-100) 100) Gen.bool) fun (x, useFirst) =>
    let inner1 : Behavior Spider Int := Behavior.constant x
    let inner2 : Behavior Spider Int := Behavior.constant (x * 2)
    let outer : Behavior Spider (Behavior Spider Int) :=
      Behavior.constant (if useFirst then inner1 else inner2)
    runSpiderIO do
      let switched := Behavior.switch outer
      let result ← sample (t := Spider) switched
      let expected := if useFirst then x else x * 2
      pure (result == expected)

proptest "Behavior.join equals switch" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    let inner : Behavior Spider Int := Behavior.constant x
    let outer : Behavior Spider (Behavior Spider Int) := Behavior.constant inner
    runSpiderIO do
      let v1 ← sample (t := Spider) (Behavior.switch outer)
      let v2 ← sample (t := Spider) (Behavior.join outer)
      pure (v1 == v2)

/-! ## Phase 3: Event Combinators -/

/-! ### Behavior-Event Interactions -/

proptest "Event.gate only fires when behavior is true" :=
  forAllIO (Gen.pair
    (Gen.listOf (Gen.chooseInt (-50) 50))
    (Gen.listOf Gen.bool)) fun (values, gates) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)

      -- Create a behavior that we'll toggle
      let (gateBeh, setGate) ← newTriggerEvent (t := Spider) (a := Bool)
      let gateDyn ← holdDyn true gateBeh
      let gated ← SpiderM.liftIO <| Event.gate ctx gateDyn.current evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← gated.subscribe fun v => received.modify (· ++ [v])

      -- Interleave gate settings and event firings
      let pairs := values.zip gates
      let mut currentGate := true
      let mut expected : List Int := []
      for (v, g) in pairs do
        setGate g
        currentGate := g
        if currentGate then expected := expected ++ [v]
        fire v

      let actual ← SpiderM.liftIO received.get
      pure (actual == expected)

proptest "Event.tag discards event value, returns behavior value" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 100)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (· + ·) init evt

      let tagged ← SpiderM.liftIO <| Event.tag ctx dyn.current evt

      let taggedVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← tagged.subscribe fun v => taggedVals.modify (· ++ [v])

      -- Fire events and compute expected post-update values
      let mut expected : List Int := []
      let mut acc := init
      for e in events do
        acc := acc + e
        expected := expected ++ [acc]
        fire e

      let actual ← SpiderM.liftIO taggedVals.get
      pure (actual == expected)

proptest "Event.attach pairs behavior and event values" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 50)
    (Gen.listOf (Gen.chooseInt 1 10))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (· + ·) init evt

      let attached ← SpiderM.liftIO <| Event.attach ctx dyn.current evt

      let attachedVals ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let _ ← attached.subscribe fun v => attachedVals.modify (· ++ [v])

      let mut expected : List (Int × Int) := []
      let mut acc := init
      for e in events do
        acc := acc + e
        expected := expected ++ [(acc, e)]
        fire e

      let actual ← SpiderM.liftIO attachedVals.get
      pure (actual == expected)

proptest "Event.attachWith applies function to behavior and event" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 50)
    (Gen.listOf (Gen.chooseInt 1 10))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← foldDyn (· + ·) init evt

      let attached ← SpiderM.liftIO <| Event.attachWith ctx (· * ·) dyn.current evt

      let attachedVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← attached.subscribe fun v => attachedVals.modify (· ++ [v])

      let mut expected : List Int := []
      let mut acc := init
      for e in events do
        acc := acc + e
        expected := expected ++ [acc * e]
        fire e

      let actual ← SpiderM.liftIO attachedVals.get
      pure (actual == expected)

proptest "Event.sample equals Event.tag" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let beh : Behavior Spider Int := Behavior.constant 42

      let tagged ← SpiderM.liftIO <| Event.tag ctx beh evt
      let sampled ← SpiderM.liftIO <| Event.sample ctx beh evt

      let taggedVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let sampledVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← tagged.subscribe fun v => taggedVals.modify (· ++ [v])
      let _ ← sampled.subscribe fun v => sampledVals.modify (· ++ [v])

      for e in events do fire e

      let t ← SpiderM.liftIO taggedVals.get
      let s ← SpiderM.liftIO sampledVals.get
      pure (t == s)

proptest "Event.snapshot equals Event.attach" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let beh : Behavior Spider Int := Behavior.constant 42

      let attached ← SpiderM.liftIO <| Event.attach ctx beh evt
      let snapshotted ← SpiderM.liftIO <| Event.snapshot ctx beh evt

      let attachedVals ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let snapshottedVals ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))

      let _ ← attached.subscribe fun v => attachedVals.modify (· ++ [v])
      let _ ← snapshotted.subscribe fun v => snapshottedVals.modify (· ++ [v])

      for e in events do fire e

      let a ← SpiderM.liftIO attachedVals.get
      let s ← SpiderM.liftIO snapshottedVals.get
      pure (a == s)

/-! ### Taking/Dropping -/

proptest "Event.takeN matches List.take" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 10)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (nInt, events) =>
    let n := nInt.toNat
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let taken ← SpiderM.liftIO <| Event.takeN ctx n evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← taken.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      let expected := events.take n
      pure (actual == expected)

proptest "Event.dropN matches List.drop" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 10)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (nInt, events) =>
    let n := nInt.toNat
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dropped ← SpiderM.liftIO <| Event.dropN ctx n evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← dropped.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      let expected := events.drop n
      pure (actual == expected)

proptest "Event.once equals takeN 1" :=
  forAllIO (Gen.listOf1 (Gen.chooseInt (-50) 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (evt2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let onced ← SpiderM.liftIO <| Event.once ctx evt1
      let taken1 ← SpiderM.liftIO <| Event.takeN ctx 1 evt2

      let oncedVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let taken1Vals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← onced.subscribe fun v => oncedVals.modify (· ++ [v])
      let _ ← taken1.subscribe fun v => taken1Vals.modify (· ++ [v])

      for e in events do
        fire1 e
        fire2 e

      let o ← SpiderM.liftIO oncedVals.get
      let t ← SpiderM.liftIO taken1Vals.get
      pure (o == t)

/-! ### Accumulation -/

proptest "Event.accumulate matches List.scanl" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 100)
    (Gen.listOf (Gen.chooseInt (-25) 25))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let accumulated ← SpiderM.liftIO <| Event.accumulate ctx (· + ·) init evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← accumulated.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- Compute running sums manually (scanl emits running sum AFTER each event)
      let rec scanl (acc : Int) : List Int → List Int
        | [] => []
        | x :: xs => let newAcc := acc + x; newAcc :: scanl newAcc xs
      let expected := scanl init events
      pure (actual == expected)

proptest "Event.scan equals accumulate" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 50)
    (Gen.listOf (Gen.chooseInt 1 10))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (evt2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let accumulated ← SpiderM.liftIO <| Event.accumulate ctx (· + ·) init evt1
      let scanned ← SpiderM.liftIO <| Event.scan ctx (· + ·) init evt2

      let accVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let scanVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← accumulated.subscribe fun v => accVals.modify (· ++ [v])
      let _ ← scanned.subscribe fun v => scanVals.modify (· ++ [v])

      for e in events do
        fire1 e
        fire2 e

      let a ← SpiderM.liftIO accVals.get
      let s ← SpiderM.liftIO scanVals.get
      pure (a == s)

proptest "Event.withPrevious emits consecutive pairs" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let withPrev ← SpiderM.liftIO <| Event.withPrevious ctx evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let _ ← withPrev.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- Expected: pairs of consecutive elements, skipping first
      let expected := events.zip (events.drop 1)
      pure (actual == expected)

/-! ### Deduplication -/

proptest "Event.distinct removes only consecutive duplicates" :=
  forAllIO (Gen.listOf (Gen.chooseInt 0 5)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let distinct ← SpiderM.liftIO <| Event.distinct ctx evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← distinct.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- Compute expected by removing consecutive duplicates
      let rec dedupe : List Int → List Int
        | [] => []
        | [x] => [x]
        | x :: y :: rest => if x == y then dedupe (y :: rest) else x :: dedupe (y :: rest)
      let expected := dedupe events
      pure (actual == expected)

proptest "Event.dedupe equals distinct" :=
  forAllIO (Gen.listOf (Gen.chooseInt 0 5)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (evt2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let distinct ← SpiderM.liftIO <| Event.distinct ctx evt1
      let deduped ← SpiderM.liftIO <| Event.dedupe ctx evt2

      let distinctVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let dedupedVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← distinct.subscribe fun v => distinctVals.modify (· ++ [v])
      let _ ← deduped.subscribe fun v => dedupedVals.modify (· ++ [v])

      for e in events do
        fire1 e
        fire2 e

      let d ← SpiderM.liftIO distinctVals.get
      let dd ← SpiderM.liftIO dedupedVals.get
      pure (d == dd)

/-! ### Buffering -/

proptest "Event.buffer emits chunks of size n" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 1 5)
    (Gen.listOf (Gen.chooseInt (-50) 50))) fun (nInt, events) =>
    let n := nInt.toNat
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let buffered ← SpiderM.liftIO <| Event.buffer ctx n evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Array Int))
      let _ ← buffered.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- All emitted chunks should have size n (leftover not emitted)
      pure (actual.all (·.size == n))

proptest "Event.buffer leftover not emitted until full" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 2 5)
    (Gen.chooseInt 1 20)) fun (nInt, totalInt) =>
    let n := nInt.toNat
    let total := totalInt.toNat
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let buffered ← SpiderM.liftIO <| Event.buffer ctx n evt

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Array Int))
      let _ ← buffered.subscribe fun v => received.modify (· ++ [v])

      for i in [:total] do fire (Int.ofNat i)

      let actual ← SpiderM.liftIO received.get
      let expectedChunks := total / n
      pure (actual.length == expectedChunks)

/-! ### Merging (sequential frames) -/

proptest "Event.leftmost takes first in list order when sequential" :=
  -- When events fire in separate frames, leftmost passes all through
  forAllIO (Gen.listOf (Gen.chooseInt 1 100)) fun values =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx

      let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (e2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let leftmost ← SpiderM.liftIO <| Event.leftmost ctx [e1, e2]

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← leftmost.subscribe fun v => received.modify (· ++ [v])

      -- Fire all from e1 first, then all from e2 (sequential frames)
      let half := values.length / 2
      let (first, second) := values.splitAt half
      for v in first do fire1 v
      for v in second do fire2 v

      let actual ← SpiderM.liftIO received.get
      pure (actual == values)

/-! ### Splitting/Partitioning -/

proptest "Event.splitE partitions by predicate" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    let p := (· > 0 : Int → Bool)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let (trueEvt, falseEvt) ← SpiderM.liftIO <| Event.splitE ctx p evt

      let trueVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let falseVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← trueEvt.subscribe fun v => trueVals.modify (· ++ [v])
      let _ ← falseEvt.subscribe fun v => falseVals.modify (· ++ [v])

      for e in events do fire e

      let t ← SpiderM.liftIO trueVals.get
      let f ← SpiderM.liftIO falseVals.get
      let expectedTrue := events.filter p
      let expectedFalse := events.filter (not ∘ p)
      pure (t == expectedTrue && f == expectedFalse)

proptest "Event.partitionE equals splitE" :=
  forAllIO (Gen.listOf (Gen.chooseInt (-50) 50)) fun events =>
    let p := (· % 2 == 0 : Int → Bool)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (evt2, fire2) ← newTriggerEvent (t := Spider) (a := Int)

      let (splitT, splitF) ← SpiderM.liftIO <| Event.splitE ctx p evt1
      let (partT, partF) ← SpiderM.liftIO <| Event.partitionE ctx p evt2

      let splitTVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let splitFVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let partTVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let partFVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← splitT.subscribe fun v => splitTVals.modify (· ++ [v])
      let _ ← splitF.subscribe fun v => splitFVals.modify (· ++ [v])
      let _ ← partT.subscribe fun v => partTVals.modify (· ++ [v])
      let _ ← partF.subscribe fun v => partFVals.modify (· ++ [v])

      for e in events do
        fire1 e
        fire2 e

      let st ← SpiderM.liftIO splitTVals.get
      let sf ← SpiderM.liftIO splitFVals.get
      let pt ← SpiderM.liftIO partTVals.get
      let pf ← SpiderM.liftIO partFVals.get
      pure (st == pt && sf == pf)

proptest "Event.fanEither splits Sum correctly" :=
  forAllIO (Gen.listOf (Gen.pair Gen.bool (Gen.chooseInt (-50) 50))) fun inputs =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Sum Int Int)
      let (leftEvt, rightEvt) ← SpiderM.liftIO <| Event.fanEither ctx evt

      let leftVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let rightVals ← SpiderM.liftIO <| IO.mkRef ([] : List Int)

      let _ ← leftEvt.subscribe fun v => leftVals.modify (· ++ [v])
      let _ ← rightEvt.subscribe fun v => rightVals.modify (· ++ [v])

      for (goLeft, v) in inputs do
        if goLeft then fire (Sum.inl v) else fire (Sum.inr v)

      let l ← SpiderM.liftIO leftVals.get
      let r ← SpiderM.liftIO rightVals.get
      let expectedLeft := (inputs.filter (·.1)).map (·.2)
      let expectedRight := (inputs.filter (not ∘ (·.1))).map (·.2)
      pure (l == expectedLeft && r == expectedRight)

/-! ## Phase 4: Same-Frame Event Properties -/

/-- Helper to fire multiple triggers simultaneously in the same propagation frame. -/
private def fireSimultaneous (fires : List (IO Unit)) : SpiderM Unit := do
  let env ← SpiderM.getEnv
  SpiderM.liftIO <| env.withFrame do
    for fire in fires do fire

proptest "Event.zipE fires only when both fire simultaneously" :=
  forAllIO (Gen.pair (Gen.chooseInt 1 50) (Gen.chooseInt 1 50)) fun (a, b) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      -- Use raw triggers (not framed) so we can control frame boundaries
      let (e1, rawFire1) ← SpiderM.liftIO <| Event.newTrigger ctx
      let (e2, rawFire2) ← SpiderM.liftIO <| Event.newTrigger ctx
      let zipped ← SpiderM.liftIO <| Event.zipE ctx e1 e2

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let _ ← zipped.subscribe fun v => received.modify (· ++ [v])

      -- Fire both in same frame
      fireSimultaneous [rawFire1 a, rawFire2 b]

      let actual ← SpiderM.liftIO received.get
      pure (actual == [(a, b)])

proptest "Event.zipE returns nothing when only one fires" :=
  forAllIO (Gen.chooseInt 1 50) fun a =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (e2, _fire2) ← newTriggerEvent (t := Spider) (a := Int)
      let zipped ← SpiderM.liftIO <| Event.zipE ctx e1 e2

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let _ ← zipped.subscribe fun v => received.modify (· ++ [v])

      -- Fire only e1
      fire1 a

      let actual ← SpiderM.liftIO received.get
      pure actual.isEmpty

proptest "Event.difference fires when e1 fires but not e2" :=
  forAllIO (Gen.chooseInt 1 50) fun a =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (e1, fire1) ← newTriggerEvent (t := Spider) (a := Int)
      let (e2, _fire2) ← newTriggerEvent (t := Spider) (a := Unit)
      let diff ← SpiderM.liftIO <| Event.difference ctx e1 e2

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← diff.subscribe fun v => received.modify (· ++ [v])

      -- Fire only e1
      fire1 a

      let actual ← SpiderM.liftIO received.get
      pure (actual == [a])

proptest "Event.difference suppressed when both fire simultaneously" :=
  forAllIO (Gen.chooseInt 1 50) fun a =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (e1, rawFire1) ← SpiderM.liftIO <| Event.newTrigger ctx
      let (e2, rawFire2) ← SpiderM.liftIO <| Event.newTrigger ctx
      let diff ← SpiderM.liftIO <| Event.difference ctx e1 e2

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← diff.subscribe fun v => received.modify (· ++ [v])

      -- Fire both in same frame
      fireSimultaneous [rawFire1 a, rawFire2 ()]

      let actual ← SpiderM.liftIO received.get
      pure actual.isEmpty

proptest "Event.mergeList collects simultaneous values into list" :=
  forAllIO (Gen.triple
    (Gen.chooseInt 1 30)
    (Gen.chooseInt 31 60)
    (Gen.chooseInt 61 90)) fun (a, b, c) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (e1, rawFire1) ← SpiderM.liftIO <| Event.newTrigger ctx
      let (e2, rawFire2) ← SpiderM.liftIO <| Event.newTrigger ctx
      let (e3, rawFire3) ← SpiderM.liftIO <| Event.newTrigger ctx
      let merged ← SpiderM.liftIO <| Event.mergeList ctx [e1, e2, e3]

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (List Int))
      let _ ← merged.subscribe fun v => received.modify (· ++ [v])

      -- Fire all three in same frame
      fireSimultaneous [rawFire1 a, rawFire2 b, rawFire3 c]

      let actual ← SpiderM.liftIO received.get
      -- Should receive one list with all three values (order may vary)
      pure (actual.length == 1 && (actual.head!.toArray.qsort (· < ·) == #[a, b, c].qsort (· < ·)))

/-! ## Phase 5: Dynamic Combinators -/

proptest "Dynamic.changes emits (old, new) pairs" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 50)
    (Gen.listOf (Gen.chooseInt 1 20))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn init evt

      let changes ← SpiderM.liftIO <| Dynamic.changes ctx dyn

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List (Int × Int))
      let _ ← changes.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- Expected: (old, new) pairs
      let allValues := init :: events
      let expected := allValues.zip (allValues.drop 1)
      pure (actual == expected)

proptest "Dynamic.tagUpdated tags with constant on each update" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 100 200)
    (Gen.listOf (Gen.chooseInt 1 50))) fun (tag, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn 0 evt

      let tagged ← SpiderM.liftIO <| Dynamic.tagUpdated ctx tag dyn

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← tagged.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      let expected := events.map (fun _ => tag)
      pure (actual == expected)

proptest "Dynamic.holdUniqDyn filters consecutive duplicates" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 0 10)
    (Gen.listOf (Gen.chooseInt 0 5))) fun (init, events) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (evt, fire) ← newTriggerEvent (t := Spider) (a := Int)
      let dyn ← holdDyn init evt

      let uniq ← SpiderM.liftIO <| Dynamic.holdUniqDyn ctx dyn

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← uniq.updated.subscribe fun v => received.modify (· ++ [v])

      for e in events do fire e

      let actual ← SpiderM.liftIO received.get
      -- Compute expected by removing consecutive duplicates from (init :: events)
      let rec dedupe : List Int → List Int
        | [] => []
        | [x] => [x]
        | x :: y :: rest => if x == y then dedupe (y :: rest) else x :: dedupe (y :: rest)
      let deduped := dedupe (init :: events)
      let expected := deduped.drop 1  -- drop the init since it's not an update
      pure (actual == expected)

proptest "Dynamic.Builder Functor identity" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let builder : Dynamic.Builder Spider Int := Dynamic.Builder.of (← Dynamic.constant ctx x)
      let mapped := Functor.map id builder
      let d ← SpiderM.liftIO <| Dynamic.Builder.run ctx mapped
      let result ← d.sample
      pure (result == x)

proptest "Dynamic.Builder Functor composition" :=
  forAllIO (Gen.chooseInt (-50) 50) fun x =>
    let f := (· + 10 : Int → Int)
    let g := (· * 2 : Int → Int)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let builder : Dynamic.Builder Spider Int := Dynamic.Builder.of (← Dynamic.constant ctx x)
      let composed := Functor.map (f ∘ g) builder
      let sequential := Functor.map f (Functor.map g builder)
      let d1 ← SpiderM.liftIO <| Dynamic.Builder.run ctx composed
      let d2 ← SpiderM.liftIO <| Dynamic.Builder.run ctx sequential
      let v1 ← d1.sample
      let v2 ← d2.sample
      pure (v1 == v2)

proptest "Dynamic.Builder Applicative identity" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let builder : Dynamic.Builder Spider Int := Dynamic.Builder.of (← Dynamic.constant ctx x)
      let applied := Seq.seq (Pure.pure id) (fun _ => builder)
      let d ← SpiderM.liftIO <| Dynamic.Builder.run ctx applied
      let result ← d.sample
      pure (result == x)

proptest "Dynamic.Builder Applicative homomorphism" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    let f := (· + 42 : Int → Int)
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let lhs : Dynamic.Builder Spider Int := Seq.seq (Pure.pure f) (fun _ => Pure.pure x)
      let rhs : Dynamic.Builder Spider Int := Pure.pure (f x)
      let d1 ← SpiderM.liftIO <| Dynamic.Builder.run ctx lhs
      let d2 ← SpiderM.liftIO <| Dynamic.Builder.run ctx rhs
      let v1 ← d1.sample
      let v2 ← d2.sample
      pure (v1 == v2)

proptest "Dynamic.pure' creates constant dynamic" :=
  forAllIO (Gen.chooseInt (-100) 100) fun x =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let d ← SpiderM.liftIO <| Dynamic.pure' ctx x
      let v1 ← d.sample
      let v2 ← d.sample
      pure (v1 == x && v2 == x)

/-! ## Phase 6: Switch Combinators -/

proptest "switchDyn fires from current inner event" :=
  forAllIO (Gen.listOf (Gen.chooseInt 1 50)) fun events =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (innerEvt, fireInner) ← newTriggerEvent (t := Spider) (a := Int)
      let dynOfEvent ← holdDyn innerEvt (← SpiderM.liftIO <| Event.never ctx)
      let switched ← SpiderM.liftIO <| switchDyn ctx dynOfEvent

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← switched.subscribe fun v => received.modify (· ++ [v])

      for e in events do fireInner e

      let actual ← SpiderM.liftIO received.get
      pure (actual == events)

proptest "switchDyn stops firing from old event after switch" :=
  forAllIO (Gen.pair
    (Gen.listOf (Gen.chooseInt 1 50))
    (Gen.listOf (Gen.chooseInt 51 100))) fun (before, after) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (oldEvt, fireOld) ← newTriggerEvent (t := Spider) (a := Int)
      let (newEvt, fireNew) ← newTriggerEvent (t := Spider) (a := Int)
      let (switchEvt, fireSwitch) ← newTriggerEvent (t := Spider) (a := Event Spider Int)

      let dynOfEvent ← holdDyn oldEvt switchEvt
      let switched ← SpiderM.liftIO <| switchDyn ctx dynOfEvent

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← switched.subscribe fun v => received.modify (· ++ [v])

      -- Fire from old event
      for e in before do fireOld e

      -- Switch to new event
      fireSwitch newEvt

      -- Fire from old event (should not appear)
      for _ in [:3] do fireOld 999

      -- Fire from new event
      for e in after do fireNew e

      let actual ← SpiderM.liftIO received.get
      pure (actual == before ++ after)

proptest "switchHold switches to new event on update" :=
  forAllIO (Gen.pair
    (Gen.listOf (Gen.chooseInt 1 25))
    (Gen.listOf (Gen.chooseInt 26 50))) fun (firstEvents, secondEvents) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      let (initial, fireInitial) ← newTriggerEvent (t := Spider) (a := Int)
      let (replacement, fireReplacement) ← newTriggerEvent (t := Spider) (a := Int)
      let (updates, fireUpdate) ← newTriggerEvent (t := Spider) (a := Event Spider Int)

      let held ← SpiderM.liftIO <| switchHold ctx initial updates

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← held.subscribe fun v => received.modify (· ++ [v])

      -- Fire from initial
      for e in firstEvents do fireInitial e

      -- Switch to replacement
      fireUpdate replacement

      -- Fire from initial (should not appear)
      for _ in [:2] do fireInitial 999

      -- Fire from replacement
      for e in secondEvents do fireReplacement e

      let actual ← SpiderM.liftIO received.get
      pure (actual == firstEvents ++ secondEvents)

proptest "switchDynamic updates from outer or inner changes" :=
  forAllIO (Gen.pair
    (Gen.chooseInt 1 50)
    (Gen.chooseInt 51 100)) fun (innerVal, innerVal2) =>
    runSpiderIO do
      let ctx ← SpiderM.getTimelineCtx
      -- Create two inner dynamics
      let (innerEvt1, fireInner1) ← newTriggerEvent (t := Spider) (a := Int)
      let (innerEvt2, fireInner2) ← newTriggerEvent (t := Spider) (a := Int)
      let innerDyn1 ← holdDyn 0 innerEvt1
      let innerDyn2 ← holdDyn 0 innerEvt2

      -- Create outer dynamic that switches between inners
      let (outerEvt, fireOuter) ← newTriggerEvent (t := Spider) (a := Dynamic Spider Int)
      let outerDyn ← holdDyn innerDyn1 outerEvt

      let switched ← SpiderM.liftIO <| switchDynamic ctx outerDyn

      let received ← SpiderM.liftIO <| IO.mkRef ([] : List Int)
      let _ ← switched.updated.subscribe fun v => received.modify (· ++ [v])

      -- Update inner1
      fireInner1 innerVal

      -- Switch to inner2
      fireOuter innerDyn2

      -- Update inner2
      fireInner2 innerVal2

      -- Update inner1 (should not appear since we switched)
      fireInner1 999

      let actual ← SpiderM.liftIO received.get
      -- Expected: innerVal from inner1, then 0 (current value of inner2 on switch), then innerVal2
      pure (actual == [innerVal, 0, innerVal2])

end ReactiveTests.PropertyTests
