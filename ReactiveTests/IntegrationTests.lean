/-
  ReactiveTests/IntegrationTests.lean

  Real-world integration tests demonstrating FRP patterns in practical scenarios.
-/
import Crucible
import Reactive

namespace ReactiveTests.IntegrationTests

open Crucible
open Reactive
open Reactive.Host

/-- Approximate Float equality for testing -/
def floatApproxEq (a b : Float) (epsilon : Float := 0.001) : Bool :=
  (a - b).abs < epsilon

/-- Assert approximate Float equality -/
def assertFloatEq (expected actual : Float) (msg : String := "") : IO Unit := do
  if floatApproxEq expected actual then
    pure ()
  else
    throw <| IO.userError s!"Float assertion failed{if msg.isEmpty then "" else ": " ++ msg}. Expected {expected}, got {actual}"

/-- Assert list of Floats approximately equal -/
def assertFloatListEq (expected actual : List Float) : IO Unit := do
  if expected.length != actual.length then
    throw <| IO.userError s!"List length mismatch. Expected {expected.length}, got {actual.length}"
  for (e, a) in expected.zip actual do
    if !floatApproxEq e a then
      throw <| IO.userError s!"Float list mismatch. Expected {expected}, got {actual}"

/-- Assert tuple of 2 Floats approximately equal -/
def assertFloat2Eq (expected actual : Float × Float) : IO Unit := do
  assertFloatEq expected.1 actual.1
  assertFloatEq expected.2 actual.2

/-- Assert tuple of 3 Floats approximately equal -/
def assertFloat3Eq (expected actual : Float × Float × Float) : IO Unit := do
  assertFloatEq expected.1 actual.1
  assertFloatEq expected.2.1 actual.2.1
  assertFloatEq expected.2.2 actual.2.2

/-! ## Gas Pump Example

The gas pump is a classic FRP example demonstrating:
- Dynamic switching (fuel grade selection changes price behavior)
- State machines (sale lifecycle: Idle → Ready → Pumping → Complete)
- Compound gating (nozzle must be up AND under prepaid limit)
- Multiple derived behaviors updating simultaneously
-/

-- Fuel grades with different prices
inductive FuelGrade where
  | regular   -- $3.50/gal
  | plus      -- $3.80/gal
  | premium   -- $4.20/gal
  deriving Repr, BEq, Inhabited

def FuelGrade.price : FuelGrade → Float
  | .regular => 3.50
  | .plus => 3.80
  | .premium => 4.20

def FuelGrade.name : FuelGrade → String
  | .regular => "Regular"
  | .plus => "Plus"
  | .premium => "Premium"

-- Sale lifecycle states
inductive SaleState where
  | idle      -- Waiting for customer
  | ready     -- Grade selected, waiting for nozzle
  | pumping   -- Actively dispensing fuel
  | complete  -- Sale finished
  deriving Repr, BEq, Inhabited

-- Display update messages (what the pump screen shows)
structure DisplayUpdate where
  gallons : Float
  cost : Float
  grade : FuelGrade
  state : SaleState
  deriving Repr

testSuite "Gas Pump"

test "gas pump accumulates gallons and calculates cost" := do
  let result ← runSpider do
    let pricePerGallon : Behavior Spider Float := Behavior.constant 3.50
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent
    let costBehavior := Behavior.zipWith (· * ·) gallonsDyn.current pricePerGallon

    pumpFlow 1.5
    pumpFlow 2.0
    pumpFlow 0.5

    let totalGallons ← sample gallonsDyn.current
    let totalCost ← sample costBehavior
    pure (totalGallons, totalCost)

  assertFloatEq 4.0 result.1
  assertFloatEq 14.0 result.2

test "gas pump stops at prepaid amount" := do
  let result ← runSpider do
    let pricePerGallon : Float := 4.00
    let prepaidAmount : Float := 20.00

    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent

    let underLimitBehavior : Behavior Spider Bool := gallonsDyn.current.map fun gallons =>
      decide ((gallons * pricePerGallon) < prepaidAmount)

    let gatedFlow ← Event.gateM underLimitBehavior flowEvent
    let actualGallonsDyn ← foldDyn (· + ·) 0.0 gatedFlow

    -- Pump 10 times (only first 4 should pass the gate)
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0

    sample actualGallonsDyn.current

  assertFloatEq 4.0 result

test "gas pump tracks cost in real-time" := do
  let result ← runSpider do
    let pricePerGallon : Float := 3.00

    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent

    let costUpdatesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Float)
    let costEvent ← Event.map' gallonsDyn.updated (· * pricePerGallon)
    let _ ← costEvent.subscribe fun cost =>
      costUpdatesRef.modify (· ++ [cost])

    pumpFlow 1.0
    pumpFlow 1.0
    pumpFlow 1.0

    SpiderM.liftIO costUpdatesRef.get

  assertFloatListEq [3.0, 6.0, 9.0] result

/-! ### Classic Gas Pump - The Famous FRP Example

This demonstrates the key FRP patterns:
1. **Dynamic switching**: Selecting a fuel grade switches to a different price behavior
2. **State machine**: Sale lifecycle with state transitions
3. **Compound gating**: Flow only when nozzle up AND under limit
4. **Simultaneous updates**: Multiple displays update together
-/

test "fuel grade selection dynamically switches price behavior" := do
  let result ← runSpider do
    -- Events from the pump interface
    let (gradeSelectEvent, selectGrade) ← newTriggerEvent (t := Spider) (a := FuelGrade)
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)

    -- Current fuel grade (starts with Regular)
    let gradeDyn ← holdDyn FuelGrade.regular gradeSelectEvent

    -- THE KEY PATTERN: Price behavior switches when grade changes
    -- This is what makes the gas pump example famous in FRP literature
    let priceBehavior : Behavior Spider Float := gradeDyn.current.map FuelGrade.price

    -- Accumulate gallons
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent

    -- Cost derived from current gallons and current price
    let costBehavior := Behavior.zipWith (· * ·) gallonsDyn.current priceBehavior

    -- Start pumping Regular at $3.50
    pumpFlow 2.0
    let cost1 ← sample costBehavior  -- 2.0 * 3.50 = 7.00

    -- Switch to Premium mid-transaction!
    -- In real life you can't do this, but it demonstrates dynamic switching
    selectGrade FuelGrade.premium
    let cost2 ← sample costBehavior  -- 2.0 * 4.20 = 8.40 (price changed!)

    -- Pump more at Premium price
    pumpFlow 1.0
    let cost3 ← sample costBehavior  -- 3.0 * 4.20 = 12.60

    pure (cost1, cost2, cost3)

  assertFloat3Eq (7.0, 8.4, 12.6) result

test "nozzle state gates fuel flow" := do
  let result ← runSpider do
    -- Nozzle up/down events
    let (nozzleEvent, setNozzle) ← newTriggerEvent (t := Spider) (a := Bool)
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)

    -- Nozzle state (starts down)
    let nozzleDyn ← holdDyn false nozzleEvent

    -- Only allow flow when nozzle is up
    let gatedFlow ← Event.gateM nozzleDyn.current flowEvent
    let gallonsDyn ← foldDyn (· + ·) 0.0 gatedFlow

    -- Try to pump with nozzle down - should be blocked
    pumpFlow 1.0
    let g1 ← sample gallonsDyn.current

    -- Lift nozzle
    setNozzle true

    -- Now pumping works
    pumpFlow 2.0
    pumpFlow 1.5
    let g2 ← sample gallonsDyn.current

    -- Put nozzle down
    setNozzle false

    -- Blocked again
    pumpFlow 5.0
    let g3 ← sample gallonsDyn.current

    pure (g1, g2, g3)

  assertFloat3Eq (0.0, 3.5, 3.5) result

test "compound gating: nozzle up AND under prepaid limit" := do
  let result ← runSpider do
    let prepaidAmount : Float := 20.00
    let pricePerGallon : Float := 4.00

    let (nozzleEvent, setNozzle) ← newTriggerEvent (t := Spider) (a := Bool)
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)

    let nozzleDyn ← holdDyn false nozzleEvent

    -- Use fixDynM to create self-referential gating:
    -- Can only pump when nozzle up AND current cost < prepaid limit
    let gallonsDyn : Dynamic Spider Float ← SpiderM.fixDynM (a := Float) fun gallonsBehavior => do
      -- Cost based on current (gated) gallons
      let costBehavior := gallonsBehavior.map (· * pricePerGallon)

      -- Compound gate: nozzle up AND cost under prepaid limit
      let canPump : Behavior Spider Bool := Behavior.zipWith (· && ·)
        nozzleDyn.current
        (costBehavior.map fun cost => decide (cost < prepaidAmount))

      let gatedFlow ← Event.gateM canPump flowEvent
      foldDyn (· + ·) (0.0 : Float) gatedFlow

    -- Nozzle down: blocked
    pumpFlow 1.0
    let g1 ← sample gallonsDyn.current

    -- Lift nozzle, pump until near limit
    setNozzle true
    pumpFlow 2.0  -- $8, passes (cost was 0)
    pumpFlow 2.0  -- $16, passes (cost was 8)
    let g2 ← sample gallonsDyn.current

    -- This would put us at $24, blocked (cost is 16 < 20, but 16+8=24 > 20)
    -- Actually: at cost $16, still under $20, so this passes and we hit $24
    -- Then the NEXT one would be blocked
    pumpFlow 2.0  -- passes, cost becomes $24
    pumpFlow 2.0  -- blocked, cost $24 >= $20
    let g3 ← sample gallonsDyn.current

    pure (g1, g2, g3)

  -- g1=0 (nozzle down), g2=4 (2+2), g3=6 (4+2, last blocked)
  assertFloat3Eq (0.0, 4.0, 6.0) result

-- Actions that can affect sale state
inductive SaleAction where
  | selectGrade : FuelGrade → SaleAction
  | setNozzle : Bool → SaleAction
  | completeSale : SaleAction
  deriving Repr

test "sale lifecycle state machine" := do
  let result ← runSpider do
    -- Single event for all state-affecting actions
    let (actionEvent, dispatch) ← newTriggerEvent (t := Spider) (a := SaleAction)

    -- Sale state machine
    -- Idle → Ready (when grade selected)
    -- Ready → Pumping (when nozzle lifted)
    -- Pumping → Complete (when nozzle returned)
    -- Complete → Idle (when sale completed)
    let stateDyn ← foldDyn (fun action state =>
      match action with
      | .selectGrade _ =>
        if state == SaleState.idle then SaleState.ready else state
      | .setNozzle nozzleUp =>
        if state == SaleState.ready && nozzleUp then SaleState.pumping
        else if state == SaleState.pumping && !nozzleUp then SaleState.complete
        else state
      | .completeSale =>
        SaleState.idle
    ) SaleState.idle actionEvent

    -- Walk through a sale
    let s0 ← sample stateDyn.current  -- Idle
    dispatch (.selectGrade .premium)
    let s1 ← sample stateDyn.current  -- Ready
    dispatch (.setNozzle true)
    let s2 ← sample stateDyn.current  -- Pumping
    dispatch (.setNozzle false)
    let s3 ← sample stateDyn.current  -- Complete
    dispatch .completeSale
    let s4 ← sample stateDyn.current  -- Back to Idle

    pure (s0, s1, s2, s3, s4)

  shouldBe result (SaleState.idle, SaleState.ready, SaleState.pumping,
                   SaleState.complete, SaleState.idle)

test "full gas pump session with display updates" := do
  let result ← runSpider do
    -- All the events
    let (gradeSelectEvent, selectGrade) ← newTriggerEvent (t := Spider) (a := FuelGrade)
    let (nozzleEvent, setNozzle) ← newTriggerEvent (t := Spider) (a := Bool)
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)

    -- Core state
    let gradeDyn ← holdDyn FuelGrade.regular gradeSelectEvent
    let nozzleDyn ← holdDyn false nozzleEvent

    -- Price switches with grade selection
    let priceBehavior := gradeDyn.current.map FuelGrade.price

    -- Gate flow on nozzle
    let gatedFlow ← Event.gateM nozzleDyn.current flowEvent
    let gallonsDyn ← foldDyn (· + ·) 0.0 gatedFlow

    -- Cost derived from gallons and current price
    let costBehavior := Behavior.zipWith (· * ·) gallonsDyn.current priceBehavior

    -- Simple state: pumping when nozzle is up and gallons > 0
    let stateBehavior := Behavior.zipWith (fun nozzle gallons =>
      if nozzle && gallons > 0.0 then SaleState.pumping
      else if gallons > 0.0 then SaleState.complete
      else if nozzle then SaleState.ready
      else SaleState.idle
    ) nozzleDyn.current gallonsDyn.current

    -- Track all display updates when gallons change
    let displaysRef ← SpiderM.liftIO <| IO.mkRef ([] : List DisplayUpdate)
    let _ ← gallonsDyn.updated.subscribe fun gallons => do
      let grade ← gradeDyn.current.sample
      let cost ← costBehavior.sample
      let state ← stateBehavior.sample
      displaysRef.modify (· ++ [{ gallons, cost, grade, state }])

    -- Full session
    selectGrade FuelGrade.plus  -- Select Plus ($3.80)
    setNozzle true               -- Lift nozzle
    pumpFlow 2.0                 -- Pump 2 gallons
    pumpFlow 3.0                 -- Pump 3 more
    setNozzle false              -- Return nozzle

    -- Get final values
    let finalGallons ← sample gallonsDyn.current
    let finalCost ← sample costBehavior
    let displays ← SpiderM.liftIO displaysRef.get

    pure (finalGallons, finalCost, displays.length)

  -- 5 gallons at $3.80 = $19.00, 2 display updates
  assertFloatEq 5.0 result.1
  assertFloatEq 19.0 result.2.1
  shouldBe result.2.2 2  -- Two flow events = two display updates

/-! ## Form Validation Example -/

testSuite "Form Validation"

/-- Simple email validation -/
def isValidEmail (s : String) : Bool :=
  s.contains '@' && s.contains '.'

/-- Password validation: at least 8 chars -/
def isValidPassword (s : String) : Bool :=
  s.length >= 8

/-- Username validation: 3-20 chars -/
def isValidUsername (s : String) : Bool :=
  s.length >= 3 && s.length <= 20

test "form validates individual fields" := do
  let result ← runSpider do
    let (usernameEvent, setUsername) ← newTriggerEvent (t := Spider) (a := String)
    let (emailEvent, setEmail) ← newTriggerEvent (t := Spider) (a := String)
    let (passwordEvent, setPassword) ← newTriggerEvent (t := Spider) (a := String)

    let usernameDyn ← holdDyn "" usernameEvent
    let emailDyn ← holdDyn "" emailEvent
    let passwordDyn ← holdDyn "" passwordEvent

    let usernameValid := usernameDyn.current.map isValidUsername
    let emailValid := emailDyn.current.map isValidEmail
    let passwordValid := passwordDyn.current.map isValidPassword

    let allValid := Behavior.zipWith (· && ·)
      (Behavior.zipWith (· && ·) usernameValid emailValid)
      passwordValid

    let v0 ← sample allValid

    setUsername "john_doe"
    let v1 ← sample allValid

    setEmail "john@example.com"
    let v2 ← sample allValid

    setPassword "secretpass123"
    let v3 ← sample allValid

    pure (v0, v1, v2, v3)

  shouldBe result (false, false, false, true)

test "form submit only fires when valid" := do
  let result ← runSpider do
    let (emailEvent, setEmail) ← newTriggerEvent (t := Spider) (a := String)
    let (submitEvent, clickSubmit) ← newTriggerEvent (t := Spider) (a := Unit)

    let emailDyn ← holdDyn "" emailEvent
    let emailValid := emailDyn.current.map isValidEmail

    let validSubmit ← Event.gateM emailValid submitEvent

    let submitsRef ← SpiderM.liftIO <| IO.mkRef (0 : Nat)
    let _ ← validSubmit.subscribe fun _ =>
      submitsRef.modify (· + 1)

    clickSubmit ()
    let s1 ← SpiderM.liftIO submitsRef.get

    setEmail "valid@email.com"
    clickSubmit ()
    let s2 ← SpiderM.liftIO submitsRef.get

    pure (s1, s2)

  shouldBe result (0, 1)

test "form shows error messages reactively" := do
  let result ← runSpider do
    let (emailEvent, setEmail) ← newTriggerEvent (t := Spider) (a := String)
    let emailDyn ← holdDyn "" emailEvent

    let errorMessage := emailDyn.current.map fun email =>
      if email.isEmpty then "Email is required"
      else if !isValidEmail email then "Invalid email format"
      else ""

    let e0 ← sample errorMessage
    setEmail "bad"
    let e1 ← sample errorMessage
    setEmail "good@email.com"
    let e2 ← sample errorMessage

    pure (e0, e1, e2)

  shouldBe result ("Email is required", "Invalid email format", "")

/-! ## Bank Account Example -/

testSuite "Bank Account"

inductive Transaction where
  | deposit : Float → Transaction
  | withdraw : Float → Transaction
  deriving Repr, BEq

test "bank account tracks balance" := do
  let result ← runSpider do
    let (txEvent, submitTx) ← newTriggerEvent (t := Spider) (a := Transaction)

    let balanceDyn ← foldDyn (fun tx bal =>
      match tx with
      | .deposit amt => bal + amt
      | .withdraw amt => bal - amt
    ) 0.0 txEvent

    submitTx (Transaction.deposit 100.0)
    submitTx (Transaction.deposit 50.0)
    submitTx (Transaction.withdraw 30.0)

    sample balanceDyn.current

  assertFloatEq 120.0 result

test "bank account prevents overdraft" := do
  let result ← runSpider do
    let (txEvent, submitTx) ← newTriggerEvent (t := Spider) (a := Transaction)

    let approvedBalanceDyn ← foldDyn (fun tx bal =>
      match tx with
      | .deposit amt => bal + amt
      | .withdraw amt => if decide (bal >= amt) then bal - amt else bal
    ) 100.0 txEvent

    submitTx (Transaction.deposit 50.0)
    let b1 ← sample approvedBalanceDyn.current

    submitTx (Transaction.withdraw 30.0)
    let b2 ← sample approvedBalanceDyn.current

    submitTx (Transaction.withdraw 200.0)
    let b3 ← sample approvedBalanceDyn.current

    pure (b1, b2, b3)

  assertFloat3Eq (150.0, 120.0, 120.0) result

test "bank account maintains transaction history" := do
  let result ← runSpider do
    let (txEvent, submitTx) ← newTriggerEvent (t := Spider) (a := Transaction)

    let historyDyn ← foldDyn (fun tx history => tx :: history) [] txEvent

    submitTx (Transaction.deposit 100.0)
    submitTx (Transaction.withdraw 25.0)
    submitTx (Transaction.deposit 50.0)

    let history ← sample historyDyn.current
    pure history.length

  shouldBe result 3

/-! ## Shopping Cart Example -/

testSuite "Shopping Cart"

structure CartItem where
  name : String
  price : Float
  quantity : Nat
  deriving Repr, BEq

inductive CartAction where
  | addItem : String → Float → CartAction
  | removeItem : String → CartAction
  | updateQuantity : String → Nat → CartAction
  | applyDiscount : Float → CartAction
  deriving Repr

test "shopping cart calculates total" := do
  let result ← runSpider do
    let (actionEvent, dispatch) ← newTriggerEvent (t := Spider) (a := CartAction)

    let cartDyn ← foldDyn (fun action cart =>
      match action with
      | .addItem name price =>
        match cart.find? (·.name == name) with
        | some _ => cart.map fun i =>
            if i.name == name then { i with quantity := i.quantity + 1 } else i
        | none => cart ++ [{ name, price, quantity := 1 }]
      | .removeItem name => cart.filter (·.name != name)
      | .updateQuantity name qty =>
        if qty == 0 then cart.filter (·.name != name)
        else cart.map fun i => if i.name == name then { i with quantity := qty } else i
      | .applyDiscount _ => cart
    ) ([] : List CartItem) actionEvent

    let subtotal := cartDyn.current.map fun items =>
      items.foldl (fun acc item => acc + item.price * item.quantity.toFloat) 0.0

    dispatch (.addItem "Widget" 10.0)
    dispatch (.addItem "Gadget" 25.0)
    dispatch (.addItem "Widget" 10.0)

    sample subtotal

  assertFloatEq 45.0 result

test "shopping cart applies discount" := do
  let result ← runSpider do
    let (actionEvent, dispatch) ← newTriggerEvent (t := Spider) (a := CartAction)

    let discountDyn ← foldDyn (fun action disc =>
      match action with
      | .applyDiscount pct => pct
      | _ => disc
    ) 0.0 actionEvent

    let cartDyn ← foldDyn (fun action cart =>
      match action with
      | .addItem name price => cart ++ [{ name, price, quantity := 1 : CartItem }]
      | _ => cart
    ) ([] : List CartItem) actionEvent

    let subtotal := cartDyn.current.map fun items =>
      items.foldl (fun acc item => acc + item.price * item.quantity.toFloat) 0.0

    let finalTotal := Behavior.zipWith (fun sub disc => sub * (1.0 - disc)) subtotal discountDyn.current

    dispatch (.addItem "Expensive Thing" 100.0)
    let t1 ← sample finalTotal

    dispatch (.applyDiscount 0.20)
    let t2 ← sample finalTotal

    pure (t1, t2)

  assertFloat2Eq (100.0, 80.0) result

test "shopping cart updates quantities" := do
  let result ← runSpider do
    let (actionEvent, dispatch) ← newTriggerEvent (t := Spider) (a := CartAction)

    let cartDyn ← foldDyn (fun action cart =>
      match action with
      | .addItem name price => cart ++ [{ name, price, quantity := 1 : CartItem }]
      | .updateQuantity name qty =>
        if qty == 0 then cart.filter (·.name != name)
        else cart.map fun i => if i.name == name then { i with quantity := qty } else i
      | _ => cart
    ) ([] : List CartItem) actionEvent

    let itemCount := cartDyn.current.map fun items =>
      items.foldl (fun acc item => acc + item.quantity) 0

    dispatch (.addItem "Apple" 1.0)
    dispatch (.addItem "Banana" 0.50)
    let c1 ← sample itemCount

    dispatch (.updateQuantity "Apple" 5)
    let c2 ← sample itemCount

    dispatch (.updateQuantity "Banana" 0)
    let c3 ← sample itemCount

    pure (c1, c2, c3)

  shouldBe result (2, 6, 5)

test "shopping cart calculates tax" := do
  let result ← runSpider do
    let taxRate : Float := 0.08

    let (actionEvent, dispatch) ← newTriggerEvent (t := Spider) (a := CartAction)

    let cartDyn ← foldDyn (fun action cart =>
      match action with
      | .addItem name price => cart ++ [{ name, price, quantity := 1 : CartItem }]
      | _ => cart
    ) ([] : List CartItem) actionEvent

    let subtotal := cartDyn.current.map fun items =>
      items.foldl (fun acc item => acc + item.price * item.quantity.toFloat) 0.0

    let tax := subtotal.map (· * taxRate)
    let total := Behavior.zipWith (· + ·) subtotal tax

    dispatch (.addItem "Book" 20.0)
    dispatch (.addItem "Pen" 5.0)

    let sub ← sample subtotal
    let t ← sample tax
    let tot ← sample total

    pure (sub, t, tot)

  assertFloat3Eq (25.0, 2.0, 27.0) result


end ReactiveTests.IntegrationTests
