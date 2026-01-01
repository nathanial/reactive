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

/-! ## Gas Pump Example -/

testSuite "Gas Pump"

test "gas pump accumulates gallons and calculates cost" := do
  let result ← runSpider do
    let pricePerGallon : Behavior Spider Float := Behavior.constant 3.50
    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent
    let costBehavior := Behavior.zipWith (· * ·) gallonsDyn.current pricePerGallon

    SpiderM.liftIO <| pumpFlow 1.5
    SpiderM.liftIO <| pumpFlow 2.0
    SpiderM.liftIO <| pumpFlow 0.5

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
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0

    sample actualGallonsDyn.current

  assertFloatEq 4.0 result

test "gas pump tracks cost in real-time" := do
  let result ← runSpider do
    let pricePerGallon : Float := 3.00

    let (flowEvent, pumpFlow) ← newTriggerEvent (t := Spider) (a := Float)
    let gallonsDyn ← foldDyn (· + ·) 0.0 flowEvent

    let costUpdatesRef ← SpiderM.liftIO <| IO.mkRef ([] : List Float)
    let costEvent ← Event.map' gallonsDyn.updated (· * pricePerGallon)
    let _ ← SpiderM.liftIO <| costEvent.subscribe fun cost =>
      costUpdatesRef.modify (· ++ [cost])

    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0
    SpiderM.liftIO <| pumpFlow 1.0

    SpiderM.liftIO costUpdatesRef.get

  assertFloatListEq [3.0, 6.0, 9.0] result

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

    SpiderM.liftIO <| setUsername "john_doe"
    let v1 ← sample allValid

    SpiderM.liftIO <| setEmail "john@example.com"
    let v2 ← sample allValid

    SpiderM.liftIO <| setPassword "secretpass123"
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
    let _ ← SpiderM.liftIO <| validSubmit.subscribe fun _ =>
      submitsRef.modify (· + 1)

    SpiderM.liftIO <| clickSubmit ()
    let s1 ← SpiderM.liftIO submitsRef.get

    SpiderM.liftIO <| setEmail "valid@email.com"
    SpiderM.liftIO <| clickSubmit ()
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
    SpiderM.liftIO <| setEmail "bad"
    let e1 ← sample errorMessage
    SpiderM.liftIO <| setEmail "good@email.com"
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

    SpiderM.liftIO <| submitTx (Transaction.deposit 100.0)
    SpiderM.liftIO <| submitTx (Transaction.deposit 50.0)
    SpiderM.liftIO <| submitTx (Transaction.withdraw 30.0)

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

    SpiderM.liftIO <| submitTx (Transaction.deposit 50.0)
    let b1 ← sample approvedBalanceDyn.current

    SpiderM.liftIO <| submitTx (Transaction.withdraw 30.0)
    let b2 ← sample approvedBalanceDyn.current

    SpiderM.liftIO <| submitTx (Transaction.withdraw 200.0)
    let b3 ← sample approvedBalanceDyn.current

    pure (b1, b2, b3)

  assertFloat3Eq (150.0, 120.0, 120.0) result

test "bank account maintains transaction history" := do
  let result ← runSpider do
    let (txEvent, submitTx) ← newTriggerEvent (t := Spider) (a := Transaction)

    let historyDyn ← foldDyn (fun tx history => tx :: history) [] txEvent

    SpiderM.liftIO <| submitTx (Transaction.deposit 100.0)
    SpiderM.liftIO <| submitTx (Transaction.withdraw 25.0)
    SpiderM.liftIO <| submitTx (Transaction.deposit 50.0)

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

    SpiderM.liftIO <| dispatch (.addItem "Widget" 10.0)
    SpiderM.liftIO <| dispatch (.addItem "Gadget" 25.0)
    SpiderM.liftIO <| dispatch (.addItem "Widget" 10.0)

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

    SpiderM.liftIO <| dispatch (.addItem "Expensive Thing" 100.0)
    let t1 ← sample finalTotal

    SpiderM.liftIO <| dispatch (.applyDiscount 0.20)
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

    SpiderM.liftIO <| dispatch (.addItem "Apple" 1.0)
    SpiderM.liftIO <| dispatch (.addItem "Banana" 0.50)
    let c1 ← sample itemCount

    SpiderM.liftIO <| dispatch (.updateQuantity "Apple" 5)
    let c2 ← sample itemCount

    SpiderM.liftIO <| dispatch (.updateQuantity "Banana" 0)
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

    SpiderM.liftIO <| dispatch (.addItem "Book" 20.0)
    SpiderM.liftIO <| dispatch (.addItem "Pen" 5.0)

    let sub ← sample subtotal
    let t ← sample tax
    let tot ← sample total

    pure (sub, t, tot)

  assertFloat3Eq (25.0, 2.0, 27.0) result

#generate_tests

end ReactiveTests.IntegrationTests
