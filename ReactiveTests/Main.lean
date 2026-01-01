import Crucible
import ReactiveTests.EventTests
import ReactiveTests.BehaviorTests
import ReactiveTests.DynamicTests
import ReactiveTests.SwitchTests
import ReactiveTests.PropertyTests

open Crucible

def main : IO UInt32 := do
  IO.println "╔════════════════════════════════════════╗"
  IO.println "║       Reactive FRP Test Suite          ║"
  IO.println "╚════════════════════════════════════════╝"

  let exitCode ← runAllSuites

  IO.println ""
  if exitCode == 0 then
    IO.println "✓ All test suites passed!"
  else
    IO.println s!"✗ {exitCode} test suite(s) had failures"

  return exitCode
