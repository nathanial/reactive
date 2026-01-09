import Lake
open Lake DSL

package reactive where
  version := v!"0.1.0"

require crucible from git "https://github.com/nathanial/crucible" @ "v0.0.7"
require plausible from git
  "https://github.com/leanprover-community/plausible.git" @ "v4.26.0"
require chronos from git "https://github.com/nathanial/chronos-lean" @ "v0.0.2"

@[default_target]
lean_lib Reactive where
  roots := #[`Reactive]

-- Test library
lean_lib ReactiveTests where
  globs := #[.submodules `ReactiveTests]

-- Test runner executable
@[test_driver]
lean_exe reactive_tests where
  root := `ReactiveTests.Main
