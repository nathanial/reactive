/-
  Reactive/Class/PostBuild.lean

  Typeclass for monads that support post-build actions.
-/
import Reactive.Core

namespace Reactive

/-- Monad that supports running actions after the FRP network is fully built.

    This is useful for:
    - Initialization that depends on the full network being set up
    - Triggering initial events
    - Starting external event sources -/
class PostBuild (t : Type) (m : Type → Type) where
  /-- Get an event that fires once after the network is fully constructed -/
  getPostBuild : m (Event t Unit)

export PostBuild (getPostBuild)

end Reactive
