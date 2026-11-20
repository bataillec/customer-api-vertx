package me.bataillec.portfolio.customer.e2e

import io.cucumber.scala.{EN, ScalaDsl}

/** Starts the platform on the first scenario, gives each scenario a clean slate, and repairs what the scenarios that
  * break the infrastructure (tagged `@destructive`) left behind, so that the next one finds a healthy platform.
  */
class Hooks extends ScalaDsl with EN {

  Before {
    World.reset()
    World.stack // starts the platform the first time
    ()
  }

  After("@destructive") {
    val stack = World.stack
    World.paused.foreach(stack.unpause)
    World.stopped.foreach(stack.startAgain)
    Waiting.until("the platform to be ready again", seconds = 180) {
      World.api.get("/health/ready").status == 200
    }
  }

  After {
    World.closeProbe()
  }
}
