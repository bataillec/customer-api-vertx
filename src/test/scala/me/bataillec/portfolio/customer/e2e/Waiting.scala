package me.bataillec.portfolio.customer.e2e

/** Polling for the things that take a moment on a real platform: a node leaving the cluster, a health check noticing. */
object Waiting {

  /** Retries `condition` every half second until it holds. Fails with `what` when `seconds` have passed.
    * A condition that throws (a connection refused while a node restarts) counts as not holding yet.
    */
  def until(what: String, seconds: Int)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + seconds * 1_000_000_000L
    var holds    = attempt(condition)
    while (!holds && System.nanoTime() < deadline) {
      Thread.sleep(500)
      holds = attempt(condition)
    }
    if (!holds) throw new AssertionError(s"Gave up after $seconds seconds waiting for $what")
  }

  private def attempt(condition: => Boolean): Boolean =
    try condition
    catch { case _: Exception => false }
}
