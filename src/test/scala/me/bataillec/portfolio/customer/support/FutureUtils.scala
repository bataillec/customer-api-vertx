package me.bataillec.portfolio.customer.support

import io.vertx.core.Future

import java.util.concurrent.{ExecutionException, TimeUnit}

object FutureUtils {

  /** Waits for the future, at most `seconds`, and returns its value. A failed future throws its cause. */
  def await[T](future: Future[T], seconds: Int = 5): T =
    try future.toCompletionStage.toCompletableFuture.get(seconds, TimeUnit.SECONDS)
    catch { case e: ExecutionException => throw e.getCause }

  /** Waits for the future, at most `seconds`, and returns its failure. Throws when the future succeeds. */
  def failureOf(future: Future[?], seconds: Int = 5): Throwable =
    try {
      future.toCompletionStage.toCompletableFuture.get(seconds, TimeUnit.SECONDS)
      throw new AssertionError("The future was expected to fail")
    } catch { case e: ExecutionException => e.getCause }
}
