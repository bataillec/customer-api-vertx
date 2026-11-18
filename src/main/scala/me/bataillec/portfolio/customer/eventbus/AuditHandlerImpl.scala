package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.audit.AuditOps
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

class AuditHandlerImpl(vertx: Vertx, operations: AuditOps) extends AuditHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(): Future[Unit] = {
    for {
      _ <- SenderUtils.pong(vertx, AUDIT_HEALTH_ADDRESS).completion()
      _ <- vertx
             .eventBus()
             .consumer(
               AUDIT_ADDRESS,
               (msg: Message[String]) =>
                 logger.info("An audit message has been received")
                 try {
                   operations
                     .audit(msg.body())
                     .onFailure(exception => logger.error("Failed to write the audit event: {}", msg.body(), exception))
                   ()
                 } catch {
                   case e: InterruptedException =>
                     // Try does not catch it: restore the flag so the thread can still be stopped
                     Thread.currentThread().interrupt()
                     logger.error("Interrupted while writing the audit event: {}", msg.body(), e)
                   case NonFatal(e)             =>
                     logger.error("Failed to write the audit event: {}", msg.body(), e)
                 }
             )
             .completion()
    } yield ()
  }

  // Shutdown must never throw: a failure is logged and the close is reported as done
  override def close(): Future[Void] = {
    try {
      operations.close().recover { e =>
        logger.error("Failed to close the audit operations", e)
        Future.succeededFuture[Void]()
      }
    } catch {
      case NonFatal(e) =>
        logger.error("Failed to close the audit operations", e)
        Future.succeededFuture[Void]()
    }
  }
}
