package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.audit.AuditOps
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

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
                 Try(operations.audit(msg.body())) match {
                   case Success(_)         => ()
                   case Failure(exception) =>
                     logger.error("Failed to write the audit event: {}", msg.body(), exception)
                 }
             )
             .completion()
    } yield ()
  }

  override def close(): Unit = {
    operations.close()
  }
}
