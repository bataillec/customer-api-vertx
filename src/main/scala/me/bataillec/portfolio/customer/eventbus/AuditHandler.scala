package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.audit.AuditOperation
import me.bataillec.portfolio.customer.config.AuditConfig
import org.slf4j.LoggerFactory

class AuditHandler(vertx: Vertx, conf: AuditConfig) {
  private val logger     = LoggerFactory.getLogger(getClass)
  private val operations = new AuditOperation(conf)

  def handle(): Future[Unit] = {
    for {
      _ <- SenderUtils.pong(vertx, AUDIT_HEALTH_ADDRESS).completion()
      _ <- vertx
             .eventBus()
             .consumer(
               AUDIT_ADDRESS,
               (msg: Message[String]) =>
                 logger.info("An audit message has been received")
                 operations.audit(msg.body())
                 msg.reply("ACK")
                 ()
             )
             .completion()
    } yield ()
  }
}
