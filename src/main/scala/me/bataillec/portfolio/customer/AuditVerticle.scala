package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import me.bataillec.portfolio.customer.audit.{AuditLogger, AuditOperation, AuditOps, KafkaAuditWriter}
import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.eventbus.{AuditHandler, AuditHandlerImpl}
import org.slf4j.LoggerFactory

import scala.compiletime.uninitialized

class AuditVerticle extends VerticleBase {
  private val logger                = LoggerFactory.getLogger(getClass)
  private var handler: AuditHandler = uninitialized

  override def start(): Future[Unit] = {
    val config                   = AuditConfig.fromEnv()
    val auditLogger: AuditLogger = KafkaAuditWriter(vertx, config)
    val operations: AuditOps     = new AuditOperation(auditLogger)

    handler = new AuditHandlerImpl(vertx, operations)
    val handling = handler.handle()

    logger.info("Audit verticle is running")

    handling
  }

  override def stop(): Future[Unit] = {
    if (handler != null) {
      handler.close().mapEmpty()
    } else Future.succeededFuture()
  }
}
