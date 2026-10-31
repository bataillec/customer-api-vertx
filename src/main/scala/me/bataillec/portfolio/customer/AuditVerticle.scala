package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import me.bataillec.portfolio.customer.audit.{AuditLogger, AuditOperation, AuditOps, FileAuditWriter}
import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.eventbus.{AuditHandler, AuditHandlerImpl}
import org.slf4j.LoggerFactory

class AuditVerticle extends VerticleBase {
  private val logger                = LoggerFactory.getLogger(getClass)
  private var handler: AuditHandler = _

  override def start(): Future[_] = {
    val config                   = AuditConfig.fromEnv()
    val auditLogger: AuditLogger = new FileAuditWriter(config.auditFilePath)
    val operations: AuditOps     = new AuditOperation(auditLogger)

    handler = new AuditHandlerImpl(vertx, operations)
    val handling = handler.handle()

    logger.info("Audit verticle is running")

    handling
  }

  override def stop(): Future[_] = {
    if (handler != null) {
      Future.succeededFuture(handler.close())
    } else Future.succeededFuture()
  }
}
