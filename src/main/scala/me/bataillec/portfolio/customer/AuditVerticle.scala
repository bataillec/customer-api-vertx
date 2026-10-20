package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.eventbus.AuditHandler
import org.slf4j.LoggerFactory

class AuditVerticle extends VerticleBase {
  private val logger = LoggerFactory.getLogger(getClass)

  override def start(): Future[_] = {
    val config = AuditConfig.fromEnv()

    val handler = new AuditHandler(vertx, config)

    val handling = handler.handle()

    logger.info("Audit verticle is running")

    handling
  }
}
