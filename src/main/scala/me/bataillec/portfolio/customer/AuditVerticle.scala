package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.PoolOptions
import me.bataillec.portfolio.customer.Constants.AUDIT_HEALTH_ADDRESS
import me.bataillec.portfolio.customer.audit.*
import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.eventbus.SenderUtils
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.compiletime.uninitialized

/** Relays the audit events that the customer verticles write to the outbox table to Kafka (see [[OutboxRelay]]). */
class AuditVerticle extends VerticleBase {
  private val logger              = LoggerFactory.getLogger(getClass)
  private var operations: AuditOps = uninitialized

  override def start(): Future[Unit] = {
    val config                   = AuditConfig.fromEnv()
    val auditLogger: AuditLogger = KafkaAuditWriter(vertx, config)
    val connectOptions           = PgConnectOptions()
      .setHost(config.pg.pgHost)
      .setPort(config.pg.pgPort)
      .setDatabase(config.pg.pgDatabase)
      .setUser(config.pg.pgUser)
      .setPassword(config.pg.pgPassword)
    // Only for the backlog: the relay reads and writes on the connection that holds the leader lock. Nothing connects before it is used.
    val pool  = PgBuilder.pool().`with`(PoolOptions().setMaxSize(2)).connectingTo(connectOptions).using(vertx).build()
    val relay = new OutboxRelay(
      vertx,
      new PGOutboxStore(vertx, connectOptions, pool),
      auditLogger,
      config.outboxPollIntervalMs,
      config.outboxBatchSize,
      Duration.ofDays(config.outboxRetentionDays)
    )
    operations = new AuditOperation(auditLogger, relay)

    SenderUtils
      .pongWithDetails(vertx, AUDIT_HEALTH_ADDRESS, () => operations.health(), "Kafka unreachable")
      .map { _ =>
        relay.start()
        logger.info("Audit verticle is running")
      }
  }

  override def stop(): Future[Unit] = {
    if (operations != null) operations.close().mapEmpty[Unit]()
    else Future.succeededFuture()
  }
}
