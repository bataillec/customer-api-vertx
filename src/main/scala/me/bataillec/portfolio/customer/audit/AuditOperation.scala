package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future
import io.vertx.core.json.JsonObject

import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

class AuditOperation(auditLogger: AuditLogger, relay: OutboxRelay, clock: Clock = Clock.systemUTC()) extends AuditOps {

  // Down only when Kafka is: the outbox backlog is information, and the database being unreachable is reported by the customer verticles
  override def health(): Future[JsonObject] =
    auditLogger.health().compose { _ =>
      relay
        .backlog()
        // A database that hangs must not hold the check up: it is not what this check is about
        .timeout(AuditOperation.BACKLOG_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .map { b =>
          val details = JsonObject().put("outbox_backlog", b.count)
          b.oldest.foreach(o => details.put("outbox_oldest_age_seconds", Duration.between(o, clock.instant()).getSeconds.max(0L)))
          details
        }
        .recover(_ => Future.succeededFuture(JsonObject().put("outbox_backlog", "unknown")))
    }

  override def close(): Future[Void] =
    relay.stop().transform(_ => auditLogger.close())
}

object AuditOperation {
  // Under HEALTH_PING_TIMEOUT_MS and the Kafka check, so that the answer still comes in time
  private[audit] val BACKLOG_TIMEOUT_MS = 1000L
}
