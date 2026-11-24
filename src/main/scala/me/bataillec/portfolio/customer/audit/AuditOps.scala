package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future
import io.vertx.core.json.JsonObject

trait AuditOps {

  /** Succeeds only when events can actually be written (Kafka is reachable). The value is the state of the outbox. */
  def health(): Future[JsonObject]
  def close(): Future[Void]
}
