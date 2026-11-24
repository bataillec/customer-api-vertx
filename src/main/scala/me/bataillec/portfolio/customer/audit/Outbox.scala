package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlClient

import java.util.UUID

/** Records an audit event in the transactional outbox: the row is written on the transaction `tx` of the change it
  * describes, so it commits or rolls back with it. A relay publishes it to Kafka afterwards (see [[OutboxRelay]]).
  */
trait Outbox {

  /** The event envelope, not written yet: for a statement that writes it itself (see `Storage.consumeWithEvent`). */
  def prepare(eventType: String, customerId: UUID, extra: (String, String | Long)*): JsonObject

  /** An extra is a string, or an integer when the schema says so. A null value is left out. */
  def record(tx: SqlClient, eventType: String, customerId: UUID, extra: (String, String | Long)*): Future[Unit]
}
