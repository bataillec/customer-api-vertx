package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlClient
import me.bataillec.portfolio.customer.customer.Storage

import java.time.Clock
import java.util.UUID

/** Writes the audit events as platform event envelopes v1 (see platform/contracts/events/envelope.v1.schema.json). */
class OutboxRecorder(
    storage: Storage,
    clock: Clock = Clock.systemUTC(),
    eventId: () => UUID = () => UUID.randomUUID()
) extends Outbox {

  override def record(tx: SqlClient, eventType: String, customerId: UUID, extra: (String, String | Long)*): Future[Unit] = {
    val event = JsonObject()
    // A null value carries no information: leave the key out rather than write a JSON null
    extra.foreach((k, v) => if v != null then event.put(k, v))
    // The core fields are written last so that an extra can never overwrite them
    event
      .put("event_id", eventId().toString)
      .put("event_type", eventType)
      .put("occurred_at", clock.instant().toString)
      .put("producer", OutboxRecorder.PRODUCER)
      .put("schema_version", OutboxRecorder.SCHEMA_VERSION)
      .put("customer_id", customerId.toString)
    storage.insertOutbox(tx, customerId, event)
  }
}

object OutboxRecorder {
  private val PRODUCER       = "customer-api"
  private val SCHEMA_VERSION = 1
}
