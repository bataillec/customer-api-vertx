package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.EventBus
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.Constants.AUDIT_ADDRESS

import java.time.Clock
import java.util.UUID

/** Publishes audit events as platform event envelopes v1 (see platform/contracts/events/envelope.v1.schema.json). */
class AuditSenderImpl(
    eventbus: EventBus,
    clock: Clock = Clock.systemUTC(),
    eventId: () => UUID = () => UUID.randomUUID()
) extends AuditSender {
  override def send(action: String, id: UUID, extra: (String, String)*): Unit = {
    val event = JsonObject()
    // A null value carries no information: leave the key out rather than write a JSON null
    extra.foreach((k, v) => if v != null then event.put(k, v))
    // The core fields are written last so that an extra can never overwrite them
    event
      .put("event_id", eventId().toString)
      .put("event_type", action)
      .put("occurred_at", clock.instant().toString)
      .put("producer", AuditSenderImpl.PRODUCER)
      .put("schema_version", AuditSenderImpl.SCHEMA_VERSION)
      .put("customer_id", id.toString)
    // The audit consumer expects a String body
    eventbus.publish(AUDIT_ADDRESS, event.encode())
  }
}

object AuditSenderImpl {
  private val PRODUCER       = "customer-api"
  private val SCHEMA_VERSION = 1
}
