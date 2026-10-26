package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.Constants.AUDIT_ADDRESS

import java.util.UUID

class AuditSender(vertx: Vertx) {
  def send(action: String, id: UUID, extra: (String, String)*): Unit = {
    val event = JsonObject()
      .put("action", action)
      .put("customer_id", id.toString)
    extra.foreach((k, v) => event.put(k, v))
    // The audit consumer expects a String body
    vertx.eventBus().publish(AUDIT_ADDRESS, event.encode())
  }
}
