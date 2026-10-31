package me.bataillec.portfolio.customer.eventbus

import java.util.UUID

trait AuditSender {
  def send(action: String, id: UUID, extra: (String, String)*): Unit
}
