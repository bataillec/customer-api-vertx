package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future

trait AuditHandler {
  def handle(): Future[Unit]

  def close(): Future[Void]
}
