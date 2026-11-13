package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future

trait AuditLogger {

  def close(): Future[Void]

  def audit(msg: String): Future[Void]
}
