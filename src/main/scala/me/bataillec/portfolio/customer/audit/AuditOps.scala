package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future

trait AuditOps {
  def audit(msg: String): Future[Void]
  def close(): Future[Void]
}
