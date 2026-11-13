package me.bataillec.portfolio.customer.audit

import io.vertx.core.Future

class AuditOperation(auditLogger: AuditLogger) extends AuditOps {

  override def audit(msg: String): Future[Void] = {
    auditLogger.audit(msg)
  }

  override def close(): Future[Void] = {
    auditLogger.close()
  }
}
