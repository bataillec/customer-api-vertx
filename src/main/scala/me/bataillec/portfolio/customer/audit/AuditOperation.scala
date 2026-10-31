package me.bataillec.portfolio.customer.audit

class AuditOperation(auditLogger: AuditLogger) extends AuditOps {

  override def audit(msg: String): Unit = {
    auditLogger.audit(msg)
  }

  override def close(): Unit = {
    auditLogger.close()
  }
}
