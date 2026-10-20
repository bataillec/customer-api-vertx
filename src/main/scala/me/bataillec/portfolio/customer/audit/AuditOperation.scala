package me.bataillec.portfolio.customer.audit

import me.bataillec.portfolio.customer.config.AuditConfig
import org.slf4j.LoggerFactory

class AuditOperation(conf: AuditConfig) {
  AuditLogger.init(conf.auditFilePath)
  private val logger = LoggerFactory.getLogger(getClass)

  def audit(msg: String): Unit = {
    AuditLogger.audit(msg)
  }
}
