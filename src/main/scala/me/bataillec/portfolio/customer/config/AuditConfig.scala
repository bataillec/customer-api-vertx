package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

final case class AuditConfig(
    auditFilePath: String
)

object AuditConfig {
  def fromEnv(env: Map[String, String] = sys.env): AuditConfig = {
    AuditConfig(
      auditFilePath = env.str(AUDIT_FILE_PATH, DEFAULT_AUDIT_FILE_PATH)
    )
  }
}
