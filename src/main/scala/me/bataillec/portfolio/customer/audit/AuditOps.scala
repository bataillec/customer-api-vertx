package me.bataillec.portfolio.customer.audit

trait AuditOps {
  def audit(msg: String): Unit
  def close(): Unit
}
