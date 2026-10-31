package me.bataillec.portfolio.customer.audit

trait AuditLogger {

  def close(): Unit

  def audit(msg: String): Unit
}
