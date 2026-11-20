package me.bataillec.portfolio.customer.e2e

import me.bataillec.portfolio.customer.support.{ApiClient, ApiResponse, AuditProbe, PlatformStack}

import scala.collection.mutable

/** What the steps of one scenario share: the customer they talk about, the last answer of the API, and what they did
  * to the infrastructure, so that the hooks can undo it. It is reset before every scenario.
  */
object World {
  var customerId: Option[String] = None
  var email: Option[String]      = None
  var last: Option[ApiResponse]  = None

  val stopped: mutable.Set[String] = mutable.Set.empty
  val paused: mutable.Set[String]  = mutable.Set.empty

  private var auditProbe: Option[AuditProbe] = None

  def stack: PlatformStack = PlatformStack.get
  def api: ApiClient       = stack.api

  def customer: String = customerId.getOrElse(throw new AssertionError("The scenario has no customer yet: a step must register one first"))

  def response: ApiResponse = last.getOrElse(throw new AssertionError("The scenario has not called the API yet"))

  /** Reads the audit topic of the platform; created on first use and closed after the scenario. */
  def probe: AuditProbe = {
    if (auditProbe.isEmpty) auditProbe = Some(new AuditProbe(stack.kafkaBootstrap, stack.auditTopic))
    auditProbe.get
  }

  def reset(): Unit = {
    closeProbe()
    customerId = None
    email = None
    last = None
    stopped.clear()
    paused.clear()
  }

  def closeProbe(): Unit = {
    auditProbe.foreach(_.close())
    auditProbe = None
  }
}
