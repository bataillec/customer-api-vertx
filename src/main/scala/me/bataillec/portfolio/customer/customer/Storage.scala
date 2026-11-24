package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlClient
import me.bataillec.portfolio.customer.domain.*

import java.time.Instant
import java.util.UUID

trait Storage {
  def inTransaction[T](f: SqlClient => Future[T]): Future[T]

  def ping(): Future[Unit]

  def insertCustomer(tx: SqlClient, customer: CreateCustomer): Future[Customer]

  def listCustomers(limit: Int, after: Option[(Instant, UUID)], status: Option[CustomerStatus]): Future[List[Customer]]

  def findCustomer(id: UUID): Future[Option[Customer]]

  def lockCustomerForUpdate(tx: SqlClient, customerId: UUID): Future[Option[CustomerStatus]]

  def updateCustomer(tx: SqlClient, customerId: UUID, updateCustomer: UpdateCustomer): Future[Customer]

  def cancelActiveSubscription(tx: SqlClient, customerId: UUID): Future[Unit]

  def findActivePlan(tx: SqlClient, customerId: UUID): Future[Option[PlanType]]

  def insertActiveSubscription(tx: SqlClient, customerId: UUID, createSubscription: CreateSubscription): Future[Subscription]

  def listPlans(): Future[List[Plan]]

  def findPlanQuotas(client: SqlClient, planType: PlanType): Future[Map[String, PlanQuota]]

  def applyPlan(tx: SqlClient, customerId: UUID, createSubscription: CreateSubscription, now: Instant): Future[List[Entitlement]]

  def listSubscriptions(customerId: UUID): Future[List[Subscription]]

  def listEntitlements(customerId: UUID): Future[List[Entitlement]]

  def findEntitlement(tx: SqlClient, customerId: UUID, featureKey: String): Future[Option[Entitlement]]

  def consume(tx: SqlClient, customerId: UUID, consumption: Consumption, now: Instant): Future[Option[ConsumeResult]]

  /** Writes an audit event on the transaction of the change it describes. */
  def insertOutbox(tx: SqlClient, customerId: UUID, payload: JsonObject): Future[Unit]
}
