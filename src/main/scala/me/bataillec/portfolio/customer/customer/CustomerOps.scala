package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import me.bataillec.portfolio.customer.domain.*

import java.util.UUID

trait CustomerOps {
  def ping(): Future[Unit]
  def createCustomer(createCustomer: CreateCustomer): Future[Customer]
  def listCustomers(limit: Int, maybeCursor: Option[String], maybeStatus: Option[CustomerStatus]): Future[ListCustomers]
  def getCustomer(customerId: UUID): Future[Customer]
  def updateCustomer(customerId: UUID, updateCustomer: UpdateCustomer): Future[Customer]
  def createSubscription(customerId: UUID, createSubscription: CreateSubscription): Future[SubscriptionWithEntitlements]
  def listPlans(): Future[List[Plan]]
  def listSubscriptions(customerId: UUID): Future[List[Subscription]]
  def listEntitlements(customerId: UUID): Future[List[Entitlement]]
  def consumeEntitlement(customerId: UUID, consumption: Consumption): Future[ConsumeResult]

}
