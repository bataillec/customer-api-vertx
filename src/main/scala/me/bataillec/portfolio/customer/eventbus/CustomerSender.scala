package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future
import io.vertx.ext.web.RoutingContext

trait CustomerSender {
  def createCustomer(rc: RoutingContext): Future[String]

  def getCustomers(rc: RoutingContext): Future[String]

  def getPlans(rc: RoutingContext): Future[String]

  def getCustomer(rc: RoutingContext): Future[String]

  def updateCustomer(rc: RoutingContext): Future[String]

  def subscribe(rc: RoutingContext): Future[String]

  def getSubscriptions(rc: RoutingContext): Future[String]

  def getEntitlements(rc: RoutingContext): Future[String]

  def consumeEntitlement(rc: RoutingContext): Future[String]
}
