package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future
import io.vertx.core.eventbus.{DeliveryOptions, EventBus, Message}
import io.vertx.ext.web.RoutingContext
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.EventbusUtils.*

class CustomerSender(eventbus: EventBus) {
  def createCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "createCustomer")
    val data            = getBodyAsJson(rc).encode()
    addOptionalHeaderParam(deliveryOptions, rc, "Idempotency-Key")

    eventbus.sendFuture[String]("customer", Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def getCustomers(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "listCustomers")

    addOptionalQueryParam(deliveryOptions, rc, "limit", Some("50"))
    addOptionalQueryParam(deliveryOptions, rc, "cursor")
    addOptionalQueryParam(deliveryOptions, rc, "status")

    eventbus.sendFuture[String]("customer", None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def getCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "getCustomer")

    addPathParam(deliveryOptions, rc, "id")

    eventbus.sendFuture[String]("customer", None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def updateCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "updateCustomer")
    val data            = getBodyAsJson(rc)
    addHeaderParam(deliveryOptions, rc, "If-Match")
    addPathParam(deliveryOptions, rc, "id")

    eventbus.sendFuture[String]("customer", Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def subscribe(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "subscribe")
    addOptionalHeaderParam(deliveryOptions, rc, "Idempotency-Key")
    addPathParam(deliveryOptions, rc, "id")
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String]("customer", Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def getSubscriptions(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "listSubscriptions")

    addPathParam(deliveryOptions, rc, "id")

    eventbus.sendFuture[String]("customer", None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def getEntitlements(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "listEntitlements")

    addPathParam(deliveryOptions, rc, "id")

    eventbus.sendFuture[String]("customer", None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def consumeEntitlement(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, "consumeEntitlement")
    addOptionalHeaderParam(deliveryOptions, rc, "Idempotency-Key")
    addPathParam(deliveryOptions, rc, "id")
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String]("customer", Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }
}
