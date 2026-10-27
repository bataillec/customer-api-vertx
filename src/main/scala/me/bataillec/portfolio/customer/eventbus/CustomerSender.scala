package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future
import io.vertx.core.eventbus.{DeliveryOptions, EventBus, Message}
import io.vertx.ext.web.RoutingContext
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.EventbusUtils.*

class CustomerSender(eventbus: EventBus) {
  def createCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, createCustomer_ID)
    val data            = getBodyAsJson(rc).encode()
    addOptionalHeaderParam(deliveryOptions, rc, IDEMPOTENCY_KEY_HEADER)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def getCustomers(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listCustomers_ID)

    addOptionalQueryParam(deliveryOptions, rc, LIMIT_QUERY_PARAM, Some(LIMIT_DEFAULT_VALUE))
    addOptionalQueryParam(deliveryOptions, rc, CURSOR_QUERY_PARAM)
    addOptionalQueryParam(deliveryOptions, rc, STATUS_QUERY_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def getCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, getCustomer_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def updateCustomer(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, updateCustomer_ID)
    val data            = getBodyAsJson(rc).encode()
    addHeaderParam(deliveryOptions, rc, IF_MATCH_HEADER)
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  def subscribe(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, subscribe_ID)
    addOptionalHeaderParam(deliveryOptions, rc, IDEMPOTENCY_KEY_HEADER)
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def getSubscriptions(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listSubscriptions_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def getEntitlements(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listEntitlements_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  def consumeEntitlement(rc: RoutingContext): Future[String] = {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, consumeEntitlement_ID)
    addOptionalHeaderParam(deliveryOptions, rc, IDEMPOTENCY_KEY_HEADER)
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }
}
