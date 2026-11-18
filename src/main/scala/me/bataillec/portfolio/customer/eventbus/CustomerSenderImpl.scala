package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future
import io.vertx.core.eventbus.{DeliveryOptions, EventBus, Message}
import io.vertx.ext.web.RoutingContext
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.EventbusUtils.*

import scala.util.control.NonFatal

class CustomerSenderImpl(eventbus: EventBus) extends CustomerSender {
  // An invalid body or a missing path param is reported as a failed Future, never as a synchronous throw
  private def guarded(f: => Future[String]): Future[String] =
    try f
    catch { case NonFatal(e) => Future.failedFuture[String](e) }

  override def createCustomer(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, createCustomer_ID)
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  override def getCustomers(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listCustomers_ID)

    addOptionalQueryParam(deliveryOptions, rc, LIMIT_QUERY_PARAM, Some(LIMIT_DEFAULT_VALUE))
    addOptionalQueryParam(deliveryOptions, rc, CURSOR_QUERY_PARAM)
    addOptionalQueryParam(deliveryOptions, rc, STATUS_QUERY_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  override def getPlans(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listPlans_ID)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  override def getCustomer(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, getCustomer_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  override def updateCustomer(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, updateCustomer_ID)
    val data            = getBodyAsJson(rc).encode()
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body()
    }
  }

  override def subscribe(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, subscribe_ID)
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  override def getSubscriptions(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listSubscriptions_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  override def getEntitlements(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, listEntitlements_ID)

    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, None, deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }

  override def consumeEntitlement(rc: RoutingContext): Future[String] = guarded {
    val deliveryOptions = new DeliveryOptions().addHeader(OPERATION_ID, consumeEntitlement_ID)
    addPathParam(deliveryOptions, rc, ID_PATH_PARAM)
    val data            = getBodyAsJson(rc).encode()

    eventbus.sendFuture[String](CUSTOMER_ADDRESS, Some(data), deliveryOptions).map { (r: Message[String]) =>
      r.body
    }
  }
}
