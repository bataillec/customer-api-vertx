package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future
import io.vertx.core.eventbus.{DeliveryOptions, EventBus, Message}
import io.vertx.core.json.{DecodeException, JsonObject}
import io.vertx.ext.web.RoutingContext

object EventbusUtils {
  implicit class MyEventBus(val eb: EventBus) {
    def sendFuture[T](address: String, message: scala.Option[AnyRef], options: DeliveryOptions): Future[Message[T]] = {
      eb.request[T](address, message.orNull, options)
    }
  }

  def getBodyAsJson(rc: RoutingContext): JsonObject = {
    try {
      Option(rc.body.asJsonObject).getOrElse(new JsonObject())
    } catch {
      case _: DecodeException =>
        throw new Exception("The given request body could not be parsed as JSON")
    }
  }

  def addPathParam(deliveryOptions: DeliveryOptions, rc: RoutingContext, name: String): DeliveryOptions = {
    val param = Option(rc.pathParam(name)).getOrElse(throw Exception(s"The path param '$name' is not found in the path"))
    deliveryOptions.addHeader(name, param)
  }

  def addHeaderParam(deliveryOptions: DeliveryOptions, rc: RoutingContext, name: String): DeliveryOptions = {
    val header = Option(rc.request().headers().get(name)).getOrElse(throw new Exception(s"The header param '$name' is not found in the request"))
    deliveryOptions.addHeader(name, header)
  }

  def addQueryParam(deliveryOptions: DeliveryOptions, rc: RoutingContext, name: String): DeliveryOptions = {
    val param = Option(rc.request.getParam(name)).getOrElse(throw Exception(s"The query param '$name' is not found in the request'"))
    deliveryOptions.addHeader(name, param)
  }

  def addOptionalQueryParam(deliveryOptions: DeliveryOptions, rc: RoutingContext, name: String, defaultValue: Option[String] = None): DeliveryOptions = {
    (Option(rc.request.getParam(name)), defaultValue) match {
      case (Some(value), _)    => deliveryOptions.addHeader(name, value)
      case (None, Some(value)) => deliveryOptions.addHeader(name, value)
      case _                   => deliveryOptions
    }
  }
}
