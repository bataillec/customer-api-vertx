package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.{DeliveryOptions, Message, MessageConsumer}
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Promise, Vertx}
import io.vertx.ext.healthchecks.Status
import me.bataillec.portfolio.customer.Constants.*

object SenderUtils {
  private val PING = "ping"
  private val PONG = "pong"

  def ping(vertx: Vertx, address: String, promise: Promise[Status]): Unit = {
    vertx
      .eventBus()
      .request[String](
        address,
        PING,
        new DeliveryOptions().setSendTimeout(HEALTH_PING_TIMEOUT_MS)
      )
      .onSuccess { _ =>
        promise.complete(Status.OK)
      }
      .onFailure { err =>
        promise.complete(Status.KO(new JsonObject().put("error", err.getMessage)))
      }
  }

  def ping(vertx: Vertx, address: String): Future[Message[String]] = {
    vertx
      .eventBus()
      .request[String](
        address,
        PING,
        new DeliveryOptions().setSendTimeout(HEALTH_AUDIT_TIMEOUT_MS)
      )
  }

  def pong(vertx: Vertx, address: String, handler: Message[String] => Future[Unit]): Future[_] = {
    vertx
      .eventBus()
      .consumer[String](
        address,
        msg =>
          handler(msg)
            .onSuccess(_ => msg.reply(PONG))
            .onFailure(err => msg.fail(503, s"Audit verticle unreachable: ${err.getMessage}"))
      )
      .completion()
  }

  def pong(vertx: Vertx, address: String): MessageConsumer[String] = {
    vertx
      .eventBus()
      .consumer[String](
        address,
        msg => msg.reply(PONG)
      )
  }
}
