package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.{DeliveryOptions, MessageConsumer}
import io.vertx.core.json.JsonObject
import io.vertx.core.{Promise, Vertx}
import io.vertx.ext.healthchecks.Status

object SenderUtils {
  def ping(vertx: Vertx, address: String, promise: Promise[Status]): Unit = {
    vertx
      .eventBus()
      .request[String](
        address,
        "ping",
        new DeliveryOptions().setSendTimeout(800)
      )
      .onSuccess { _ =>
        promise.complete(Status.OK)
      }
      .onFailure { err =>
        promise.complete(Status.KO(new JsonObject().put("error", err.getMessage)))
      }
  }

  def pong(vertx: Vertx, address: String): MessageConsumer[String] = {
    vertx
      .eventBus()
      .consumer[String](
        address,
        msg => msg.reply("pong")
      )
  }
}
