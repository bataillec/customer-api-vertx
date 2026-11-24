package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.{DeliveryOptions, Message, MessageConsumer}
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Promise, Vertx}
import io.vertx.ext.healthchecks.Status
import me.bataillec.portfolio.customer.Constants.*

import scala.util.Try

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
      .onSuccess { reply =>
        // The audit verticle answers with details (JSON); the others with a plain "pong"
        val details = Try(JsonObject(reply.body())).toOption
        promise.complete(details.fold(Status.OK)(d => Status.OK(d)))
      }
      .onFailure { err =>
        promise.complete(Status.KO(new JsonObject().put("error", err.getMessage)))
      }
  }

  def pong(vertx: Vertx, address: String, handler: Message[String] => Future[Unit], failureText: String = "Postgres unreachable"): Future[Unit] = {
    vertx
      .eventBus()
      .consumer[String](
        address,
        msg =>
          handler(msg)
            .onSuccess(_ => msg.reply(PONG))
            .onFailure(_ => msg.fail(503, failureText))
      )
      .completion()
      .mapEmpty()
  }

  /** Like `pong`, but the reply is the JSON details the check returns, which the health ping reports. */
  def pongWithDetails(vertx: Vertx, address: String, check: () => Future[JsonObject], failureText: String): Future[Unit] = {
    vertx
      .eventBus()
      .consumer[String](
        address,
        msg =>
          check()
            .onSuccess(details => msg.reply(details.encode()))
            .onFailure(_ => msg.fail(503, failureText))
      )
      .completion()
      .mapEmpty()
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
