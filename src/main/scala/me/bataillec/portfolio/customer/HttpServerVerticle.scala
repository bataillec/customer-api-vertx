package me.bataillec.portfolio.customer

import io.vertx.core.http.HttpServer
import io.vertx.core.{Future, VerticleBase}
import me.bataillec.portfolio.customer.config.HttpServerConfig
import me.bataillec.portfolio.customer.eventbus.CustomerSender
import me.bataillec.portfolio.customer.http.ApiRoutes
import org.slf4j.LoggerFactory

class HttpServerVerticle extends VerticleBase {
  private val logger = LoggerFactory.getLogger(getClass)

  override def start(): Future[HttpServer] = {
    val config = HttpServerConfig.fromEnv()
    val sender = CustomerSender(vertx.eventBus())

    ApiRoutes
      .build(vertx, sender)
      .compose { router =>
        vertx
          .createHttpServer()
          .requestHandler(router)
          .listen(config.httpPort)
      }
      .onSuccess { server =>
        logger.info("HTTP server listening on port {}", server.actualPort())
      }
  }
}
