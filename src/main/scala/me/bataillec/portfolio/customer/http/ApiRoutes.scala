package me.bataillec.portfolio.customer.http

import io.vertx.core.{Future, Vertx}
import io.vertx.ext.healthchecks.{HealthChecks, Status}
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.healthchecks.HealthCheckHandler
import io.vertx.ext.web.openapi.router.{RequestExtractor, RouterBuilder}
import io.vertx.ext.web.{Router, RoutingContext}
import io.vertx.openapi.contract.OpenAPIContract
import me.bataillec.portfolio.customer.eventbus.{CustomerSender, SenderUtils}

object ApiRoutes {
  private val openApiPath        = "openapi.yaml"
  private val defaultContentType = "application/json"

  private val healthcheckTimeout = 1000 // in milliseconds

  def build(vertx: Vertx, sender: CustomerSender): Future[Router] = {
    OpenAPIContract
      .builder(vertx)
      .setContractPath(openApiPath)
      .build()
      .map { contract =>
        val builder = RouterBuilder.create(vertx, contract, RequestExtractor.withBodyHandler())
        builder.rootHandler(BodyHandler.create(false).setBodyLimit(64 * 1024))

        def op(id: String)(handler: RoutingContext => Future[Unit]): Unit = {
          builder.getRoute(id).addHandler(ctx => handler(ctx).onFailure(ctx.fail(_)))
        }

        op("createCustomer") { ctx =>
          sender.createCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("listCustomers") { ctx =>
          sender.getCustomers(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("getCustomer") { ctx =>
          sender.getCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("updateCustomer") { ctx =>
          sender.updateCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("subscribe") { ctx =>
          sender.subscribe(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("listSubscriptions") { ctx =>
          sender.getSubscriptions(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("listEntitlements") { ctx =>
          sender.getEntitlements(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op("consumeEntitlement") { ctx =>
          sender.consumeEntitlement(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        // Liveness: no dependency checks, just "the eventloop answers"
        val live = HealthChecks.create(vertx)
        live.register("process", promise => promise.complete(Status.OK))

        // Readiness: dependencies,each with a timeout
        val ready = HealthChecks.create(vertx)
        ready.register(
          "customer-verticle",
          healthcheckTimeout,
          promise => SenderUtils.ping(vertx, "customer.health", promise)
        )
        ready.register(
          "audit-verticle",
          healthcheckTimeout,
          promise => SenderUtils.ping(vertx, "audit.health", promise)
        )

        builder
          .getRoute("live")
          .addHandler(HealthCheckHandler.createWithHealthChecks(live))
        builder
          .getRoute("ready")
          .addHandler(HealthCheckHandler.createWithHealthChecks(ready))

        val router = builder.createRouter()
        router.route().failureHandler(Problem.failureHandler)

        router
      }
  }

  private def makeResponse(ctx: RoutingContext, response: String): Unit = {
    val r = HttpResponse.fromJsonString(response)
    Problem.getDomainErrorFromHttpResponse(r) match {
      case Some(value) => throw value
      case None        => json(ctx, r)
    }
  }

  private def json(ctx: RoutingContext, response: HttpResponse): Unit = {
    val res = ctx
      .response()
      .setStatusCode(response.statusCode)
      .putHeader("Content-Type", response.contentType)
    response.headers.foreach { (k, v) =>
      res.putHeader(k, v)
    }
    res.end(response.body)
  }
}
