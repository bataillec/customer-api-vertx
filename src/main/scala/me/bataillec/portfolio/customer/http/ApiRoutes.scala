package me.bataillec.portfolio.customer.http

import io.vertx.core.{Future, Vertx}
import io.vertx.ext.healthchecks.{HealthChecks, Status}
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.healthchecks.HealthCheckHandler
import io.vertx.ext.web.openapi.router.{RequestExtractor, RouterBuilder}
import io.vertx.ext.web.{Router, RoutingContext}
import io.vertx.openapi.contract.OpenAPIContract
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.{CustomerSender, SenderUtils}

object ApiRoutes {

  def build(vertx: Vertx, sender: CustomerSender): Future[Router] = {
    OpenAPIContract
      .builder(vertx)
      .setContractPath(OPENAPI_PATH)
      .build()
      .map { contract =>
        val builder = RouterBuilder.create(vertx, contract, RequestExtractor.withBodyHandler())
        builder.rootHandler(BodyHandler.create(false).setBodyLimit(BODY_LIMIT))

        def op(id: String)(handler: RoutingContext => Future[Unit]): Unit = {
          builder.getRoute(id).addHandler(ctx => handler(ctx).onFailure(ctx.fail(_)))
        }

        op(createCustomer_ID) { ctx =>
          sender.createCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(listCustomers_ID) { ctx =>
          sender.getCustomers(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(getCustomer_ID) { ctx =>
          sender.getCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(updateCustomer_ID) { ctx =>
          sender.updateCustomer(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(subscribe_ID) { ctx =>
          sender.subscribe(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(listSubscriptions_ID) { ctx =>
          sender.getSubscriptions(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(listEntitlements_ID) { ctx =>
          sender.getEntitlements(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        op(listPlans_ID) { ctx =>
          sender.getPlans().map { r =>
            makeResponse(ctx, r)
          }
        }

        op(consumeEntitlement_ID) { ctx =>
          sender.consumeEntitlement(ctx).map { r =>
            makeResponse(ctx, r)
          }
        }

        // Liveness: no dependency checks, just "the eventloop answers"
        val live = HealthChecks.create(vertx)
        live.register(HTTPSERVER_VERTICLE, promise => promise.complete(Status.OK))

        // Readiness: dependencies,each with a timeout
        val ready = HealthChecks.create(vertx)
        ready.register(
          CUSTOMER_VERTICLE,
          HEALTH_CHECK_TIMEOUT,
          promise => {
            SenderUtils.ping(vertx, CUSTOMER_HEALTH_ADDRESS, promise)
          }
        )
        ready.register(
          AUDIT_VERTICLE,
          HEALTH_CHECK_TIMEOUT,
          promise => {
            SenderUtils.ping(vertx, AUDIT_HEALTH_ADDRESS, promise)
          }
        )

        builder
          .getRoute(live_ID)
          .addHandler(HealthCheckHandler.createWithHealthChecks(live))
        builder
          .getRoute(ready_ID)
          .addHandler(HealthCheckHandler.createWithHealthChecks(ready))

        val router = builder.createRouter()
        router.route().failureHandler(Problem.failureHandler)

        // expose the spec as JSON
        router
          .get("/openapi.json")
          .handler(ctx =>
            ctx
              .response()
              .putHeader(CONTENT_TYPE_FIELD, DEFAULT_CONTENT_TYPE)
              .end(contract.getRawContract().encode())
          );

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
      .putHeader(CONTENT_TYPE, response.contentType)
    response.headers.foreach { (k, v) =>
      res.putHeader(k, v)
    }
    res.end(response.body)
  }
}
