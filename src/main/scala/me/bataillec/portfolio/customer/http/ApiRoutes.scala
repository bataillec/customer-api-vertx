package me.bataillec.portfolio.customer.http

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{Future, Vertx}
import io.vertx.ext.healthchecks.{HealthChecks, Status}
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.healthchecks.HealthCheckHandler
import io.vertx.ext.web.openapi.router.{RequestExtractor, RouterBuilder}
import io.vertx.ext.web.{Router, RoutingContext}
import io.vertx.openapi.contract.OpenAPIContract
import io.vertx.openapi.mediatype.{ContentAnalyser, MediaTypeRegistration, MediaTypeRegistry}
import io.vertx.openapi.validation.ValidationContext
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.{CustomerSender, SenderUtils}

import java.util.List as JList
import scala.util.control.NonFatal

object ApiRoutes {
  private[customer] val problemMediaType = new MediaTypeRegistration {
    override def createContentAnalyser(contentType: String, content: Buffer, context: ValidationContext): ContentAnalyser =
      MediaTypeRegistration.APPLICATION_JSON.createContentAnalyser(contentType, content, context)

    override def canHandle(mediaType: String): Boolean = mediaType == PROBLEM_CONTENT_TYPE

    override def supportedTypes(): JList[String] = JList.of(PROBLEM_CONTENT_TYPE)
  }

  def build(vertx: Vertx, sender: CustomerSender): Future[Router] = {
    OpenAPIContract
      .builder(vertx)
      .setContractPath(OPENAPI_PATH)
      .mediaTypeRegistry(MediaTypeRegistry.createDefault().register(problemMediaType))
      .build()
      .map { contract =>
        val builder = RouterBuilder.create(vertx, contract, RequestExtractor.withBodyHandler())
        builder.rootHandler(BodyHandler.create(false).setBodyLimit(BODY_LIMIT))

        def op(id: String)(handler: RoutingContext => Future[Unit]): Unit = {
          builder
            .getRoute(id)
            .addHandler { ctx =>
              // A handler that throws synchronously is turned into a failed Future
              val result =
                try handler(ctx)
                catch { case NonFatal(e) => Future.failedFuture[Unit](e) }
              // Problem.failureHandler logs the unexpected failures
              result.onFailure(ctx.fail(_))
            }
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
          sender.getPlans(ctx).map { r =>
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

        // Readiness: dependencies, each with a timeout
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
        // Requests that match no route never reach the failure handler
        ROUTING_ERROR_STATUSES.foreach(router.errorHandler(_, Problem.failureHandler))

        // expose the spec as JSON
        router
          .get("/openapi.json")
          .handler(ctx =>
            ctx
              .response()
              .putHeader(CONTENT_TYPE, DEFAULT_CONTENT_TYPE)
              .end(publicSpec(contract.getRawContract()).encode())
          )

        router
      }
  }

  // The contract keeps its internal "app:///#/..." refs: make them plain document refs again
  private[http] def publicSpec(raw: JsonObject): JsonObject =
    JsonObject(raw.encode().replace("\"app:///#", "\"#")).put("servers", JsonArray().add(JsonObject().put("url", "/")))

  private def makeResponse(ctx: RoutingContext, response: String): Unit = {
    json(ctx, Problem.withTraceId(ctx, HttpResponse.fromJsonString(response)))
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
