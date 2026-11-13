package me.bataillec.portfolio.customer.unit.http

import io.vertx.core.eventbus.{ReplyException, ReplyFailure}
import io.vertx.core.http.{HttpServerRequest, HttpServerResponse}
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.openapi.validation.ValidatorException
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.domain.CustomerStatus.SUSPENDED
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.domain.PlanType.ENTERPRISE
import me.bataillec.portfolio.customer.http.{HttpResponse, Problem}
import me.bataillec.portfolio.customer.unit.http.ProblemSpec.*
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{anyInt, anyString}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.prop.Tables.Table
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class ProblemSpec extends AnyWordSpec with Matchers {
  "Problem" should {
    val traceId     = "4bf92f3577b34da6a3ce929d0e0e4736"
    val traceparent = s"00-$traceId-00f067aa0ba902b7-01"

    "build a HttpResponse fromDomainError" when {
      val fixedUUIDString = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
      val fixedUUID       = UUID.fromString(fixedUUIDString)

      val table = Table(
        ("DomainError", "Error", "HttpResponse"),
        (
          "InvalidCursor",
          InvalidCursor("badCursor"),
          HttpResponse(
            statusCode = 400,
            body = """{"title":"Invalid cursor","status":400,"code":"INVALID_CURSOR","detail":"Cursor 'badCursor' is invalid"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "CustomerNotFound",
          CustomerNotFound(fixedUUID),
          HttpResponse(
            statusCode = 404,
            body =
              """{"title":"Customer not found","status":404,"code":"CUSTOMER_NOT_FOUND","detail":"Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' does not exist"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "EmailAlreadyUsed",
          EmailAlreadyUsed("email@domain.com"),
          HttpResponse(
            statusCode = 409,
            body =
              """{"title":"Email already used","status":409,"code":"EMAIL_ALREADY_USED","detail":"A customer with email 'email@domain.com' already exists"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "QuotaExceeded w/ retry",
          QuotaExceeded("feature1", Some(42L)),
          HttpResponse(
            statusCode = 429,
            body = """{"title":"Quota exceeded","status":429,"code":"QUOTA_EXCEEDED","detail":"Quota for 'feature1' is exhausted. Retry in 42 seconds"}""",
            contentType = "application/problem+json",
            headers = Map("Retry-After" -> "42")
          )
        ),
        (
          "QuotaExceeded w/o retry",
          QuotaExceeded("feature1", None),
          HttpResponse(
            statusCode = 429,
            body = """{"title":"Quota exceeded","status":429,"code":"QUOTA_EXCEEDED","detail":"Quota for 'feature1' is exhausted"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "EntitlementNotFound",
          EntitlementNotFound(fixedUUID, "feature1"),
          HttpResponse(
            statusCode = 404,
            body =
              """{"title":"Entitlement not found","status":404,"code":"ENTITLEMENT_NOT_FOUND","detail":"Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' has no entitlement 'feature1'"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "PlanNotConfigured",
          PlanNotConfigured(ENTERPRISE),
          HttpResponse(
            statusCode = 500,
            body = """{"title":"Plan not configured","status":500,"code":"PLAN_NOT_CONFIGURED","detail":"No quotas are configured for plan 'ENTERPRISE'"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "CustomerNotActive",
          CustomerNotActive(fixedUUID, SUSPENDED),
          HttpResponse(
            statusCode = 409,
            body =
              """{"title":"Customer not active","status":409,"code":"CUSTOMER_NOT_ACTIVE","detail":"Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' is SUSPENDED"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "PlanAlreadyActive",
          PlanAlreadyActive(fixedUUID, ENTERPRISE),
          HttpResponse(
            statusCode = 409,
            body =
              """{"title":"Plan already active","status":409,"code":"PLAN_ALREADY_ACTIVE","detail":"Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' is already subscribed to plan 'ENTERPRISE'"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        ),
        (
          "NoActiveSubscription",
          NoActiveSubscription(fixedUUID),
          HttpResponse(
            statusCode = 409,
            body =
              """{"title":"No active subscription","status":409,"code":"NO_ACTIVE_SUBSCRIPTION","detail":"Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' has no active subscription"}""",
            contentType = "application/problem+json",
            headers = Map.empty
          )
        )
      )

      forAll(table) { case (str, err, response) =>
        s"$str" in {
          val result = Problem.fromDomainError(err)
          val json   = JsonObject(result.body)

          result shouldBe response
          result.statusCode shouldBe json.getInteger("status")
          err.getMessage shouldBe json.getString("detail")
          json.containsKey("trace_id") shouldBe false
        }
      }
    }

    "build a HttpResponse withTraceId" when {
      val problem = HttpResponse(
        statusCode = 404,
        body = JsonObject()
          .put("title", "Customer not found")
          .put("status", 404)
          .put("code", "CUSTOMER_NOT_FOUND")
          .encode(),
        contentType = "application/problem+json",
        headers = Map("X-Custom" -> "value")
      )

      "add the trace_id from the traceparent header to a problem response" in {
        val result = Problem.withTraceId(ctxWith(Some(traceparent)), problem)

        val json = JsonObject(result.body)
        json.getString("trace_id") shouldBe traceId
      }

      "preserve the original body fields, status, content type and headers" in {
        val result = Problem.withTraceId(ctxWith(Some(traceparent)), problem)

        val json = JsonObject(result.body)
        json.getString("title") shouldBe "Customer not found"
        json.getInteger("status") shouldBe 404
        json.getString("code") shouldBe "CUSTOMER_NOT_FOUND"
        result.statusCode shouldBe 404
        result.contentType shouldBe "application/problem+json"
        result.headers shouldBe Map("X-Custom" -> "value")
      }

      "return the response unchanged when there is no traceparent header" in {
        Problem.withTraceId(ctxWith(None), problem) shouldBe problem
      }

      "return a non-problem response unchanged even with a valid traceparent header" in {
        val ok = HttpResponse(200, """{"id":"1"}""")
        Problem.withTraceId(ctxWith(Some(traceparent)), ok) shouldBe ok
      }

      "overwrite a trace_id already present in the body" in {
        val withOld = problem.copy(body =
          JsonObject()
            .put("title", "Customer not found")
            .put("status", 404)
            .put("code", "CUSTOMER_NOT_FOUND")
            .put("trace_id", "old")
            .encode()
        )
        val result  = Problem.withTraceId(ctxWith(Some(traceparent)), withOld)
        JsonObject(result.body).getString("trace_id") shouldBe traceId
      }
    }

    "handle failure using failureHandler" when {

      "do nothing when the response has already ended" in {
        val env = Env(failure = RuntimeException("boom"), ended = true).run()

        verify(env.response, never()).setStatusCode(anyInt())
        verify(env.response, never()).end(anyString())
      }

      // region ReplyException

      "map a ReplyException TIMEOUT to 504" in {
        val env = Env(failure = ReplyException(ReplyFailure.TIMEOUT, "timed out")).run()

        assertProblem(env, 504, TIMEOUT_COMMUNICATION, TIMEOUT_COMMUNICATION_BETWEEN_VERTICLES, Some("Check the application status"))
      }

      "map a ReplyException NO_HANDLERS to 503" in {
        val env = Env(failure = ReplyException(ReplyFailure.NO_HANDLERS, "no handlers")).run()

        assertProblem(env, 503, NO_HANDLERS, COMMUNICATION_PROBLEM_BETWEEN_VERTICLES, Some("Check the application status"))
      }

      "map a ReplyException RECIPIENT_FAILURE using its failureCode as status" in {
        Seq(409, 500).foreach { code =>
          val env = Env(failure = ReplyException(ReplyFailure.RECIPIENT_FAILURE, code, "rejected")).run()

          assertProblem(env, code, RECIPIENT_FAILURE, MESSAGES_REJECTED_BY_VERTICLES, Some("Check the application status"))
        }
      }

      // endregion

      // region HttpException

      "map an HttpException caused by a ValidatorException to VALIDATION_FAILED with the payload as detail" in {
        val cause = mock(classOf[ValidatorException])
        val env   = Env(failure = HttpException(400, "invalid field 'email'", cause)).run()

        assertProblem(env, 400, VALIDATION_FAILED, INVALID_REQUEST, Some("invalid field 'email'"))
      }

      "map an HttpException with a payload to HTTP_ERROR using the payload as detail" in {
        val env = Env(failure = HttpException(401, "Unauthorized")).run()

        assertProblem(env, 401, HTTP_ERROR, REQUEST_FAILED, Some("Unauthorized"))
      }

      "fall back to the message as detail when an HttpException has no payload" in {
        val cause = RuntimeException("db down")
        val ex    = HttpException(502, cause) // payload == null, message == cause.toString
        val env   = Env(failure = ex).run()

        assertProblem(env, 502, HTTP_ERROR, REQUEST_FAILED, Some(ex.getMessage))
      }

      "take the generic HttpException branch when its cause is not a ValidatorException" in {
        val env = Env(failure = HttpException(400, "bad", RuntimeException("other"))).run()

        assertProblem(env, 400, HTTP_ERROR, REQUEST_FAILED, Some("bad")) // not VALIDATION_FAILED
      }

      // endregion

      // region Unhandled failure

      "map an unknown exception to 500 without leaking its message" in {
        val env = Env(failure = RuntimeException("secret internal detail")).run()

        assertProblem(env, 500, INTERNAL_ERROR, INTERNAL_SERVER_ERROR, None)
        env.body.encode() should not include "secret internal detail"
      }

      // endregion

      // region No failure (e.g. router 404)

      "map a missing failure with status 404 to NOT_FOUND" in {
        val env = Env(statusCode = 404).run()

        assertProblem(env, 404, NOT_FOUND, REQUEST_FAILED, None)
      }

      "map a missing failure with another status to HTTP_ERROR with that status" in {
        val env = Env(statusCode = 405).run()

        assertProblem(env, 405, HTTP_ERROR, REQUEST_FAILED, None)
      }

      "default to 500 when there is no failure and no status code" in {
        Seq(-1, 0).foreach { sc =>
          val env = Env(statusCode = sc).run()

          assertProblem(env, 500, HTTP_ERROR, REQUEST_FAILED, None)
        }
      }

      // endregion

      // region trace_id (shared `write` behavior, one representative branch is enough)

      "include trace_id in the body when a traceparent header is present" in {
        val env = Env(failure = RuntimeException("boom"), header = Some(traceparent)).run()

        env.body.getString(TRACE_ID_FIELD) shouldBe traceId
      }

      "omit trace_id when there is no traceparent header" in {

        val env = Env(failure = RuntimeException("boom")).run()

        env.body.containsKey(TRACE_ID_FIELD) shouldBe false
      }

      // endregion
    }
  }
}

object ProblemSpec {

  /** Builds a mocked RoutingContext whose request returns the given traceparent header (null = header absent). */
  def ctxWith(header: Option[String]): RoutingContext = {
    val request = mock(classOf[HttpServerRequest])
    when(request.getHeader("traceparent")).thenReturn(header.orNull)

    val ctx = mock(classOf[RoutingContext])
    when(ctx.request()).thenReturn(request)
    ctx
  }

  /** A mocked RoutingContext + response, with helpers to inspect what was written. */
  private final class Env(
      failure: Throwable = null,
      statusCode: Int = -1,
      ended: Boolean = false,
      header: Option[String] = None
  ) {
    private val request: HttpServerRequest = mock(classOf[HttpServerRequest])
    val response: HttpServerResponse       = mock(classOf[HttpServerResponse])
    private val ctx: RoutingContext        = mock(classOf[RoutingContext])

    when(request.getHeader("traceparent")).thenReturn(header.orNull)
    when(ctx.request()).thenReturn(request)
    when(ctx.response()).thenReturn(response)
    when(ctx.failure()).thenReturn(failure)
    when(ctx.statusCode()).thenReturn(statusCode)
    when(response.ended()).thenReturn(ended)
    // fluent API: must return the mock itself or the chain NPEs
    when(response.setStatusCode(anyInt())).thenReturn(response)
    when(response.putHeader(anyString(), anyString())).thenReturn(response)

    def run(): Env = {
      Problem.failureHandler(ctx)
      this
    }

    /** The JSON passed to response.end(...). Fails if end() was not called exactly once. */
    def body: JsonObject = {
      val captor = ArgumentCaptor.forClass(classOf[String])
      verify(response, times(1)).end(captor.capture())
      JsonObject(captor.getValue)
    }
  }

  /** Asserts the whole problem+json contract for one branch. detail = None means the field must be absent. */
  private def assertProblem(env: Env, status: Int, code: String, title: String, detail: Option[String]): Unit = {
    verify(env.response).setStatusCode(status)
    verify(env.response).putHeader("Content-Type", "application/problem+json")

    val json = env.body
    json.getString("title") shouldBe title
    json.getString("code") shouldBe code
    json.getInteger("status") shouldBe status
    detail match {
      case Some(d) => json.getString("detail") shouldBe d
      case None    => json.containsKey("detail") shouldBe false
    }
  }
}
