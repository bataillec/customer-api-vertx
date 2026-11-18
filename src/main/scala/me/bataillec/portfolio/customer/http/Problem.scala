package me.bataillec.portfolio.customer.http

import io.vertx.core.eventbus.{ReplyException, ReplyFailure}
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.openapi.validation.ValidatorException
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.domain.DomainError
import org.slf4j.LoggerFactory

object Problem {
  private val logger = LoggerFactory.getLogger(getClass)

  def fromDomainError(e: DomainError): HttpResponse = {
    val headers = e match {
      case DomainError.QuotaExceeded(_, Some(retryAfter)) => Map(RETRY_AFTER_HEADER -> retryAfter.toString)
      case _                                              => Map.empty[String, String]
    }
    HttpResponse(e.status, body(e.status, e.code, e.title, Some(e.getMessage)).encode(), PROBLEM_CONTENT_TYPE, headers)
  }

  def badRequest(detail: String): HttpResponse =
    HttpResponse(400, body(400, VALIDATION_FAILED, INVALID_REQUEST, Some(detail)).encode(), PROBLEM_CONTENT_TYPE)

  def withTraceId(ctx: RoutingContext, response: HttpResponse): HttpResponse = {
    if (response.contentType == PROBLEM_CONTENT_TYPE) {
      traceId(ctx).fold(response)(id => response.copy(body = JsonObject(response.body).put(TRACE_ID_FIELD, id).encode()))
    } else response
  }

  def failureHandler(ctx: RoutingContext): Unit = {
    if (!ctx.response().ended()) {
      Option(ctx.failure()) match {
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.TIMEOUT           =>
          write(ctx, 504, TIMEOUT_COMMUNICATION, TIMEOUT_COMMUNICATION_BETWEEN_VERTICLES, Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.NO_HANDLERS       =>
          write(ctx, 503, NO_HANDLERS, COMMUNICATION_PROBLEM_BETWEEN_VERTICLES, Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.RECIPIENT_FAILURE =>
          write(ctx, e.failureCode(), RECIPIENT_FAILURE, MESSAGES_REJECTED_BY_VERTICLES, Some("Check the application status"))
        case Some(e: HttpException) if e.getCause.isInstanceOf[ValidatorException]        =>
          write(ctx, e.getStatusCode, VALIDATION_FAILED, INVALID_REQUEST, Some(e.getPayload))
        case Some(e: HttpException)                                                       =>
          write(ctx, e.getStatusCode, HTTP_ERROR, REQUEST_FAILED, Option(e.getPayload).orElse(Option(e.getMessage)))
        case Some(e)                                                                      =>
          logger.error("Unhandled failure on {} {}", ctx.request().method(), ctx.request().path(), e)
          write(ctx, 500, INTERNAL_ERROR, INTERNAL_SERVER_ERROR, None)
        case None                                                                         =>
          val status = if ctx.statusCode() > 0 then ctx.statusCode() else 500
          write(ctx, status, if status == 404 then NOT_FOUND else HTTP_ERROR, REQUEST_FAILED, None)
      }
    } else ()
  }

  private def body(status: Int, code: String, title: String, detail: Option[String]): JsonObject = {
    val json = JsonObject()
      .put(TITLE_FIELD, title)
      .put(STATUS_FIELD, Integer.valueOf(status))
      .put(CODE_FIELD, code)
    detail.foreach(json.put(DETAIL_FIELD, _))
    json
  }

  private def traceId(ctx: RoutingContext): Option[String] =
    Option(ctx.request().getHeader(TRACEPARENT_FIELD)).flatMap(_.split('-').lift(1))

  private def write(ctx: RoutingContext, status: Int, code: String, title: String, detail: Option[String]): Unit = {
    val json = body(status, code, title, detail)
    traceId(ctx).foreach(json.put(TRACE_ID_FIELD, _))
    ctx.response().setStatusCode(status).putHeader(CONTENT_TYPE, PROBLEM_CONTENT_TYPE).end(json.encode())
  }
}
