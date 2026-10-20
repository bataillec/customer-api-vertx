package me.bataillec.portfolio.customer.http

import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import me.bataillec.portfolio.customer.domain.DomainError
import org.slf4j.LoggerFactory

object Problem {
  private val logger = LoggerFactory.getLogger(getClass)

  def failureHandler(ctx: RoutingContext): Unit = {
    if (!ctx.response().ended()) {
      Option(ctx.failure()) match {
        case Some(e: DomainError.CustomerNotFound)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EmailAlreadyUsed)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EntitlementNotFound)                                                                     =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.QuotaExceeded)                                                                           =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage), "Retry-After" -> e.retryAfterSeconds.toString)
        case Some(e: HttpException)                                                                                       =>
          write(ctx, e.getStatusCode, "HTTP_ERROR", "Request failed", Option(e.getPayload).orElse(Option(e.getMessage)))
        case Some(e) if e.getClass.getSimpleName.contains("Validator") || e.getClass.getSimpleName.contains("BadRequest") =>
          write(ctx, 400, "VALIDATION_FAILED", "Invalid request", Some(e.getMessage))
        case Some(e)                                                                                                      =>
          logger.error("Unhandled failure on {} {}", ctx.request().method(), ctx.request().path(), e)
          write(ctx, 500, "INTERNAL_ERROR", "Internal server error", None)
        case None                                                                                                         =>
          val status = if ctx.statusCode() > 0 then ctx.statusCode() else 500
          write(ctx, status, if status == 404 then "NOT_FOUND" else "HTTP_ERROR", "Request failed", None)
      }
    } else ()
  }

  private def write(ctx: RoutingContext, status: Int, code: String, title: String, detail: Option[String], headers: (String, String)*): Unit = {
    val body     = JsonObject()
      .put("title", title)
      .put("status", Integer.valueOf(status))
      .put("code", code)
    detail.foreach(body.put("detail", _))
    Option(ctx.request().getHeader("traceparent")).flatMap(_.split('-').lift(1)).foreach(body.put("trace_id", _))
    val response = ctx.response().setStatusCode(status).putHeader("Content-Type", "application/json")
    headers.foreach((k, v) => response.putHeader(k, v))
    response.end(body.encode())
  }
}
