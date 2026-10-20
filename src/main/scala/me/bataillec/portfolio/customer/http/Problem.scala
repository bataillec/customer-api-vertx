package me.bataillec.portfolio.customer.http

import io.vertx.core.eventbus.{ReplyException, ReplyFailure}
import io.vertx.core.json.{Json, JsonObject}
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import me.bataillec.portfolio.customer.domain.DomainError
import me.bataillec.portfolio.customer.domain.DomainError.{CustomerNotFound, EmailAlreadyUsed, EntitlementNotFound, QuotaExceeded, UnknownError}
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.{Failure, Success, Try}

object Problem {
  private val logger = LoggerFactory.getLogger(getClass)

  def getDomainErrorFromHttpResponse(response: HttpResponse): Option[DomainError] = {
    if (response.statusCode > 299) { // Error codes 3**, 4** or 5** are errors
      val body = Json.decodeValue(response.body).asInstanceOf[JsonObject]
      if (body.containsKey("code")) {
        val code = body.getString("code")

        code match {
          case "CUSTOMER_NOT_FOUND"    =>
            if (!body.containsKey("id")) {
              Some(UnknownError("UUID not found"))
            } else {
              Try(UUID.fromString(body.getString("id"))) match {
                case Failure(exception) => Some(UnknownError("Malformed UUID"))
                case Success(value)     => Some(CustomerNotFound(value))
              }
            }
          case "EMAIL_ALREADY_USED"    =>
            if (!body.containsKey("email")) {
              Some(UnknownError("Email not found"))
            } else {
              Some(EmailAlreadyUsed(body.getString("email")))
            }
          case "QUOTA_EXCEEDED"        =>
            (body.containsKey("featureKey"), body.containsKey("retryAfterSeconds")) match {
              case (true, true)   => Some(QuotaExceeded(body.getString("featureKey"), body.getLong("retryAfterSeconds")))
              case (false, true)  => Some(UnknownError("FeatureKey not found"))
              case (true, false)  => Some(UnknownError("RetryAfterSeconds not found"))
              case (false, false) => Some(UnknownError("FeatureKey and RetryAfterSeconds not found"))
            }
          case "ENTITLEMENT_NOT_FOUND" =>
            (body.containsKey("id"), body.containsKey("featurekey")) match {
              case (true, true)   =>
                Try(UUID.fromString(body.getString("id"))) match {
                  case Failure(exception) => Some(UnknownError("Malformed UUID"))
                  case Success(value)     => Some(EntitlementNotFound(value, body.getString("featureKey")))
                }
              case (false, true)  => Some(UnknownError("UUID not found"))
              case (true, false)  => Some(UnknownError("FeatureKey not found"))
              case (false, false) => Some(UnknownError("Id and FeatureKey not found"))
            }
          case "UNKOWN_ERROR"          => Some(UnknownError("Unknown error"))
          case _                       => Some(UnknownError("Unknown error"))
        }
      } else {
        Some(UnknownError("Unknown error"))
      }
    } else None
  }

  def failureHandler(ctx: RoutingContext): Unit = {
    if (!ctx.response().ended()) {
      Option(ctx.failure()) match {
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.TIMEOUT                                           =>
          write(ctx, 504, "TIMEOUT_COMMUNICATION", "Timeout communication between verticles", Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.NO_HANDLERS                                       =>
          write(ctx, 503, "NO_HANDLERS", "Communication problem between verticles", Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.RECIPIENT_FAILURE                                 =>
          write(ctx, e.failureCode(), "RECIPIENT_FAILURE", "Messages rejected by verticles", Some("Check the application status"))
        case Some(e: DomainError.CustomerNotFound)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EmailAlreadyUsed)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EntitlementNotFound)                                                                     =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.QuotaExceeded)                                                                           =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage), "Retry-After" -> e.retryAfterSeconds.toString)
        case Some(e) if e.getClass.getSimpleName.contains("Validator") || e.getClass.getSimpleName.contains("BadRequest") =>
          write(ctx, 400, "VALIDATION_FAILED", "Invalid request", Some(e.getMessage))
        case Some(e: HttpException)                                                                                       =>
          write(ctx, e.getStatusCode, "HTTP_ERROR", "Request failed", Option(e.getPayload).orElse(Option(e.getMessage)))
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
