package me.bataillec.portfolio.customer.http

import io.vertx.core.eventbus.{ReplyException, ReplyFailure}
import io.vertx.core.json.{Json, JsonObject}
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.domain.DomainError
import me.bataillec.portfolio.customer.domain.DomainError.*
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.{Failure, Success, Try}

object Problem {
  private val logger = LoggerFactory.getLogger(getClass)

  def getDomainErrorFromHttpResponse(response: HttpResponse): Option[DomainError] = {
    if (response.statusCode > 299) { // Error codes 3**, 4** or 5** are errors
      val body = Json.decodeValue(response.body).asInstanceOf[JsonObject]
      if (body.containsKey(CODE_FIELD)) {
        val code = body.getString(CODE_FIELD)

        code match {
          case CUSTOMER_NOT_FOUND    =>
            if (!body.containsKey(ID_FIELD)) {
              Some(UnknownError(UUID_NOT_FOUND))
            } else {
              Try(UUID.fromString(body.getString(ID_FIELD))) match {
                case Failure(exception) => Some(UnknownError(MALFORMED_UUID))
                case Success(value)     => Some(CustomerNotFound(value))
              }
            }
          case EMAIL_ALREADY_USED    =>
            if (!body.containsKey(EMAIL_FIELD)) {
              Some(UnknownError(EMAIL_NOT_FOUND))
            } else {
              Some(EmailAlreadyUsed(body.getString(EMAIL_FIELD)))
            }
          case QUOTA_EXCEEDED        =>
            (body.containsKey(FEATURE_KEY_FIELD), body.containsKey(RETRY_AFTER_SECONDS_FIELD)) match {
              case (true, true)   => Some(QuotaExceeded(body.getString(FEATURE_KEY_FIELD), body.getLong(RETRY_AFTER_SECONDS_FIELD)))
              case (false, true)  => Some(UnknownError(FEATURE_KEY_NOT_FOUND))
              case (true, false)  => Some(UnknownError(RETRY_AFTER_SECONDS_NOT_FOUND))
              case (false, false) => Some(UnknownError(FEATURE_KEY_AND_RETRY_AFTER_SECONDS_NOT_FOUND))
            }
          case ENTITLEMENT_NOT_FOUND =>
            (body.containsKey(ID_FIELD), body.containsKey(FEATURE_KEY_FIELD)) match {
              case (true, true)   =>
                Try(UUID.fromString(body.getString(ID_FIELD))) match {
                  case Failure(exception) => Some(UnknownError(MALFORMED_UUID))
                  case Success(value)     => Some(EntitlementNotFound(value, body.getString(FEATURE_KEY_FIELD)))
                }
              case (false, true)  => Some(UnknownError(UUID_NOT_FOUND))
              case (true, false)  => Some(UnknownError(FEATURE_KEY_NOT_FOUND))
              case (false, false) => Some(UnknownError(ID_AND_FEATURE_KEY_NOT_FOUND))
            }
          case UNKOWN_ERROR          => Some(UnknownError(UNKNOWN_ERROR))
          case _                     => Some(UnknownError(UNKNOWN_ERROR))
        }
      } else {
        Some(UnknownError(UNKNOWN_ERROR))
      }
    } else None
  }

  def failureHandler(ctx: RoutingContext): Unit = {
    if (!ctx.response().ended()) {
      Option(ctx.failure()) match {
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.TIMEOUT                                           =>
          write(ctx, 504, TIMEOUT_COMMUNICATION, TIMEOUT_COMMUNICATION_BETWEEN_VERTICLES, Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.NO_HANDLERS                                       =>
          write(ctx, 503, NO_HANDLERS, COMMUNICATION_PROBLEM_BETWEEN_VERTICLES, Some("Check the application status"))
        case Some(e: ReplyException) if e.failureType() == ReplyFailure.RECIPIENT_FAILURE                                 =>
          write(ctx, e.failureCode(), RECIPIENT_FAILURE, MESSAGES_REJECTED_BY_VERTICLES, Some("Check the application status"))
        case Some(e: DomainError.CustomerNotFound)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EmailAlreadyUsed)                                                                        =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.EntitlementNotFound)                                                                     =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage))
        case Some(e: DomainError.QuotaExceeded)                                                                           =>
          write(ctx, e.status, e.code, e.title, Some(e.getMessage), RETRY_AFTER_HEADER -> e.retryAfterSeconds.toString)
        case Some(e) if e.getClass.getSimpleName.contains("Validator") || e.getClass.getSimpleName.contains("BadRequest") =>
          write(ctx, 400, VALIDATION_FAILED, INVALID_REQUEST, Some(e.getMessage))
        case Some(e: HttpException)                                                                                       =>
          write(ctx, e.getStatusCode, HTTP_ERROR, REQUEST_FAILED, Option(e.getPayload).orElse(Option(e.getMessage)))
        case Some(e)                                                                                                      =>
          logger.error("Unhandled failure on {} {}", ctx.request().method(), ctx.request().path(), e)
          write(ctx, 500, INTERNAL_ERROR, INTERNAL_SERVER_ERROR, None)
        case None                                                                                                         =>
          val status = if ctx.statusCode() > 0 then ctx.statusCode() else 500
          write(ctx, status, if status == 404 then NOT_FOUND else HTTP_ERROR, REQUEST_FAILED, None)
      }
    } else ()
  }

  private def write(ctx: RoutingContext, status: Int, code: String, title: String, detail: Option[String], headers: (String, String)*): Unit = {
    val body     = JsonObject()
      .put(TITLE_FIELD, title)
      .put(STATUS_CODE_FIELD, Integer.valueOf(status))
      .put(CODE_FIELD, code)
    detail.foreach(body.put(DETAIL_FIELD, _))
    Option(ctx.request().getHeader(TRACEPARENT_FIELD)).flatMap(_.split('-').lift(1)).foreach(body.put(TRACE_ID_FIELD, _))
    val response = ctx.response().setStatusCode(status).putHeader(CONTENT_TYPE, DEFAULT_CONTENT_TYPE)
    headers.foreach((k, v) => response.putHeader(k, v))
    response.end(body.encode())
  }
}
