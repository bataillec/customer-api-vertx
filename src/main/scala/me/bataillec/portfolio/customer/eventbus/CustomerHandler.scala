package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.json.Json
import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.customer.CustomerOperations
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.http.HttpResponse
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class CustomerHandler(vertx: Vertx) {
  private val logger     = LoggerFactory.getLogger(getClass)
  private val operations = new CustomerOperations()

  def handle(): Future[Unit] = {
    for {
      _ <- SenderUtils.pong(
             vertx,
             CUSTOMER_HEALTH_ADDRESS,
             (msg: Message[String]) =>
               SenderUtils
                 .ping(vertx, AUDIT_HEALTH_ADDRESS)
                 .mapEmpty()
           )
      _ <- vertx
             .eventBus()
             .consumer(
               CUSTOMER_ADDRESS,
               (msg: Message[String]) =>
                 logger.debug("A customer message has been received: {}", msg)
                 consumeMessage(msg)
             )
             .completion
    } yield ()
  }

  private def consumeMessage(msg: Message[String]): Unit = {
    val maybeOperationId = Option(msg.headers().get(OPERATION_ID))
    Try {
      maybeOperationId match {
        case Some(value) if value == createCustomer_ID =>
          val newCustomer         = Json.decodeValue(msg.body(), classOf[NewCustomer])
          val maybeIdempotencyKey = Option(msg.headers().get(IDEMPOTENCY_KEY_HEADER))
          operations.createCustomer(newCustomer, maybeIdempotencyKey).map((r: Customer) => r.toHttpResponse(statusCode = 201))

        case Some(value) if value == listSubscriptions_ID => ???
        case Some(value) if value == updateCustomer_ID    => ???
        case Some(value) if value == listEntitlements_ID  => ???
        case Some(value) if value == subscribe_ID         => ???
        case Some(value) if value == listCustomers_ID     =>
          val limit       = msg.headers().get(LIMIT_QUERY_PARAM).toInt
          val maybeCursor = Option(msg.headers().get(CURSOR_QUERY_PARAM))
          val maybeStatus = Option(msg.headers().get(STATUS_QUERY_PARAM)).map(CustomerStatus.valueOf)
          operations.listCustomers(limit, maybeCursor, maybeStatus).map((r: ListCustomers) => r.toHttpResponse())

        case Some(value) if value == consumeEntitlement_ID => ???
        case Some(value) if value == getCustomer_ID        => ???
        case None                                          => ???
        case Some(_)                                       => ???
      }
    } match {
      case Failure(exception) =>
        logger.error("An unexpected error occurred...", exception)
        msg.fail(500, Json.encode(exception))
      case Success(value)     =>
        value
          .map((v: HttpResponse) => msg.reply(v.toJson.encode()))
          .onFailure { (e: Throwable) =>
            logger.error("Failed to process or reply to the customer message", e)
            msg.fail(500, e.getMessage)
          }
    }
  }
}
