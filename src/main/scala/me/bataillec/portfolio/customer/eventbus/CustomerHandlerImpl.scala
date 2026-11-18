package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.customer.CustomerOps
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.http.{HttpResponse, Problem}
import me.bataillec.portfolio.customer.json.JsonSupport
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.{Failure, Success, Try}

class CustomerHandlerImpl(vertx: Vertx, operations: CustomerOps) extends CustomerHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(): Future[Unit] = {
    for {
      _ <- SenderUtils.pong(
             vertx,
             CUSTOMER_HEALTH_ADDRESS,
             (_: Message[String]) => operations.ping().onFailure(err => logger.warn("Postgres is unreachable", err))
           )
      _ <- vertx
             .eventBus()
             .consumer(
               CUSTOMER_ADDRESS,
               (msg: Message[String]) =>
                 logger.debug("A customer message has been received")
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
          val newCustomer = decodeBody(msg, classOf[CreateCustomer])
          operations
            .createCustomer(newCustomer)
            .map((r: Customer) => r.toHttpResponse(statusCode = 201, headers = Map("Location" -> s"/customers/${r.id}")))

        case Some(value) if value == listCustomers_ID =>
          val limit       = parse(msg.headers().get(LIMIT_QUERY_PARAM).toInt)
          val maybeCursor = Option(msg.headers().get(CURSOR_QUERY_PARAM))
          val maybeStatus = Option(msg.headers().get(STATUS_QUERY_PARAM)).map(v => parse(CustomerStatus.valueOf(v)))
          operations
            .listCustomers(limit, maybeCursor, maybeStatus)
            .map((r: ListCustomers) => r.toHttpResponse())

        case Some(value) if value == getCustomer_ID =>
          val customerId = parseId(msg)
          operations
            .getCustomer(customerId)
            .map((r: Customer) => r.toHttpResponse())

        case Some(value) if value == updateCustomer_ID =>
          val customerId     = parseId(msg)
          val updateCustomer = decodeBody(msg, classOf[UpdateCustomer])
          operations
            .updateCustomer(customerId, updateCustomer)
            .map((r: Customer) => r.toHttpResponse())

        case Some(value) if value == subscribe_ID =>
          val customerId   = parseId(msg)
          val subscription = decodeBody(msg, classOf[CreateSubscription])
          operations
            .createSubscription(customerId, subscription)
            .map((r: SubscriptionWithEntitlements) =>
              r.toHttpResponse(statusCode = 201, headers = Map("Location" -> s"/customers/${r.customerId}/subscriptions"))
            )

        case Some(value) if value == listPlans_ID =>
          operations
            .listPlans()
            .map((r: List[Plan]) =>
              HttpResponse(
                statusCode = 200,
                body = JsonSupport.encode(r)
              )
            )

        case Some(value) if value == listSubscriptions_ID =>
          val customerId = parseId(msg)
          operations.listSubscriptions(customerId).map { (r: List[Subscription]) =>
            HttpResponse(
              statusCode = 200,
              body = JsonSupport.encode(r)
            )
          }

        case Some(value) if value == listEntitlements_ID =>
          val customerId = parseId(msg)
          operations
            .listEntitlements(customerId)
            .map((r: List[Entitlement]) =>
              HttpResponse(
                statusCode = 200,
                body = JsonSupport.encode(r)
              )
            )

        case Some(value) if value == consumeEntitlement_ID =>
          val customerId  = parseId(msg)
          val consumption = decodeBody(msg, classOf[Consumption])
          operations.consumeEntitlement(customerId, consumption).map((r: ConsumeResult) => r.toHttpResponse())

        case Some(value) => throw new Exception(s"Unknown OperationId $value")
        case None        => throw new Exception("No OperationId found")
      }
    } match {
      // Number, status or UUID that cannot be decoded: the client's fault, not ours
      case Failure(exception: InvalidRequest) =>
        logger.warn("Invalid request: {}", exception.getMessage)
        msg.reply(Problem.badRequest(messageOf(exception)).toJson.encode())
      case Failure(exception) =>
        logger.error("An unexpected error occurred...", exception)
        msg.fail(500, messageOf(exception))
      case Success(value)     =>
        value
          .map((v: HttpResponse) => msg.reply(v.toJson.encode()))
          .onFailure {
            case e: DomainError =>
              logger.error("A domain error occured: {}", e.getMessage)
              msg.reply(Problem.fromDomainError(e).toJson.encode())
            case e: Throwable   =>
              logger.error("Failed to process or reply to the customer message: {}", e.getMessage)
              msg.fail(500, messageOf(e))
          }
    }
  }

  // Only the decoding of the request goes through here, so that an IllegalArgumentException
  // thrown by the operations themselves is still a 500
  private final class InvalidRequest(message: String) extends RuntimeException(message)

  private def parse[T](f: => T): T =
    try f
    catch { case e: IllegalArgumentException => throw new InvalidRequest(messageOf(e)) }

  private def parseId(msg: Message[String]): UUID = parse(UUID.fromString(msg.headers().get(ID_PATH_PARAM)))

  private def decodeBody[T](msg: Message[String], clazz: Class[T]): T = parse(JsonSupport.decode(msg.body(), clazz))

  private def messageOf(t: Throwable): String = Option(t.getMessage).getOrElse(t.toString)
}
