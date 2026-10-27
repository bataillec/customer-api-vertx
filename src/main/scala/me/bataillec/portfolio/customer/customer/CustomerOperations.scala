package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import io.vertx.sqlclient.Pool
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.eventbus.AuditSender
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.{Base64, UUID}
import scala.util.{Failure, Success, Try}

class CustomerOperations(pool: Pool, auditSender: AuditSender) {
  private val storage = new Storage(pool)

  private val logger = LoggerFactory.getLogger(getClass)

  def createCustomer(createCustomer: CreateCustomer, maybeIdempotencyKey: Option[String]): Future[Customer] = {
    logger.debug("Creating a new customer: {}", createCustomer)
    for {
      c <- storage.insertCustomer(createCustomer)
    } yield {
      auditSender.send("CUSTOMER_CREATED", c.id)
      c
    }
  }

  def listCustomers(limit: Int, maybeCursor: Option[String], maybeStatus: Option[CustomerStatus]): Future[ListCustomers] = {
    logger.debug("Listing customers: limit={}, cursor={}, status={}", limit, maybeCursor, maybeStatus)
    Try(maybeCursor.map(decodeCursor)) match {
      case Failure(_)      => Future.failedFuture(new IllegalArgumentException("Invalid cursor"))
      case Success(cursor) =>
        // Fetch one extra row to know whether another page exists.
        storage.listCustomers(limit + 1, cursor, maybeStatus).map { rows =>
          val items      = rows.take(limit)
          val nextCursor = if (rows.size > limit) items.lastOption.map(encodeCursor) else None
          ListCustomers(items, nextCursor)
        }
    }
  }

  def getCustomer(customerId: UUID): Future[Customer] = {
    logger.debug("Getting the customer with id = {}", customerId.toString)
    storage.findCustomer(customerId).compose {
      case Some(customer) => Future.succeededFuture(customer)
      case None           => Future.failedFuture(CustomerNotFound(customerId))
    }
  }

  def updateCustomer(customerId: UUID, updateCustomer: UpdateCustomer): Future[Customer] = {
    logger.debug("Updating the customer {} using {}", customerId, updateCustomer)
    pool.withTransaction { tx =>
      storage.lockCustomerForUpdate(tx, customerId).compose { exist =>
        if (!exist) {
          Future.failedFuture(CustomerNotFound(customerId))
        } else {
          storage.updateCustomer(tx, customerId, updateCustomer)
        }
      }
    }
  }

  // Opaque cursor: base64url("<createdAt>|<id>") of the last item of the previous page.
  private def encodeCursor(c: Customer): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(s"${c.createdAt}|${c.id}".getBytes(UTF_8))

  private def decodeCursor(cursor: String): (Instant, UUID) = {
    val Array(createdAt, id) = new String(Base64.getUrlDecoder.decode(cursor), UTF_8).split('|')
    (Instant.parse(createdAt), UUID.fromString(id))
  }
}
