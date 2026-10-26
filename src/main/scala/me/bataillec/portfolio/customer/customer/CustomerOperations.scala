package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import me.bataillec.portfolio.customer.domain.*
import org.slf4j.LoggerFactory

class CustomerOperations {
  private val logger = LoggerFactory.getLogger(getClass)

  def createCustomer(newCustomer: NewCustomer, maybeIdempotencyKey: Option[String]): Future[Customer] = {
    ???
  }

  def listCustomers(limit: Int, maybeCursor: Option[String], maybeStatus: Option[CustomerStatus]): Future[ListCustomers] = {
    logger.debug("In listCustomers...")
    Future.succeededFuture(ListCustomers(List.empty[Customer], None))
  }
}
