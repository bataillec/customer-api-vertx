package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import io.vertx.sqlclient.{Pool, Tuple}
import me.bataillec.portfolio.customer.domain.*

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class Storage(pool: Pool) {
  def insertCustomer(customer: CreateCustomer): Future[Customer] = {
    pool
      .preparedQuery(
        """ INSERT INTO customers (
          |   email,
          |   company_name
          | ) VALUES (
          |   $1,
          |   $2
          |) RETURNING *
          |""".stripMargin
      )
      .execute(Tuple.of(customer.email, customer.companyName.orNull))
      .map(rows => Customer.fromRow(rows.iterator().next()))
      .recover { case e =>
        Future.failedFuture(e)
      }
  }

  /** Keyset pagination: returns up to `limit` customers, newest first, strictly after `after` (created_at, id). */
  def listCustomers(limit: Int, after: Option[(Instant, UUID)], status: Option[CustomerStatus]): Future[List[Customer]] = {
    val parameters = Tuple.of(
      after.map(_._1.atOffset(ZoneOffset.UTC)).orNull,
      after.map(_._2).orNull,
      Int.box(limit)
    )

    val statusFilter = status match {
      case Some(value) =>
        parameters.addString(value.toString)
        "status = $4 AND "
      case None        => ""
    }

    pool
      .preparedQuery(
        """ SELECT *
          | FROM customers
          | WHERE """.stripMargin + statusFilter + """($1::timestamptz IS NULL OR (created_at, id) < ($1::timestamptz, $2::uuid))
          | ORDER BY created_at DESC, id DESC
          | LIMIT $3""".stripMargin
      )
      .execute(
        parameters
      )
      .map(rows => rows.asScala.map(Customer.fromRow).toList)
  }
}
