package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.{Pool, SqlClient, Tuple}
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
      .map(r => Customer.fromRow(r.asScala.head))
      .recover {
        case e: PgException if e.getSqlState == "23505" => Future.failedFuture(DomainError.EmailAlreadyUsed(customer.email))
        case e                                          => Future.failedFuture(e)
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
      .execute(parameters)
      .map(rows => rows.asScala.map(Customer.fromRow).toList)
  }

  def findCustomer(id: UUID): Future[Option[Customer]] = {
    pool
      .preparedQuery(
        """ SELECT *
          | FROM customers
          | WHERE id = $1
          |""".stripMargin
      )
      .execute(Tuple.of(id))
      .map(_.asScala.headOption.map(Customer.fromRow))
  }

  def lockCustomerForUpdate(tx: SqlClient, customerId: UUID): Future[Boolean] = {
    tx
      .preparedQuery("""SELECT id FROm customers WHERE id = $1 FOR UPDATE""")
      .execute(Tuple.of(customerId))
      .map(_.asScala.headOption.isDefined)
  }

  def updateCustomer(tx: SqlClient, customerId: UUID, updateCustomer: UpdateCustomer): Future[Customer] = {
    tx
      .preparedQuery(""" UPDATE customers
          | SET company_name = COALESCE($2, company_name),
          |     status = COALESCE($3::customer_status, status),
          |     updated_at = CURRENT_TIMESTAMP
          | WHERE id = $1
          | RETURNING *
          |""".stripMargin)
      .execute(Tuple.of(customerId, updateCustomer.companyName.orNull, updateCustomer.status.map(_.toString).orNull))
      .map(r => Customer.fromRow(r.asScala.head))
  }
}
