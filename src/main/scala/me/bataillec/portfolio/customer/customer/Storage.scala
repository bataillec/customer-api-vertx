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

  def cancelActiveSubscription(tx: SqlClient, customerId: UUID): Future[Unit] = {
    tx
      .preparedQuery(
        """ UPDATE subscriptions
          | SET status = 'CANCELED', end_date = now()
          | WHERE customer_id = $1 AND status = 'ACTIVE'
          |""".stripMargin
      )
      .execute(Tuple.of(customerId))
      .mapEmpty()
  }

  def insertActiveSubscription(tx: SqlClient, customerId: UUID, createSubscription: CreateSubscription): Future[Subscription] = {
    tx
      .preparedQuery(""" INSERT INTO subscriptions (customer_id, plan_type)
          | VALUES ($1, $2::plan_type)
          | RETURNING *
          |""".stripMargin)
      .execute(Tuple.of(customerId, createSubscription.planType.toString))
      .map(r => Subscription.fromRow(r.asScala.head))
  }

  def resetForPlan(tx: SqlClient, customerId: UUID, createSubscription: CreateSubscription, now: Instant): Future[List[Entitlement]] = {
    val resetAt = PlanQuotas.nextResetDate(now).atOffset(ZoneOffset.UTC)
    val upserts = PlanQuotas.forPlan(createSubscription.planType).toList.sortBy(_._1).map { (feature, quota) =>
      tx
        .preparedQuery(
          """ INSERT INTO entitlements (customer_id, feature_key, allocated_quota, consumed_quota, reset_date)
            | VALUES ($1, $2, $3, 0, $4)
            | ON CONFLICT (customer_id, feature_key)
            | DO UPDATE
            | SET allocated_quota = EXCLUDED.allocated_quota, consumed_quota = 0, reset_date = EXCLUDED.reset_date
            | RETURNING feature_key, allocated_quota, consumed_quota, reset_date
            | """.stripMargin
        )
        .execute(
          Tuple.of(
            customerId,
            feature,
            quota,
            resetAt
          )
        )
        .map(r => Entitlement.fromRow(r.asScala.head))
    }
    upserts
      .foldLeft(Future.succeededFuture(List.empty[Entitlement])) { (acc, next) =>
        acc.compose(done => next.map(e => done :+ e))
      }
  }

  def listSubscriptions(tx: SqlClient, customerId: UUID): Future[List[Subscription]] = {
    tx
      .preparedQuery(
        """ SELECT *
          | FROM subscriptions
          | WHERE customer_id = $1
          | ORDER BY start_date DESC, id
          | """.stripMargin
      )
      .execute(Tuple.of(customerId))
      .map(r => r.asScala.map(Subscription.fromRow).toList)
  }

  def listEntitlements(tx: SqlClient, customerId: UUID): Future[List[Entitlement]] = {
    tx
      .preparedQuery(
        """ SELECT feature_key, allocated_quota, consumed_quota, reset_date
          | FROM entitlements
          | WHERE customer_id = $1
          | ORDER BY feature_key
          | """.stripMargin
      )
      .execute(Tuple.of(customerId))
      .map(r => r.asScala.map(Entitlement.fromRow).toList)
  }

  def findEntitlement(tx: SqlClient, customerId: UUID, featureKey: String): Future[Option[Entitlement]] = {
    tx
      .preparedQuery(
        """ SELECT feature_key, allocated_quota, consumed_quota, reset_date
          | FROM entitlements
          | WHERE customer_id = $1 AND feature_key = $2
          |""".stripMargin
      )
      .execute(
        Tuple.of(
          customerId,
          featureKey
        )
      )
      .map(_.asScala.headOption.map(Entitlement.fromRow))
  }

  def consume(tx: SqlClient, customerId: UUID, consumption: Consumption): Future[Option[ConsumeResult]] = {
    tx
      .preparedQuery(
        """ UPDATE entitlements
          | SET consumed_quota = consumed_quota + $3
          | WHERE customer_id = $1
          | AND feature_key = $2
          | AND consumed_quota + $3 <= allocated_quota
          | RETURNING feature_key, allocated_quota, consumed_quota
          |""".stripMargin
      )
      .execute(
        Tuple.of(
          customerId,
          consumption.featureKey,
          consumption.amount
        )
      )
      .map(_.asScala.headOption.map(ConsumeResult.fromRow))
  }
}
