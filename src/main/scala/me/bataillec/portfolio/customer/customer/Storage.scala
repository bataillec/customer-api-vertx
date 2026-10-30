package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.{Pool, Row, SqlClient, Tuple}
import me.bataillec.portfolio.customer.domain.*

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*

class Storage(pool: Pool) {
  def ping(): Future[Unit] = pool.query("SELECT 1").execute().mapEmpty()

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
      case None        => "status <> 'DELETED' AND "
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

  def lockCustomerForUpdate(tx: SqlClient, customerId: UUID): Future[Option[CustomerStatus]] = {
    tx
      .preparedQuery("""SELECT status FROM customers WHERE id = $1 FOR UPDATE""")
      .execute(Tuple.of(customerId))
      .map(_.asScala.headOption.map(v => CustomerStatus.valueOf(v.getString("status"))))
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

  def findActivePlan(tx: SqlClient, customerId: UUID): Future[Option[PlanType]] = {
    tx
      .preparedQuery(
        """ SELECT plan_type
          | FROM subscriptions
          | WHERE customer_id = $1 AND status = 'ACTIVE'
          |""".stripMargin
      )
      .execute(Tuple.of(customerId))
      .map(_.asScala.headOption.map(r => PlanType.valueOf(r.getString("plan_type"))))
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

  def listPlans(): Future[List[Plan]] = {
    pool
      .preparedQuery(
        """ SELECT plan_type, feature_key, quota, resets_monthly
          | FROM plan_quotas
          | ORDER BY plan_type, feature_key
          |""".stripMargin
      )
      .execute()
      .map { rows =>
        rows.asScala
          .map(r => (PlanType.valueOf(r.getString("plan_type")), r.getString("feature_key"), planQuotaFromRow(r)))
          .foldLeft(ListMap.empty[PlanType, ListMap[String, PlanQuota]]) { case (acc, (plan, feature, quota)) =>
            acc.updated(plan, acc.getOrElse(plan, ListMap.empty[String, PlanQuota]).updated(feature, quota))
          }
          .map((plan, quotas) => Plan(plan, quotas))
          .toList
      }
  }

  private def planQuotaFromRow(r: Row): PlanQuota =
    PlanQuota(quota = r.getLong("quota"), resetsMonthly = r.getBoolean("resets_monthly"))

  def findPlanQuotas(client: SqlClient, planType: PlanType): Future[Map[String, PlanQuota]] = {
    client
      .preparedQuery(
        """ SELECT feature_key, quota, resets_monthly
          | FROM plan_quotas
          | WHERE plan_type = $1::plan_type
          |""".stripMargin
      )
      .execute(Tuple.of(planType.toString))
      .map(_.asScala.map(r => r.getString("feature_key") -> planQuotaFromRow(r)).toMap)
  }

  /** Aligns the entitlements with the plan. The usage survives a plan change, so that switching plans can't be used to bypass a quota. */
  def applyPlan(tx: SqlClient, customerId: UUID, createSubscription: CreateSubscription, now: Instant): Future[List[Entitlement]] =
    findPlanQuotas(tx, createSubscription.planType).compose { quotas =>
      if (quotas.isEmpty) Future.failedFuture(DomainError.PlanNotConfigured(createSubscription.planType))
      else deleteEntitlementsOutsidePlan(tx, customerId, quotas.keySet).compose(_ => upsertEntitlements(tx, customerId, quotas, now))
    }

  private def deleteEntitlementsOutsidePlan(tx: SqlClient, customerId: UUID, features: Set[String]): Future[Unit] = {
    tx
      .preparedQuery(
        """ DELETE FROM entitlements
          | WHERE customer_id = $1 AND feature_key <> ALL($2::varchar[])
          |""".stripMargin
      )
      .execute(Tuple.of(customerId, features.toArray))
      .mapEmpty()
  }

  private def upsertEntitlements(tx: SqlClient, customerId: UUID, quotas: Map[String, PlanQuota], now: Instant): Future[List[Entitlement]] = {
    val resetAt = PlanQuotas.nextResetDate(now).atOffset(ZoneOffset.UTC)
    val upserts = quotas.toList.sortBy(_._1).map { (feature, planQuota) =>
      tx
        .preparedQuery(
          """ INSERT INTO entitlements (customer_id, feature_key, allocated_quota, consumed_quota, reset_date)
            | VALUES ($1, $2, $3, 0, $4)
            | ON CONFLICT (customer_id, feature_key)
            | DO UPDATE
            | SET allocated_quota = EXCLUDED.allocated_quota,
            |     reset_date = CASE WHEN EXCLUDED.reset_date IS NULL THEN NULL ELSE COALESCE(entitlements.reset_date, EXCLUDED.reset_date) END
            | RETURNING feature_key, allocated_quota, consumed_quota, reset_date
            | """.stripMargin
        )
        .execute(
          Tuple.of(
            customerId,
            feature,
            planQuota.quota,
            Option.when(planQuota.resetsMonthly)(resetAt).orNull
          )
        )
        .map(r => Entitlement.fromRow(r.asScala.head))
    }
    upserts
      .foldLeft(Future.succeededFuture(List.empty[Entitlement])) { (acc, next) =>
        acc.compose(done => next.map(e => done :+ e))
      }
  }

  def listSubscriptions(customerId: UUID): Future[List[Subscription]] = {
    pool
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

  def listEntitlements(customerId: UUID): Future[List[Entitlement]] = {
    pool
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

  /** Starts a new period when `reset_date` has passed, so the quota is checked against the usage of the current period only. */
  def consume(tx: SqlClient, customerId: UUID, consumption: Consumption, now: Instant): Future[Option[ConsumeResult]] = {
    tx
      .preparedQuery(
        """ UPDATE entitlements
          | SET consumed_quota = CASE WHEN reset_date <= $4 THEN 0 ELSE consumed_quota END + $3::bigint,
          |     reset_date = CASE WHEN reset_date <= $4 THEN $5 ELSE reset_date END
          | WHERE customer_id = $1
          | AND feature_key = $2
          | AND $3::bigint <= allocated_quota - CASE WHEN reset_date <= $4 THEN 0 ELSE consumed_quota END
          | RETURNING feature_key, allocated_quota, consumed_quota
          |""".stripMargin
      )
      .execute(
        Tuple.of(
          customerId,
          consumption.featureKey,
          consumption.amount,
          now.atOffset(ZoneOffset.UTC),
          PlanQuotas.nextResetDate(now).atOffset(ZoneOffset.UTC)
        )
      )
      .map(_.asScala.headOption.map(ConsumeResult.fromRow))
  }
}
