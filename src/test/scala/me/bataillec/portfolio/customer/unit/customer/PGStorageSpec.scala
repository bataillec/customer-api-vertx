package me.bataillec.portfolio.customer.unit.customer

import io.vertx.core.Future
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.*
import me.bataillec.portfolio.customer.customer.PGStorage
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID
import java.util.function.Function as JFunction
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer

/** Unit tests of [[PGStorage]] on a mocked `Pool`.
  *
  * The SQL is not run: each statement is only checked by a significant fragment, and the parameters by their values. The validity of the SQL itself is covered
  * by running the migrations and the service against a real Postgres.
  */
class PGStorageSpec extends AnyWordSpec with Matchers {
  private val id      = UUID.fromString("11111111-2222-3333-4444-555555555555")
  private val subId   = UUID.fromString("66666666-7777-8888-9999-000000000000")
  private val created = Instant.parse("2026-01-02T03:04:05Z")
  private val now     = Instant.parse("2026-01-15T10:00:00Z")
  private val nextReset = Instant.parse("2026-02-01T00:00:00Z")
  private val boom    = new RuntimeException("boom")

  private def utc(i: Instant): OffsetDateTime = i.atOffset(ZoneOffset.UTC)

  // region Helpers

  /** A mocked `Row` that answers the typed getters from `values`. */
  private def row(values: (String, Any)*): Row = {
    val map = values.toMap
    val r   = mock(classOf[Row])
    when(r.getString(anyString())).thenAnswer(i => map.getOrElse(i.getArgument[String](0), null).asInstanceOf[String])
    when(r.getUUID(anyString())).thenAnswer(i => map.getOrElse(i.getArgument[String](0), null).asInstanceOf[UUID])
    when(r.getOffsetDateTime(anyString())).thenAnswer(i => map.getOrElse(i.getArgument[String](0), null).asInstanceOf[OffsetDateTime])
    when(r.getLong(anyString())).thenAnswer(i => map.getOrElse(i.getArgument[String](0), null).asInstanceOf[java.lang.Long])
    when(r.getBoolean(anyString())).thenAnswer(i => map.getOrElse(i.getArgument[String](0), null).asInstanceOf[java.lang.Boolean])
    r
  }

  private def rowSet(rows: Seq[Row]): RowSet[Row] = {
    val rs = mock(classOf[RowSet[Row]])
    when(rs.iterator()).thenAnswer { _ =>
      val it = mock(classOf[RowIterator[Row]])
      val underlying = rows.iterator
      when(it.hasNext).thenAnswer(_ => underlying.hasNext)
      when(it.next()).thenAnswer(_ => underlying.next())
      it
    }
    rs
  }

  private def customerRow(status: String = "ACTIVE", company: String = "ACME"): Row =
    row("id" -> id, "email" -> "a@b.c", "company_name" -> company, "status" -> status, "created_at" -> utc(created), "updated_at" -> utc(created))

  private val customer = Customer(id, "a@b.c", Some("ACME"), CustomerStatus.ACTIVE, created, created)

  private def subscriptionRow(end: OffsetDateTime = null): Row =
    row("id" -> subId, "customer_id" -> id, "plan_type" -> "PRO", "status" -> "ACTIVE", "start_date" -> utc(created), "end_date" -> end)

  private def entitlementRow(feature: String = "api_calls", allocated: Long = 100L, consumed: Long = 10L, reset: OffsetDateTime = null): Row =
    row("feature_key" -> feature, "allocated_quota" -> allocated, "consumed_quota" -> consumed, "reset_date" -> reset)

  private def quotaRow(plan: String, feature: String, quota: Long, monthly: Boolean): Row =
    row("plan_type" -> plan, "feature_key" -> feature, "quota" -> quota, "resets_monthly" -> monthly)

  private def values(t: Tuple): Seq[Any] = (0 until t.size()).map(t.getValue)

  /** The statements seen by the mocked pool and transaction client. */
  private case class Call(sql: String, params: Option[Tuple])

  /** A mocked `Pool` and transaction client: every prepared query is recorded and answered by `answer`. */
  private class Db(answer: Call => Future[RowSet[Row]]) {
    val calls: ListBuffer[Call] = ListBuffer.empty
    val pool: Pool              = mockClient(classOf[Pool])
    val tx: SqlConnection       = mockClient(classOf[SqlConnection])
    val storage                 = new PGStorage(pool)

    def last: Call = calls.last

    private def mockClient[T <: SqlClient](clazz: Class[T]): T = {
      val client = mock(clazz)
      when(client.preparedQuery(anyString())).thenAnswer { invocation =>
        val sql = invocation.getArgument[String](0)
        val pq  = mock(classOf[PreparedQuery[RowSet[Row]]])
        when(pq.execute(any[Tuple]())).thenAnswer { i =>
          val call = Call(sql, Some(i.getArgument[Tuple](0)))
          calls += call
          answer(call)
        }
        when(pq.execute()).thenAnswer { _ =>
          val call = Call(sql, None)
          calls += call
          answer(call)
        }
        pq
      }
      client
    }

    when(pool.withTransaction[Any](any[JFunction[SqlConnection, Future[Any]]]())).thenAnswer { invocation =>
      invocation.getArgument[JFunction[SqlConnection, Future[Any]]](0).apply(tx)
    }
  }

  private def returning(rows: Row*): Db = new Db(_ => Future.succeededFuture(rowSet(rows)))

  private def failing(error: Throwable): Db = new Db(_ => Future.failedFuture[RowSet[Row]](error))

  // endregion

  "inTransaction" should {
    "run the function on the transaction client and return its result" in {
      val db     = returning()
      val result = await(db.storage.inTransaction(client => Future.succeededFuture((client eq db.tx, 42))))

      result shouldBe (true, 42)
    }
  }

  "ping" should {
    "run SELECT 1" in {
      val db    = returning()
      val query = mock(classOf[Query[RowSet[Row]]])
      when(db.pool.query("SELECT 1")).thenReturn(query)
      val empty = rowSet(Nil)
      when(query.execute()).thenReturn(Future.succeededFuture(empty))

      await(db.storage.ping())

      verify(db.pool).query("SELECT 1")
    }

    "fail when the database is unreachable" in {
      val db    = returning()
      val query = mock(classOf[Query[RowSet[Row]]])
      when(db.pool.query("SELECT 1")).thenReturn(query)
      when(query.execute()).thenReturn(Future.failedFuture[RowSet[Row]](boom))

      failureOf(db.storage.ping()) shouldBe boom
    }
  }

  "insertCustomer" should {
    "insert the customer and return it" in {
      val db = returning(customerRow())

      await(db.storage.insertCustomer(CreateCustomer("a@b.c", Some("ACME")))) shouldBe customer

      db.last.sql should include("INSERT INTO customers")
      values(db.last.params.get) shouldBe Seq("a@b.c", "ACME")
    }

    "send a null company name when there is none" in {
      val db = returning(customerRow(company = null))

      await(db.storage.insertCustomer(CreateCustomer("a@b.c", None))).companyName shouldBe None

      values(db.last.params.get) shouldBe Seq("a@b.c", null)
    }

    "fail with EmailAlreadyUsed on a unique violation" in {
      val db = failing(new PgException("duplicate", "ERROR", "23505", "detail"))

      failureOf(db.storage.insertCustomer(CreateCustomer("a@b.c", None))) shouldBe EmailAlreadyUsed("a@b.c")
    }

    "propagate a Postgres error that is not a unique violation" in {
      val error = new PgException("bad", "ERROR", "42601", "detail")
      val db    = failing(error)

      failureOf(db.storage.insertCustomer(CreateCustomer("a@b.c", None))) shouldBe error
    }

    "propagate any other error" in {
      failureOf(failing(boom).storage.insertCustomer(CreateCustomer("a@b.c", None))) shouldBe boom
    }
  }

  "listCustomers" should {
    "hide the deleted customers on the first page when no status is asked" in {
      val db = returning(customerRow())

      await(db.storage.listCustomers(10, None, None)) shouldBe List(customer)

      db.last.sql should include("status <> 'DELETED'")
      db.last.sql should not include "$4"
      values(db.last.params.get) shouldBe Seq(null, null, 10)
    }

    "page after a (created_at, id) pair" in {
      val db = returning()

      await(db.storage.listCustomers(5, Some((created, id)), None)) shouldBe Nil

      values(db.last.params.get) shouldBe Seq(utc(created), id, 5)
    }

    "filter on the status when it is asked" in {
      val db = returning(customerRow("SUSPENDED"))

      await(db.storage.listCustomers(5, None, Some(CustomerStatus.SUSPENDED))).map(_.status) shouldBe List(CustomerStatus.SUSPENDED)

      db.last.sql should include("status = $4")
      values(db.last.params.get) shouldBe Seq(null, null, 5, "SUSPENDED")
    }

    "propagate the failure" in {
      failureOf(failing(boom).storage.listCustomers(5, None, None)) shouldBe boom
    }
  }

  "findCustomer" should {
    "return the customer" in {
      val db = returning(customerRow())

      await(db.storage.findCustomer(id)) shouldBe Some(customer)

      values(db.last.params.get) shouldBe Seq(id)
    }

    "return None when there is no row" in {
      await(returning().storage.findCustomer(id)) shouldBe None
    }
  }

  "lockCustomerForUpdate" should {
    "lock the row and return the status" in {
      val db = returning(row("status" -> "SUSPENDED"))

      await(db.storage.lockCustomerForUpdate(db.tx, id)) shouldBe Some(CustomerStatus.SUSPENDED)

      db.last.sql should include("FOR UPDATE")
      values(db.last.params.get) shouldBe Seq(id)
    }

    "return None when the customer does not exist" in {
      val db = returning()

      await(db.storage.lockCustomerForUpdate(db.tx, id)) shouldBe None
    }
  }

  "updateCustomer" should {
    "send the new values" in {
      val db = returning(customerRow("SUSPENDED", "New"))

      await(db.storage.updateCustomer(db.tx, id, UpdateCustomer(Some("New"), Some(CustomerStatus.SUSPENDED)))).companyName shouldBe Some("New")

      db.last.sql should include("UPDATE customers")
      values(db.last.params.get) shouldBe Seq(id, "New", "SUSPENDED")
    }

    "send nulls for the fields that are not changed" in {
      val db = returning(customerRow())

      await(db.storage.updateCustomer(db.tx, id, UpdateCustomer(None, None))) shouldBe customer

      values(db.last.params.get) shouldBe Seq(id, null, null)
    }
  }

  "cancelActiveSubscription" should {
    "cancel the active subscription of the customer" in {
      val db = returning()

      await(db.storage.cancelActiveSubscription(db.tx, id))

      db.last.sql should include("SET status = 'CANCELED'")
      values(db.last.params.get) shouldBe Seq(id)
    }
  }

  "findActivePlan" should {
    "return the plan type" in {
      val db = returning(row("plan_type" -> "ENTERPRISE"))

      await(db.storage.findActivePlan(db.tx, id)) shouldBe Some(PlanType.ENTERPRISE)

      values(db.last.params.get) shouldBe Seq(id)
    }

    "return None without an active subscription" in {
      val db = returning()

      await(db.storage.findActivePlan(db.tx, id)) shouldBe None
    }
  }

  "insertActiveSubscription" should {
    "insert the subscription and return it" in {
      val db = returning(subscriptionRow())

      await(db.storage.insertActiveSubscription(db.tx, id, CreateSubscription(PlanType.PRO))) shouldBe
        Subscription(subId, id, PlanType.PRO, SubscriptionStatus.ACTIVE, created, None)

      db.last.sql should include("INSERT INTO subscriptions")
      values(db.last.params.get) shouldBe Seq(id, "PRO")
    }
  }

  "listPlans" should {
    "group the quotas by plan and keep the order" in {
      val db = returning(
        quotaRow("FREE_TIER", "api_calls", 10L, monthly = true),
        quotaRow("FREE_TIER", "storage_gb", 1L, monthly = false),
        quotaRow("PRO", "api_calls", 1000L, monthly = true)
      )

      await(db.storage.listPlans()) shouldBe List(
        Plan(PlanType.FREE_TIER, ListMap("api_calls" -> PlanQuota(10, true), "storage_gb" -> PlanQuota(1, false))),
        Plan(PlanType.PRO, ListMap("api_calls" -> PlanQuota(1000, true)))
      )

      db.last.sql should include("FROM plan_quotas")
      db.last.params shouldBe None
    }

    "return no plan when nothing is configured" in {
      await(returning().storage.listPlans()) shouldBe Nil
    }
  }

  "findPlanQuotas" should {
    "return the quotas by feature key" in {
      val db = returning(quotaRow("PRO", "api_calls", 1000L, monthly = true), quotaRow("PRO", "storage_gb", 5L, monthly = false))

      await(db.storage.findPlanQuotas(db.tx, PlanType.PRO)) shouldBe Map("api_calls" -> PlanQuota(1000, true), "storage_gb" -> PlanQuota(5, false))

      values(db.last.params.get) shouldBe Seq("PRO")
    }
  }

  "applyPlan" should {
    val create = CreateSubscription(PlanType.PRO)

    /** Answers like Postgres would: the quotas of the plan, an empty delete, and the upserted entitlement echoed back. */
    def planDb(quotas: Seq[Row], failOn: Call => Option[Throwable] = _ => None): Db = new Db({ call =>
      failOn(call) match {
        case Some(error)                                   => Future.failedFuture[RowSet[Row]](error)
        case None if call.sql.contains("FROM plan_quotas") => Future.succeededFuture(rowSet(quotas))
        case None if call.sql.contains("DELETE")           => Future.succeededFuture(rowSet(Nil))
        case None                                          =>
          val p = call.params.get
          Future.succeededFuture(
            rowSet(Seq(entitlementRow(p.getString(1), p.getLong(2), 0L, p.getOffsetDateTime(3))))
          )
      }
    })

    "fail with PlanNotConfigured when the plan has no quota" in {
      val db = planDb(Nil)

      failureOf(db.storage.applyPlan(db.tx, id, create, now)) shouldBe PlanNotConfigured(PlanType.PRO)

      db.calls.map(_.sql).exists(_.contains("DELETE")) shouldBe false
    }

    "drop the entitlements outside the plan and upsert the others by feature key" in {
      val db = planDb(Seq(quotaRow("PRO", "storage_gb", 5L, monthly = false), quotaRow("PRO", "api_calls", 1000L, monthly = true)))

      val result = await(db.storage.applyPlan(db.tx, id, create, now))

      result shouldBe List(Entitlement("api_calls", 1000, 0, Some(nextReset)), Entitlement("storage_gb", 5, 0, None))

      val delete = db.calls.find(_.sql.contains("DELETE FROM entitlements")).get
      delete.params.get.getValue(0) shouldBe id
      delete.params.get.getValue(1).asInstanceOf[Array[String]].toSet shouldBe Set("storage_gb", "api_calls")

      val upserts = db.calls.filter(_.sql.contains("INSERT INTO entitlements"))
      upserts.map(c => c.params.get.getString(1)) shouldBe Seq("api_calls", "storage_gb")
      values(upserts.head.params.get) shouldBe Seq(id, "api_calls", 1000L, utc(nextReset))
      values(upserts.last.params.get) shouldBe Seq(id, "storage_gb", 5L, null)
    }

    "propagate a failure of the delete and not upsert" in {
      val db = planDb(Seq(quotaRow("PRO", "api_calls", 1000L, monthly = true)), c => Option.when(c.sql.contains("DELETE"))(boom))

      failureOf(db.storage.applyPlan(db.tx, id, create, now)) shouldBe boom

      db.calls.exists(_.sql.contains("INSERT INTO entitlements")) shouldBe false
    }

    "propagate a failure of an upsert (all the upserts are sent at once)" in {
      val db = planDb(
        Seq(quotaRow("PRO", "api_calls", 1000L, monthly = true), quotaRow("PRO", "storage_gb", 5L, monthly = false)),
        c => Option.when(c.sql.contains("INSERT INTO entitlements") && c.params.exists(_.getString(1) == "api_calls"))(boom)
      )

      failureOf(db.storage.applyPlan(db.tx, id, create, now)) shouldBe boom

      db.calls.count(_.sql.contains("INSERT INTO entitlements")) shouldBe 2
    }
  }

  "listSubscriptions" should {
    "return the subscriptions, with and without an end date" in {
      val end = utc(now)
      val db  = returning(subscriptionRow(end), subscriptionRow())

      await(db.storage.listSubscriptions(id)).map(_.endDate) shouldBe List(Some(now), None)

      db.last.sql should include("FROM subscriptions")
      values(db.last.params.get) shouldBe Seq(id)
    }
  }

  "listEntitlements" should {
    "return the entitlements" in {
      val db = returning(entitlementRow(reset = utc(nextReset)), entitlementRow("storage_gb", 5L, 1L))

      await(db.storage.listEntitlements(id)) shouldBe List(Entitlement("api_calls", 100, 10, Some(nextReset)), Entitlement("storage_gb", 5, 1, None))

      values(db.last.params.get) shouldBe Seq(id)
    }
  }

  "findEntitlement" should {
    "return the entitlement of the feature" in {
      val db = returning(entitlementRow())

      await(db.storage.findEntitlement(db.tx, id, "api_calls")) shouldBe Some(Entitlement("api_calls", 100, 10, None))

      values(db.last.params.get) shouldBe Seq(id, "api_calls")
    }

    "return None when the feature is not there" in {
      val db = returning()

      await(db.storage.findEntitlement(db.tx, id, "nope")) shouldBe None
    }
  }

  "consume" should {
    "return the new usage" in {
      val db = returning(row("feature_key" -> "api_calls", "allocated_quota" -> 100L, "consumed_quota" -> 15L))

      await(db.storage.consume(db.tx, id, Consumption("api_calls", 5), now)) shouldBe Some(ConsumeResult("api_calls", 100, 15, 85))

      db.last.sql should include("UPDATE entitlements")
      values(db.last.params.get) shouldBe Seq(id, "api_calls", 5L, utc(now), utc(nextReset))
    }

    "return None when the quota would be exceeded" in {
      val db = returning()

      await(db.storage.consume(db.tx, id, Consumption("api_calls", 500), now)) shouldBe None
    }
  }
}
