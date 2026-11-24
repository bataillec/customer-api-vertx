package me.bataillec.portfolio.customer.integration

import io.vertx.core.Future
import me.bataillec.portfolio.customer.customer.{CustomerCursor, PGStorage}
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import me.bataillec.portfolio.customer.support.PostgresFixture
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.UUID
import scala.compiletime.uninitialized

/** The real SQL of PGStorage, which the unit spec only checks as fragments over a mocked pool. */
class PGStorageIT extends AnyWordSpec with Matchers with PostgresFixture {
  private var storage: PGStorage = uninitialized

  private val now = Instant.parse("2026-11-19T12:00:00Z")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    storage = new PGStorage(pool)
  }

  private def insert(email: String, company: Option[String]): Future[Customer] =
    storage.inTransaction(tx => storage.insertCustomer(tx, CreateCustomer(email, company)))

  private def newCustomer(email: String = s"${UUID.randomUUID()}@example.com", company: Option[String] = None): Customer =
    await(insert(email, company))

  private def subscribe(customer: Customer, plan: PlanType, at: Instant = now): List[Entitlement] =
    await(storage.inTransaction { tx =>
      storage
        .insertActiveSubscription(tx, customer.id, CreateSubscription(plan))
        .compose(_ => storage.applyPlan(tx, customer.id, CreateSubscription(plan), at))
    })

  private def consume(customer: Customer, feature: String, amount: Long, at: Instant = now): Option[ConsumeResult] =
    await(storage.inTransaction(tx => storage.consume(tx, customer.id, Consumption(feature, amount), at)))

  private def entitlements(customer: Customer): Map[String, Entitlement] =
    await(storage.listEntitlements(customer.id)).map(e => e.featureKey -> e).toMap

  "ping" should {
    "reach the database" in {
      noException should be thrownBy await(storage.ping())
    }
  }

  "insertCustomer and findCustomer" should {
    "store a customer as ACTIVE and read it back" in {
      val created = newCustomer("jane@example.com", Some("Acme"))

      created.status shouldBe CustomerStatus.ACTIVE
      created.companyName shouldBe Some("Acme")
      await(storage.findCustomer(created.id)) shouldBe Some(created)
    }

    "store a missing company name as None" in {
      newCustomer(company = None).companyName shouldBe None
    }

    "return None for an unknown customer" in {
      await(storage.findCustomer(UUID.randomUUID())) shouldBe None
    }

    "map a duplicate email to EmailAlreadyUsed" in {
      newCustomer("dup@example.com")

      failureOf(insert("dup@example.com", None)) shouldBe DomainError.EmailAlreadyUsed("dup@example.com")
    }

    "treat emails that only differ by their case as the same one" in {
      newCustomer("Case@Example.com")

      failureOf(insert("case@example.COM", None)) shouldBe a[DomainError.EmailAlreadyUsed]
    }

    "keep the email of a DELETED customer reserved" in {
      val customer = newCustomer("gone@example.com")
      await(storage.inTransaction(tx => storage.updateCustomer(tx, customer.id, UpdateCustomer(None, Some(CustomerStatus.DELETED)))))

      failureOf(insert("gone@example.com", None)) shouldBe a[DomainError.EmailAlreadyUsed]
    }
  }

  "updateCustomer and lockCustomerForUpdate" should {
    "update the company name only, leaving the status" in {
      val customer = newCustomer(company = Some("Old"))

      val updated = await(storage.inTransaction(tx => storage.updateCustomer(tx, customer.id, UpdateCustomer(Some("New"), None))))

      updated.companyName shouldBe Some("New")
      updated.status shouldBe CustomerStatus.ACTIVE
      updated.updatedAt.isAfter(customer.updatedAt) shouldBe true
    }

    "update the status only, leaving the company name" in {
      val customer = newCustomer(company = Some("Keep"))

      val updated = await(storage.inTransaction(tx => storage.updateCustomer(tx, customer.id, UpdateCustomer(None, Some(CustomerStatus.SUSPENDED)))))

      updated.status shouldBe CustomerStatus.SUSPENDED
      updated.companyName shouldBe Some("Keep")
    }

    "return the status of a locked customer, None when it does not exist" in {
      val customer = newCustomer()

      await(storage.inTransaction(tx => storage.lockCustomerForUpdate(tx, customer.id))) shouldBe Some(CustomerStatus.ACTIVE)
      await(storage.inTransaction(tx => storage.lockCustomerForUpdate(tx, UUID.randomUUID()))) shouldBe None
    }
  }

  "listCustomers" should {
    "return the customers newest first" in {
      val first  = newCustomer()
      val second = newCustomer()
      val third  = newCustomer()

      await(storage.listCustomers(10, None, None)).map(_.id) shouldBe List(third.id, second.id, first.id)
    }

    "honour the limit" in {
      (1 to 5).foreach(_ => newCustomer())

      await(storage.listCustomers(2, None, None)) should have size 2
    }

    "walk through every customer exactly once with the cursor" in {
      val all = (1 to 7).map(_ => newCustomer()).reverse.map(_.id).toList

      def page(after: Option[(Instant, UUID)]): List[Customer] = await(storage.listCustomers(3, after, None))
      val pages = Iterator
        .iterate((page(None), 0)) { case (items, n) => (page(items.lastOption.map(c => (c.createdAt, c.id))), n + 1) }
        .takeWhile(_._1.nonEmpty)
        .map(_._1)
        .toList

      pages.map(_.size) shouldBe List(3, 3, 1)
      pages.flatten.map(_.id) shouldBe all
    }

    "keep a stable order for customers created at the same instant, by id" in {
      val sameInstant = "2026-01-01T00:00:00Z"
      val ids         = (1 to 4).map(i => UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
      ids.zipWithIndex.foreach((id, i) =>
        sql(s"INSERT INTO customers (id, email, created_at) VALUES ('$id', 'tie$i@example.com', '$sameInstant')")
      )

      val firstPage  = await(storage.listCustomers(2, None, None))
      val secondPage = await(storage.listCustomers(2, firstPage.lastOption.map(c => (c.createdAt, c.id)), None))

      (firstPage ++ secondPage).map(_.id) shouldBe ids.reverse.toList
    }

    "not skip or repeat a customer when one is created between two pages" in {
      val oldest = newCustomer()
      val middle = newCustomer()
      val newest = newCustomer()
      val first  = await(storage.listCustomers(1, None, None))
      first.map(_.id) shouldBe List(newest.id)

      val late = newCustomer() // newer than the cursor, so it belongs to the pages before it
      val next = await(storage.listCustomers(10, first.lastOption.map(c => (c.createdAt, c.id)), None))

      next.map(_.id) shouldBe List(middle.id, oldest.id)
      next.map(_.id) should not contain late.id
    }

    "still page after a cursor whose customer no longer exists" in {
      val older = newCustomer()
      val gone  = newCustomer()
      val newer = newCustomer()
      val stale = (gone.createdAt, gone.id)
      sql(s"DELETE FROM customers WHERE id = '${gone.id}'")

      val result = await(storage.listCustomers(10, Some(stale), None))

      result.map(_.id) shouldBe List(older.id)
      result.map(_.id) should not contain newer.id
    }

    "decode the cursor it is given back to the same position" in {
      val customer = newCustomer()
      val decoded  = CustomerCursor.decodeCursor(CustomerCursor.encodeCursor(customer))

      decoded shouldBe (customer.createdAt, customer.id)
    }

    "reject a cursor that is garbage" in {
      an[Exception] should be thrownBy CustomerCursor.decodeCursor("not-a-cursor")
    }

    "hide the DELETED customers by default" in {
      val active  = newCustomer()
      val deleted = newCustomer()
      sql(s"UPDATE customers SET status = 'DELETED' WHERE id = '${deleted.id}'")

      await(storage.listCustomers(10, None, None)).map(_.id) shouldBe List(active.id)
    }

    "filter by status" in {
      val active    = newCustomer()
      val suspended = newCustomer()
      val deleted   = newCustomer()
      sql(s"UPDATE customers SET status = 'SUSPENDED' WHERE id = '${suspended.id}'")
      sql(s"UPDATE customers SET status = 'DELETED' WHERE id = '${deleted.id}'")

      await(storage.listCustomers(10, None, Some(CustomerStatus.SUSPENDED))).map(_.id) shouldBe List(suspended.id)
      await(storage.listCustomers(10, None, Some(CustomerStatus.DELETED))).map(_.id) shouldBe List(deleted.id)
      await(storage.listCustomers(10, None, Some(CustomerStatus.ACTIVE))).map(_.id) shouldBe List(active.id)
    }

    "combine the status filter with the cursor" in {
      val a = newCustomer()
      val b = newCustomer()
      newCustomer() // a SUSPENDED one in between must not be returned
      sql(s"UPDATE customers SET status = 'SUSPENDED' WHERE id NOT IN ('${a.id}', '${b.id}')")
      val c = newCustomer()

      val result = await(storage.listCustomers(10, Some((c.createdAt, c.id)), Some(CustomerStatus.ACTIVE)))

      result.map(_.id) shouldBe List(b.id, a.id)
    }
  }

  "listPlans" should {
    "group the seeded quotas by plan, in the order of the plan enum" in {
      val plans = await(storage.listPlans())

      plans.map(_.planType) shouldBe List(PlanType.FREE_TIER, PlanType.PRO, PlanType.ENTERPRISE)
      plans.find(_.planType == PlanType.PRO).get.quotas("API_CALLS_MONTHLY") shouldBe PlanQuota(100000, resetsMonthly = true)
      plans.find(_.planType == PlanType.PRO).get.quotas("STORAGE_GB") shouldBe PlanQuota(50, resetsMonthly = false)
    }
  }

  "the subscriptions" should {
    "reject a second ACTIVE subscription of the same customer" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      val error = failureOf(storage.inTransaction(tx => storage.insertActiveSubscription(tx, customer.id, CreateSubscription(PlanType.PRO))))

      error.getMessage should include("unique_active_subscription")
    }

    "find the active plan, none once it is canceled" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.PRO)

      await(storage.inTransaction(tx => storage.findActivePlan(tx, customer.id))) shouldBe Some(PlanType.PRO)

      await(storage.inTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))

      await(storage.inTransaction(tx => storage.findActivePlan(tx, customer.id))) shouldBe None
      val history = await(storage.listSubscriptions(customer.id))
      history.map(_.status) shouldBe List(SubscriptionStatus.CANCELED)
      history.head.endDate shouldBe defined
    }

    "list the subscriptions of a customer, newest first" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      await(storage.inTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))
      subscribe(customer, PlanType.PRO)

      await(storage.listSubscriptions(customer.id)).map(s => (s.planType, s.status)) shouldBe
        List((PlanType.PRO, SubscriptionStatus.ACTIVE), (PlanType.FREE_TIER, SubscriptionStatus.CANCELED))
    }
  }

  "applyPlan" should {
    "allocate the quotas of the plan, resetting the monthly ones at the start of next month" in {
      val customer = newCustomer()

      val result = subscribe(customer, PlanType.PRO).map(e => e.featureKey -> e).toMap

      result("API_CALLS_MONTHLY") shouldBe Entitlement("API_CALLS_MONTHLY", 100000, 0, Some(Instant.parse("2026-12-01T00:00:00Z")))
      result("STORAGE_GB") shouldBe Entitlement("STORAGE_GB", 50, 0, None)
    }

    "keep the usage on a plan change, only the allocation follows the new plan" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      consume(customer, "API_CALLS_MONTHLY", 400) shouldBe defined
      consume(customer, "STORAGE_GB", 1) shouldBe defined

      await(storage.inTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))
      subscribe(customer, PlanType.PRO)

      val after = entitlements(customer)
      after("API_CALLS_MONTHLY").allocatedQuota shouldBe 100000
      after("API_CALLS_MONTHLY").consumedQuota shouldBe 400
      after("STORAGE_GB").allocatedQuota shouldBe 50
      after("STORAGE_GB").consumedQuota shouldBe 1
    }

    "not let a downgrade erase the usage either" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.PRO)
      consume(customer, "API_CALLS_MONTHLY", 5000) shouldBe defined

      await(storage.inTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))
      subscribe(customer, PlanType.FREE_TIER)

      val after = entitlements(customer)("API_CALLS_MONTHLY")
      after.allocatedQuota shouldBe 1000
      after.consumedQuota shouldBe 5000
      consume(customer, "API_CALLS_MONTHLY", 1) shouldBe None // already over the new quota
    }

    "keep the reset date of an entitlement that already has one" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER, at = Instant.parse("2026-11-19T12:00:00Z"))

      subscribe0(customer, PlanType.PRO, at = Instant.parse("2026-11-25T12:00:00Z"))

      entitlements(customer)("API_CALLS_MONTHLY").resetDate shouldBe Some(Instant.parse("2026-12-01T00:00:00Z"))
    }

    "delete the entitlements that are not part of the new plan" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.PRO)
      sql(s"INSERT INTO entitlements (customer_id, feature_key, allocated_quota) VALUES ('${customer.id}', 'LEGACY_FEATURE', 10)")
      entitlements(customer).keySet should contain("LEGACY_FEATURE")

      subscribe0(customer, PlanType.ENTERPRISE, at = now)

      entitlements(customer).keySet shouldBe Set("API_CALLS_MONTHLY", "STORAGE_GB")
    }

    "fail with PlanNotConfigured when a plan has no quotas" in {
      val customer = newCustomer()
      sql("DELETE FROM plan_quotas WHERE plan_type = 'PRO'")
      try {
        val error = failureOf(storage.inTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.PRO), now)))

        error shouldBe DomainError.PlanNotConfigured(PlanType.PRO)
      } finally {
        sql("""INSERT INTO plan_quotas (plan_type, feature_key, quota, resets_monthly)
              |VALUES ('PRO', 'API_CALLS_MONTHLY', 100000, TRUE), ('PRO', 'STORAGE_GB', 50, FALSE)""".stripMargin)
      }
    }

    "roll back everything when the transaction fails" in {
      val customer = newCustomer()

      val error = failureOf(storage.inTransaction { tx =>
        storage
          .insertActiveSubscription(tx, customer.id, CreateSubscription(PlanType.PRO))
          .compose(_ => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.PRO), now))
          .compose(_ => Future.failedFuture[Unit](new RuntimeException("boom")))
      })

      error.getMessage shouldBe "boom"
      await(storage.listSubscriptions(customer.id)) shouldBe empty
      await(storage.listEntitlements(customer.id)) shouldBe empty
    }
  }

  // The same as subscribe, for a customer that already has a subscription: only the entitlements are aligned.
  private def subscribe0(customer: Customer, plan: PlanType, at: Instant): List[Entitlement] =
    await(storage.inTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(plan), at)))

  "listEntitlements and findEntitlement" should {
    "list the entitlements of a customer by feature key" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      await(storage.listEntitlements(customer.id)).map(_.featureKey) shouldBe List("API_CALLS_MONTHLY", "STORAGE_GB")
    }

    "find one entitlement, None for an unknown feature" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      await(storage.inTransaction(tx => storage.findEntitlement(tx, customer.id, "STORAGE_GB"))).map(_.allocatedQuota) shouldBe Some(1)
      await(storage.inTransaction(tx => storage.findEntitlement(tx, customer.id, "NOPE"))) shouldBe None
    }
  }

  "consume" should {
    "add the amount to the usage and report what remains" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      consume(customer, "API_CALLS_MONTHLY", 100) shouldBe Some(ConsumeResult("API_CALLS_MONTHLY", 1000, 100, 900))
      consume(customer, "API_CALLS_MONTHLY", 250) shouldBe Some(ConsumeResult("API_CALLS_MONTHLY", 1000, 350, 650))
    }

    "allow consuming exactly what remains, and nothing more" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      consume(customer, "API_CALLS_MONTHLY", 999) shouldBe defined

      consume(customer, "API_CALLS_MONTHLY", 2) shouldBe None
      consume(customer, "API_CALLS_MONTHLY", 1).map(_.remainingQuota) shouldBe Some(0)
      consume(customer, "API_CALLS_MONTHLY", 1) shouldBe None
      entitlements(customer)("API_CALLS_MONTHLY").consumedQuota shouldBe 1000
    }

    "refuse an amount bigger than the whole quota without changing the usage" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      consume(customer, "API_CALLS_MONTHLY", 5000) shouldBe None
      entitlements(customer)("API_CALLS_MONTHLY").consumedQuota shouldBe 0
    }

    "return None for an unknown feature" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      consume(customer, "NOPE", 1) shouldBe None
    }

    "return None without an active subscription" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      await(storage.inTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))

      consume(customer, "API_CALLS_MONTHLY", 1) shouldBe None
    }

    "start a new period when the reset date has passed" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      consume(customer, "API_CALLS_MONTHLY", 1000) shouldBe defined
      consume(customer, "API_CALLS_MONTHLY", 1) shouldBe None

      val nextMonth = Instant.parse("2026-12-03T08:00:00Z")
      consume(customer, "API_CALLS_MONTHLY", 10, at = nextMonth) shouldBe Some(ConsumeResult("API_CALLS_MONTHLY", 1000, 10, 990))

      val after = entitlements(customer)("API_CALLS_MONTHLY")
      after.consumedQuota shouldBe 10
      after.resetDate shouldBe Some(Instant.parse("2027-01-01T00:00:00Z"))
    }

    "reset exactly at the reset date" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      consume(customer, "API_CALLS_MONTHLY", 1000) shouldBe defined

      consume(customer, "API_CALLS_MONTHLY", 1, at = Instant.parse("2026-11-30T23:59:59Z")) shouldBe None
      consume(customer, "API_CALLS_MONTHLY", 1, at = Instant.parse("2026-12-01T00:00:00Z")) shouldBe defined
    }

    "not reset STORAGE_GB whatever the date" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      consume(customer, "STORAGE_GB", 1) shouldBe defined

      consume(customer, "STORAGE_GB", 1, at = Instant.parse("2030-01-01T00:00:00Z")) shouldBe None
      entitlements(customer)("STORAGE_GB").consumedQuota shouldBe 1
      entitlements(customer)("STORAGE_GB").resetDate shouldBe None
    }

    "never let the usage exceed the allocation under concurrent consumption" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)
      sql(s"UPDATE entitlements SET allocated_quota = 25 WHERE customer_id = '${customer.id}' AND feature_key = 'API_CALLS_MONTHLY'")

      val attempts = (1 to 100).map { _ =>
        storage.inTransaction(tx => storage.consume(tx, customer.id, Consumption("API_CALLS_MONTHLY", 1), now))
      }
      val results = attempts.map(f => await(f, 30))

      results.count(_.isDefined) shouldBe 25
      results.count(_.isEmpty) shouldBe 75
      entitlements(customer)("API_CALLS_MONTHLY").consumedQuota shouldBe 25
    }

    "grant the remaining quota to exactly one of two concurrent bigger consumptions" in {
      val customer = newCustomer()
      subscribe(customer, PlanType.FREE_TIER)

      val results = (1 to 2).map(_ => storage.inTransaction(tx => storage.consume(tx, customer.id, Consumption("API_CALLS_MONTHLY", 600), now))).map(f => await(f, 30))

      results.count(_.isDefined) shouldBe 1
      entitlements(customer)("API_CALLS_MONTHLY").consumedQuota shouldBe 600
    }
  }
}
