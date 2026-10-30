package me.bataillec.portfolio.customer.customer

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.vertx.core.Vertx
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.{Pool, PoolOptions, Tuple}
import me.bataillec.portfolio.customer.domain.*
import org.flywaydb.core.Flyway
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.util.Try

class StorageSpec extends AnyFlatSpec with Matchers with TestContainerForAll {
  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def("postgres:17-alpine")

  private val vertx = Vertx.vertx()

  private def await[T](future: io.vertx.core.Future[T]): T = future.toCompletionStage.toCompletableFuture.get(10, TimeUnit.SECONDS)

  private def withStorage[A](f: (Pool, Storage) => A): A = withContainers { pg =>
    Flyway.configure().dataSource(pg.jdbcUrl, pg.username, pg.password).load().migrate()
    val pool = PgBuilder
      .pool()
      .`with`(PoolOptions().setMaxSize(2))
      .connectingTo(
        PgConnectOptions()
          .setHost(pg.host)
          .setPort(pg.mappedPort(5432))
          .setDatabase(pg.databaseName)
          .setUser(pg.username)
          .setPassword(pg.password)
      )
      .using(vertx)
      .build()
    try f(pool, new Storage(pool))
    finally await(pool.close())
  }

  "findPlanQuotas" should "return the seeded quotas of each plan" in withStorage { (pool, storage) =>
    await(storage.findPlanQuotas(pool, PlanType.FREE_TIER)) shouldBe Map(
      "API_CALLS_MONTHLY" -> PlanQuota(1_000L, resetsMonthly = true),
      "STORAGE_GB"        -> PlanQuota(1L, resetsMonthly = false)
    )
    await(storage.findPlanQuotas(pool, PlanType.PRO)) shouldBe Map(
      "API_CALLS_MONTHLY" -> PlanQuota(100_000L, resetsMonthly = true),
      "STORAGE_GB"        -> PlanQuota(50L, resetsMonthly = false)
    )
    await(storage.findPlanQuotas(pool, PlanType.ENTERPRISE)) shouldBe Map(
      "API_CALLS_MONTHLY" -> PlanQuota(10_000_000L, resetsMonthly = true),
      "STORAGE_GB"        -> PlanQuota(1_000L, resetsMonthly = false)
    )
  }

  "listPlans" should "group the quotas by plan" in withStorage { (_, storage) =>
    val plans = await(storage.listPlans())

    plans.map(_.planType) shouldBe List(PlanType.FREE_TIER, PlanType.PRO, PlanType.ENTERPRISE)
    plans.find(_.planType == PlanType.PRO).get.quotas shouldBe Map(
      "API_CALLS_MONTHLY" -> PlanQuota(100_000L, resetsMonthly = true),
      "STORAGE_GB"        -> PlanQuota(50L, resetsMonthly = false)
    )
  }

  "applyPlan" should "use the quotas stored in the database" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("a@b.com", None)))
    await(pool.preparedQuery("UPDATE plan_quotas SET quota = 42 WHERE plan_type = 'PRO' AND feature_key = 'STORAGE_GB'").execute())

    val entitlements = await(pool.withTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.PRO), Instant.now())))

    entitlements.map(e => e.featureKey -> e.allocatedQuota).toMap shouldBe Map("API_CALLS_MONTHLY" -> 100_000L, "STORAGE_GB" -> 42L)
  }

  it should "fail with PlanNotConfigured when the plan has no quota" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("c@d.com", None)))
    await(pool.preparedQuery("DELETE FROM plan_quotas WHERE plan_type = 'PRO'").execute())

    val result = Try(await(pool.withTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.PRO), Instant.now()))))

    result.failed.get.getCause shouldBe DomainError.PlanNotConfigured(PlanType.PRO)
  }

  it should "remove the entitlements that are not part of the new plan" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("downgrade@b.com", None)))
    await(pool.preparedQuery("INSERT INTO plan_quotas (plan_type, feature_key, quota) VALUES ('ENTERPRISE', 'SEATS', 10)").execute())

    try {
      await(pool.withTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.ENTERPRISE), Instant.now())))
      await(pool.withTransaction(tx => storage.applyPlan(tx, customer.id, CreateSubscription(PlanType.FREE_TIER), Instant.now())))

      await(storage.listEntitlements(customer.id)).map(_.featureKey) shouldBe List("API_CALLS_MONTHLY", "STORAGE_GB")
    } finally await(pool.preparedQuery("DELETE FROM plan_quotas WHERE feature_key = 'SEATS'").execute())
  }

  private def applyPlan(pool: Pool, storage: Storage, customerId: java.util.UUID, planType: PlanType, at: Instant) =
    await(pool.withTransaction(tx => storage.applyPlan(tx, customerId, CreateSubscription(planType), at)))

  it should "keep the usage and the reset date across a plan change" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("keep@b.com", None)))
    applyPlan(pool, storage, customer.id, PlanType.FREE_TIER, Instant.parse("2026-03-15T00:00:00Z"))
    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("API_CALLS_MONTHLY", 1_000), Instant.parse("2026-03-15T00:00:00Z"))))

    val upgraded = applyPlan(pool, storage, customer.id, PlanType.ENTERPRISE, Instant.parse("2026-03-20T00:00:00Z"))
    val apiCalls = upgraded.find(_.featureKey == "API_CALLS_MONTHLY").get

    apiCalls.allocatedQuota shouldBe 10_000_000L
    apiCalls.consumedQuota shouldBe 1_000L
    apiCalls.resetDate shouldBe Some(Instant.parse("2026-04-01T00:00:00Z"))
  }

  it should "refuse consumption when a downgrade leaves the usage above the allocation" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("above@b.com", None)))
    val at       = Instant.parse("2026-03-15T00:00:00Z")
    applyPlan(pool, storage, customer.id, PlanType.ENTERPRISE, at)
    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("API_CALLS_MONTHLY", 5_000), at)))

    val downgraded = applyPlan(pool, storage, customer.id, PlanType.FREE_TIER, at)

    downgraded.find(_.featureKey == "API_CALLS_MONTHLY").get.consumedQuota shouldBe 5_000L
    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("API_CALLS_MONTHLY", 1), at))) shouldBe None
  }

  it should "only set a reset date on the features that reset monthly" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("policy@b.com", None)))

    val entitlements = applyPlan(pool, storage, customer.id, PlanType.FREE_TIER, Instant.parse("2026-03-15T00:00:00Z"))

    entitlements.map(e => e.featureKey -> e.resetDate) shouldBe List(
      "API_CALLS_MONTHLY" -> Some(Instant.parse("2026-04-01T00:00:00Z")),
      "STORAGE_GB"        -> None
    )
  }

  it should "follow the reset policy of the new plan" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("switch@b.com", None)))
    val at       = Instant.parse("2026-03-15T00:00:00Z")
    val update   = "UPDATE plan_quotas SET resets_monthly = $1 WHERE plan_type = 'ENTERPRISE' AND feature_key = 'STORAGE_GB'"
    await(pool.preparedQuery(update).execute(Tuple.of(java.lang.Boolean.TRUE)))

    try {
      def storageReset(planType: PlanType) = applyPlan(pool, storage, customer.id, planType, at).find(_.featureKey == "STORAGE_GB").get.resetDate

      storageReset(PlanType.FREE_TIER) shouldBe None
      storageReset(PlanType.ENTERPRISE) shouldBe Some(Instant.parse("2026-04-01T00:00:00Z"))
      storageReset(PlanType.FREE_TIER) shouldBe None
    } finally await(pool.preparedQuery(update).execute(Tuple.of(java.lang.Boolean.FALSE)))
  }

  "findActivePlan" should "return the plan of the ACTIVE subscription only" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("active@b.com", None)))

    await(storage.findActivePlan(pool, customer.id)) shouldBe None

    await(pool.withTransaction(tx => storage.insertActiveSubscription(tx, customer.id, CreateSubscription(PlanType.FREE_TIER))))
    await(storage.findActivePlan(pool, customer.id)) shouldBe Some(PlanType.FREE_TIER)

    await(pool.withTransaction(tx => storage.cancelActiveSubscription(tx, customer.id)))
    await(storage.findActivePlan(pool, customer.id)) shouldBe None
  }

  private val now = Instant.parse("2026-03-15T10:15:30Z")

  private def customerWithUsage(pool: Pool, storage: Storage, email: String, consumed: Long, resetDate: Instant) = {
    val customer = await(storage.insertCustomer(CreateCustomer(email, None)))
    await(
      pool
        .preparedQuery("INSERT INTO entitlements (customer_id, feature_key, allocated_quota, consumed_quota, reset_date) VALUES ($1, 'CALLS', 1000, $2, $3)")
        .execute(Tuple.of(customer.id, Long.box(consumed), resetDate.atOffset(ZoneOffset.UTC)))
    )
    customer
  }

  private def entitlement(storage: Storage, customerId: java.util.UUID) = await(storage.listEntitlements(customerId)).head

  "consume" should "add to the usage of the current period" in withStorage { (pool, storage) =>
    val resetDate = Instant.parse("2026-04-01T00:00:00Z")
    val customer  = customerWithUsage(pool, storage, "current@b.com", 100, resetDate)

    val result = await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 50), now)))

    result.map(_.consumedQuota) shouldBe Some(150L)
    result.map(_.remainingQuota) shouldBe Some(850L)
    entitlement(storage, customer.id).resetDate shouldBe Some(resetDate)
  }

  it should "start a new period once reset_date has passed" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "rollover@b.com", 1000, Instant.parse("2026-02-01T00:00:00Z"))

    val result = await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 50), now)))

    result.map(_.consumedQuota) shouldBe Some(50L)
    entitlement(storage, customer.id).resetDate shouldBe Some(Instant.parse("2026-04-01T00:00:00Z"))
  }

  it should "start a new period exactly at reset_date" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "boundary@b.com", 1000, now)

    val result = await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 1), now)))

    result.map(_.consumedQuota) shouldBe Some(1L)
  }

  it should "refuse an amount above the remaining quota" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "exceeded@b.com", 1000, Instant.parse("2026-04-01T00:00:00Z"))

    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 1), now))) shouldBe None
    entitlement(storage, customer.id).consumedQuota shouldBe 1000L
  }

  it should "refuse an amount above the allocated quota in a new period" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "tooMuch@b.com", 10, Instant.parse("2026-02-01T00:00:00Z"))

    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 1001), now))) shouldBe None
  }

  it should "refuse an amount that would overflow a bigint" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "overflow@b.com", 100, Instant.parse("2026-04-01T00:00:00Z"))

    await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", Long.MaxValue), now))) shouldBe None
  }

  it should "never reset an entitlement without a reset date" in withStorage { (pool, storage) =>
    val customer = customerWithUsage(pool, storage, "noreset@b.com", 400, now)
    await(pool.preparedQuery("UPDATE entitlements SET reset_date = NULL WHERE customer_id = $1").execute(Tuple.of(customer.id)))

    val result = await(pool.withTransaction(tx => storage.consume(tx, customer.id, Consumption("CALLS", 100), now)))

    result.map(_.consumedQuota) shouldBe Some(500L)
    entitlement(storage, customer.id).resetDate shouldBe None
  }

  "Entitlement.effective" should "keep the values until reset_date" in {
    val entitlement = Entitlement("CALLS", 1000, 400, Some(now.plusSeconds(1)))

    entitlement.effective(now) shouldBe entitlement
  }

  it should "report a new period once reset_date has passed" in {
    Entitlement("CALLS", 1000, 400, Some(now)).effective(now) shouldBe Entitlement("CALLS", 1000, 0, Some(Instant.parse("2026-04-01T00:00:00Z")))
  }

  it should "keep the usage of an entitlement that never resets" in {
    val entitlement = Entitlement("STORAGE_GB", 50, 40, None)

    entitlement.effective(now) shouldBe entitlement
  }
}
