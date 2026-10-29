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

import java.time.Instant
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
    await(storage.findPlanQuotas(pool, PlanType.FREE_TIER)) shouldBe Map("API_CALLS_MONTHLY" -> 1_000L, "STORAGE_GB" -> 1L)
    await(storage.findPlanQuotas(pool, PlanType.PRO)) shouldBe Map("API_CALLS_MONTHLY" -> 100_000L, "STORAGE_GB" -> 50L)
    await(storage.findPlanQuotas(pool, PlanType.ENTERPRISE)) shouldBe Map("API_CALLS_MONTHLY" -> 10_000_000L, "STORAGE_GB" -> 1_000L)
  }

  "listPlans" should "group the quotas by plan" in withStorage { (_, storage) =>
    val plans = await(storage.listPlans())

    plans.map(_.planType) shouldBe List(PlanType.FREE_TIER, PlanType.PRO, PlanType.ENTERPRISE)
    plans.find(_.planType == PlanType.PRO).get.quotas shouldBe Map("API_CALLS_MONTHLY" -> 100_000L, "STORAGE_GB" -> 50L)
  }

  "resetForPlan" should "use the quotas stored in the database" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("a@b.com", None)))
    await(pool.preparedQuery("UPDATE plan_quotas SET quota = 42 WHERE plan_type = 'PRO' AND feature_key = 'STORAGE_GB'").execute())

    val entitlements = await(pool.withTransaction(tx => storage.resetForPlan(tx, customer.id, CreateSubscription(PlanType.PRO), Instant.now())))

    entitlements.map(e => e.featureKey -> e.allocatedQuota).toMap shouldBe Map("API_CALLS_MONTHLY" -> 100_000L, "STORAGE_GB" -> 42L)
  }

  it should "fail with PlanNotConfigured when the plan has no quota" in withStorage { (pool, storage) =>
    val customer = await(storage.insertCustomer(CreateCustomer("c@d.com", None)))
    await(pool.preparedQuery("DELETE FROM plan_quotas WHERE plan_type = 'PRO'").execute())

    val result = Try(await(pool.withTransaction(tx => storage.resetForPlan(tx, customer.id, CreateSubscription(PlanType.PRO), Instant.now()))))

    result.failed.get.getCause shouldBe DomainError.PlanNotConfigured(PlanType.PRO)
  }
}
