package me.bataillec.portfolio.customer.customer

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.vertx.core.Vertx
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.{Pool, PoolOptions}
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.eventbus.AuditSender
import org.flywaydb.core.Flyway
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Try

class CustomerOperationsSpec extends AnyFlatSpec with Matchers with TestContainerForAll {
  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def("postgres:17-alpine")

  private val vertx = Vertx.vertx()

  private def await[T](future: io.vertx.core.Future[T]): T = future.toCompletionStage.toCompletableFuture.get(10, TimeUnit.SECONDS)

  private def failure[T](future: => io.vertx.core.Future[T]): Throwable = Try(await(future)).failed.get.getCause

  private def withOperations[A](f: CustomerOperations => A): A = withContainers { pg =>
    Flyway.configure().dataSource(pg.jdbcUrl, pg.username, pg.password).load().migrate()
    val pool: Pool = PgBuilder
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
    try f(new CustomerOperations(pool, new AuditSender(vertx)))
    finally await(pool.close())
  }

  private val apiCalls = "API_CALLS_MONTHLY"

  private def subscribedCustomer(operations: CustomerOperations, email: String): UUID = {
    val customer = await(operations.createCustomer(CreateCustomer(email, None)))
    await(operations.createSubscription(customer.id, CreateSubscription(PlanType.FREE_TIER)))
    customer.id
  }

  private def setStatus(operations: CustomerOperations, id: UUID, status: CustomerStatus) =
    await(operations.updateCustomer(id, UpdateCustomer(None, Some(status))))

  private def usage(operations: CustomerOperations, id: UUID) =
    await(operations.listEntitlements(id)).find(_.featureKey == apiCalls).get.consumedQuota

  "updateCustomer" should "cancel the subscription and keep the usage of a deleted customer" in withOperations { operations =>
    val id = subscribedCustomer(operations, "deleted@b.com")
    await(operations.consumeEntitlement(id, Consumption(apiCalls, 400)))

    setStatus(operations, id, CustomerStatus.DELETED).status shouldBe CustomerStatus.DELETED

    await(operations.listSubscriptions(id)).map(_.status) shouldBe List(SubscriptionStatus.CANCELED)
    usage(operations, id) shouldBe 400L
  }

  it should "keep the subscription when the customer is suspended" in withOperations { operations =>
    val id = subscribedCustomer(operations, "suspended@b.com")

    setStatus(operations, id, CustomerStatus.SUSPENDED)

    await(operations.listSubscriptions(id)).map(_.status) shouldBe List(SubscriptionStatus.ACTIVE)
  }

  it should "reject a company name change while the customer stays deleted" in withOperations { operations =>
    val id = subscribedCustomer(operations, "rename@b.com")
    setStatus(operations, id, CustomerStatus.DELETED)

    failure(operations.updateCustomer(id, UpdateCustomer(Some("Acme"), None))) shouldBe DomainError.CustomerNotActive(id, CustomerStatus.DELETED)
    failure(operations.updateCustomer(id, UpdateCustomer(Some("Acme"), Some(CustomerStatus.DELETED)))) shouldBe
      DomainError.CustomerNotActive(id, CustomerStatus.DELETED)
    await(operations.getCustomer(id)).companyName shouldBe None
  }

  it should "accept a company name change that comes with a reactivation" in withOperations { operations =>
    val id = subscribedCustomer(operations, "back@b.com")
    setStatus(operations, id, CustomerStatus.DELETED)

    val customer = await(operations.updateCustomer(id, UpdateCustomer(Some("Acme"), Some(CustomerStatus.ACTIVE))))

    customer.status shouldBe CustomerStatus.ACTIVE
    customer.companyName shouldBe Some("Acme")
  }

  it should "allow a deleted customer to become suspended" in withOperations { operations =>
    val id = subscribedCustomer(operations, "toSuspended@b.com")
    setStatus(operations, id, CustomerStatus.DELETED)

    setStatus(operations, id, CustomerStatus.SUSPENDED).status shouldBe CustomerStatus.SUSPENDED
  }

  "A reactivated customer" should "not consume before subscribing again" in withOperations { operations =>
    val id = subscribedCustomer(operations, "noSubscription@b.com")
    setStatus(operations, id, CustomerStatus.DELETED)
    setStatus(operations, id, CustomerStatus.ACTIVE)

    failure(operations.consumeEntitlement(id, Consumption(apiCalls, 1))) shouldBe DomainError.NoActiveSubscription(id)
  }

  it should "find its usage back when subscribing again" in withOperations { operations =>
    val id = subscribedCustomer(operations, "resubscribed@b.com")
    await(operations.consumeEntitlement(id, Consumption(apiCalls, 1_000)))
    setStatus(operations, id, CustomerStatus.DELETED)
    setStatus(operations, id, CustomerStatus.ACTIVE)

    await(operations.createSubscription(id, CreateSubscription(PlanType.FREE_TIER)))

    usage(operations, id) shouldBe 1_000L
    failure(operations.consumeEntitlement(id, Consumption(apiCalls, 1))) shouldBe a[DomainError.QuotaExceeded]
  }

  "consumeEntitlement" should "report a missing entitlement for a customer that never subscribed" in withOperations { operations =>
    val id = await(operations.createCustomer(CreateCustomer("never@b.com", None))).id

    failure(operations.consumeEntitlement(id, Consumption(apiCalls, 1))) shouldBe DomainError.EntitlementNotFound(id, apiCalls)
  }
}
