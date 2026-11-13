package me.bataillec.portfolio.customer.unit

import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import me.bataillec.portfolio.customer.Constants.CUSTOMER_HEALTH_ADDRESS
import me.bataillec.portfolio.customer.CustomerVerticle
import me.bataillec.portfolio.customer.config.CustomerConfig
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.flywaydb.core.api.output.MigrateResult
import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** The migrations are replaced by a stub, except in the test of the real `runMigrations`. The pool never connects: Postgres is unreachable (port 1). */
class CustomerVerticleSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {
  private var vertx: Vertx = _

  override def beforeEach(): Unit = {
    vertx = Vertx.vertx()
    EnvironmentUtils.setEnv("PG_PORT", "1")
  }

  override def afterEach(): Unit = {
    await(vertx.close())
    EnvironmentUtils.unsetEnv("PG_PORT")
  }

  private class StubbedMigrations(outcome: => MigrateResult) extends CustomerVerticle {
    var configs: List[CustomerConfig]                             = Nil
    override protected def runMigrations(conf: CustomerConfig): MigrateResult = {
      configs = conf :: configs
      outcome
    }
  }

  "start" should {
    "migrate, then register the customer consumers" in {
      val verticle = new StubbedMigrations(mock(classOf[MigrateResult]))

      await(vertx.deployVerticle(verticle))

      verticle.configs.map(_.pgPort) shouldBe List(1)
      // Registered: Postgres is unreachable, so the health consumer answers 503 instead of failing with NO_HANDLERS.
      val failure = failureOf(vertx.eventBus().request[String](CUSTOMER_HEALTH_ADDRESS, "ping"))
      failure shouldBe a[ReplyException]
      failure.asInstanceOf[ReplyException].failureCode() shouldBe 503
    }

    "fail when the migration fails" in {
      val error    = new RuntimeException("migration failed")
      val verticle = new StubbedMigrations(throw error)

      failureOf(vertx.deployVerticle(verticle)) shouldBe error

      val failure = failureOf(vertx.eventBus().request[String](CUSTOMER_HEALTH_ADDRESS, "ping"))
      failure.asInstanceOf[ReplyException].failureCode() shouldBe -1
    }

    "run the real Flyway migration, which fails when Postgres is unreachable" in {
      failureOf(vertx.deployVerticle(new CustomerVerticle), 30) should not be null
    }
  }

  "stop" should {
    "close the database pool when the verticle was started" in {
      val deploymentId = await(vertx.deployVerticle(new StubbedMigrations(mock(classOf[MigrateResult]))))

      await(vertx.undeploy(deploymentId))

      vertx.deploymentIDs() should not contain deploymentId
    }

    "do nothing when the verticle was never started" in {
      await(new CustomerVerticle().stop())
    }
  }
}
