package me.bataillec.portfolio.customer.unit

import io.vertx.core.Vertx
import io.vertx.core.eventbus.{DeliveryOptions, ReplyException, ReplyFailure}
import me.bataillec.portfolio.customer.AuditVerticle
import me.bataillec.portfolio.customer.Constants.AUDIT_HEALTH_ADDRESS
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.compiletime.uninitialized

class AuditVerticleSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {
  private var vertx: Vertx = uninitialized

  // Postgres is unreachable (port 1): the relay must not find a database of the machine and publish its events
  override def beforeEach(): Unit = {
    EnvironmentUtils.setEnv("PG_PORT", "1")
    vertx = Vertx.vertx()
  }

  override def afterEach(): Unit = {
    try await(vertx.close())
    finally EnvironmentUtils.unsetEnv("PG_PORT")
  }

  "start" should {
    "register the health consumer and start relaying, without connecting to Kafka or Postgres" in {
      await(vertx.deployVerticle(new AuditVerticle))

      // The health check now asks Kafka for the topic metadata, and there is no broker in a unit test:
      // the consumer is registered (not NO_HANDLERS) but cannot answer "pong" while the broker is unreachable
      val failure = failureOf(vertx.eventBus().request[String](AUDIT_HEALTH_ADDRESS, "ping", new DeliveryOptions().setSendTimeout(1000)))

      failure shouldBe a[ReplyException]
      failure.asInstanceOf[ReplyException].failureType() shouldBe ReplyFailure.TIMEOUT
    }
  }

  "stop" should {
    "close the audit writer when the verticle was started" in {
      val deploymentId = await(vertx.deployVerticle(new AuditVerticle))

      await(vertx.undeploy(deploymentId))

      vertx.deploymentIDs() should not contain deploymentId
    }

    "do nothing when the verticle was never started" in {
      await(new AuditVerticle().stop())
    }
  }
}
