package me.bataillec.portfolio.customer.unit

import io.vertx.core.Vertx
import me.bataillec.portfolio.customer.AuditVerticle
import me.bataillec.portfolio.customer.Constants.AUDIT_HEALTH_ADDRESS
import me.bataillec.portfolio.customer.support.FutureUtils.await
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.compiletime.uninitialized

class AuditVerticleSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {
  private var vertx: Vertx = uninitialized

  override def beforeEach(): Unit = vertx = Vertx.vertx()

  override def afterEach(): Unit = await(vertx.close())

  "start" should {
    "register the audit consumers, without connecting to Kafka" in {
      await(vertx.deployVerticle(new AuditVerticle))

      await(vertx.eventBus().request[String](AUDIT_HEALTH_ADDRESS, "ping")).body() shouldBe "pong"
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
