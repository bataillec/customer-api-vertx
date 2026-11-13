package me.bataillec.portfolio.customer.unit

import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import me.bataillec.portfolio.customer.HttpServerVerticle
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.ServerSocket
import scala.compiletime.uninitialized

class HttpServerVerticleSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {
  private var vertx: Vertx = uninitialized

  override def beforeEach(): Unit = vertx = Vertx.vertx()

  override def afterEach(): Unit = {
    await(vertx.close())
    EnvironmentUtils.unsetEnv("HTTP_PORT")
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  "start" should {
    "serve the API on the configured port" in {
      val port = freePort()
      EnvironmentUtils.setEnv("HTTP_PORT", port.toString)

      await(vertx.deployVerticle(new HttpServerVerticle))

      val client   = WebClient.create(vertx)
      val response = await(client.get(port, "localhost", "/openapi.json").send())
      response.statusCode() shouldBe 200
    }

    "fail to deploy when the port is already used" in {
      val busy = vertx.createHttpServer().requestHandler(_.response().end()).listen(0)
      val port = await(busy).actualPort()
      EnvironmentUtils.setEnv("HTTP_PORT", port.toString)

      failureOf(vertx.deployVerticle(new HttpServerVerticle)) should not be null
    }
  }

  "undeploy" should {
    "stop listening" in {
      val port = freePort()
      EnvironmentUtils.setEnv("HTTP_PORT", port.toString)
      val deploymentId = await(vertx.deployVerticle(new HttpServerVerticle))

      await(vertx.undeploy(deploymentId))

      val client = WebClient.create(vertx)
      failureOf(client.get(port, "localhost", "/openapi.json").send()) should not be null
    }
  }
}
