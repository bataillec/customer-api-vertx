package me.bataillec.portfolio.customer.support

import me.bataillec.portfolio.customer.{AuditVerticle, CustomerVerticle, HttpServerVerticle}
import me.bataillec.portfolio.customer.support.FutureUtils.await
import org.scalatest.Suite

import java.net.ServerSocket
import java.util.UUID
import scala.compiletime.uninitialized

/** The whole app in the JVM: one non-clustered Vert.x deploying the three verticles on the Postgres and Kafka
  * containers, with the HTTP server on a free port. The configuration goes through the environment, as in production.
  */
trait AppFixture extends PostgresFixture { this: Suite =>
  protected var api: ApiClient   = uninitialized
  protected var auditTopic: String = uninitialized

  private val envKeys = List("PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD", "KAFKA_BOOTSTRAP_SERVERS", "AUDIT_TOPIC", "HTTP_PORT")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val port = freePort()
    auditTopic = s"customer.events.${UUID.randomUUID()}"
    Map(
      "PG_HOST"                 -> ContainersSupport.pgHost,
      "PG_PORT"                 -> ContainersSupport.pgPort.toString,
      "PG_DATABASE"             -> database,
      "PG_USER"                 -> ContainersSupport.pgUser,
      "PG_PASSWORD"             -> ContainersSupport.pgPassword,
      "KAFKA_BOOTSTRAP_SERVERS" -> ContainersSupport.kafkaBootstrap,
      "AUDIT_TOPIC"             -> auditTopic,
      "HTTP_PORT"               -> port.toString
    ).foreach((k, v) => EnvironmentUtils.setEnv(k, v))

    await(vertx.deployVerticle(new CustomerVerticle), 60)
    await(vertx.deployVerticle(new AuditVerticle), 60)
    await(vertx.deployVerticle(new HttpServerVerticle), 60)

    api = new ApiClient(vertx, port)
    awaitReady()
  }

  override protected def afterAll(): Unit = {
    try if (api != null) api.close()
    finally {
      try super.afterAll() // closes Vert.x, hence the verticles
      finally envKeys.foreach(EnvironmentUtils.unsetEnv)
    }
  }

  private def awaitReady(): Unit = {
    val deadline = System.nanoTime() + 60_000_000_000L
    var ready    = false
    while (!ready && System.nanoTime() < deadline) {
      ready = try api.get("/health/ready").status == 200
      catch { case _: Exception => false }
      if (!ready) Thread.sleep(500)
    }
    if (!ready) throw new IllegalStateException("The app is not ready after 60 s: " + api.get("/health/ready").body)
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}
