package me.bataillec.portfolio.customer.http

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.{Pool, PoolOptions}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.{AuditSender, CustomerHandler, CustomerSender, SenderUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

class ReadinessSpec extends AnyFlatSpec with Matchers with TestContainerForAll {
  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def("postgres:17-alpine")

  private val vertx = Vertx.vertx()

  private def await[T](future: io.vertx.core.Future[T]): T = future.toCompletionStage.toCompletableFuture.get(10, TimeUnit.SECONDS)

  private def buildPool(pg: PostgreSQLContainer): Pool = PgBuilder
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

  private def checks(body: JsonObject): Map[String, JsonObject] =
    body.getJsonArray("checks").asScala.map { case check: JsonObject => check.getString("id") -> check }.toMap

  "GET /health/ready" should "follow the state of Postgres" in withContainers { pg =>
    val pool = buildPool(pg)
    await(new CustomerHandler(vertx, pool, new AuditSender(vertx)).handle())
    await(SenderUtils.pong(vertx, AUDIT_HEALTH_ADDRESS).completion())
    val server: HttpServer = await(ApiRoutes.build(vertx, new CustomerSender(vertx.eventBus())).compose(router => vertx.createHttpServer().requestHandler(router).listen(0)))
    val client = WebClient.create(vertx)

    def ready() = await(client.get(server.actualPort(), "localhost", "/health/ready").send())

    ready().statusCode() shouldBe 200

    await(pool.close())

    val response = ready()
    response.statusCode() shouldBe 503
    val result = checks(response.bodyAsJsonObject())
    result(CUSTOMER_VERTICLE).getString("status") shouldBe "DOWN"
    result(CUSTOMER_VERTICLE).getJsonObject("data").getString("error") shouldBe "Postgres unreachable"
    result(AUDIT_VERTICLE).getString("status") shouldBe "UP"
  }

  it should "report the audit verticle independently of Postgres" in withContainers { pg =>
    val other = Vertx.vertx()
    try {
      val pool = buildPool(pg)
      await(new CustomerHandler(other, pool, new AuditSender(other)).handle())
      val server = await(ApiRoutes.build(other, new CustomerSender(other.eventBus())).compose(router => other.createHttpServer().requestHandler(router).listen(0)))

      val response = await(WebClient.create(other).get(server.actualPort(), "localhost", "/health/ready").send())

      response.statusCode() shouldBe 503
      val result = checks(response.bodyAsJsonObject())
      result(CUSTOMER_VERTICLE).getString("status") shouldBe "UP"
      result(AUDIT_VERTICLE).getString("status") shouldBe "DOWN"
    } finally await(other.close())
  }
}
