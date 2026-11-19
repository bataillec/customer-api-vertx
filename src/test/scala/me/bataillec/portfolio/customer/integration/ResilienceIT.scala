package me.bataillec.portfolio.customer.integration

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.support.{ApiResponse, AppFixture, AuditProbe, Containers}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

/** What the app does when a dependency stops answering. Postgres and Kafka are paused, not stopped: the clients then
  * see a service that hangs, the harder case, and the mapped ports survive.
  *
  * The suite has containers of its own, so that pausing them can never disturb the other suites.
  */
class ResilienceIT extends AnyWordSpec with Matchers with AppFixture {
  override protected lazy val containers: Containers = new Containers

  private def checks(ready: ApiResponse): Map[String, String] =
    ready.json.getJsonArray("checks").asScala.map(_.asInstanceOf[JsonObject]).map(c => c.getString("id") -> c.getString("status")).toMap

  /** Polls /health/ready until it answers `status`, and returns that answer with the time it took. */
  private def awaitReady(status: Int, timeoutSeconds: Int = 90): (ApiResponse, Long) = {
    val start    = System.nanoTime()
    val deadline = start + timeoutSeconds * 1_000_000_000L
    var last     = api.get("/health/ready")
    while (last.status != status && System.nanoTime() < deadline) {
      Thread.sleep(500)
      last = api.get("/health/ready")
    }
    withClue(s"/health/ready never answered $status within $timeoutSeconds s, last: ${last.status} ${last.body}: ") {
      last.status shouldBe status
    }
    (last, (System.nanoTime() - start) / 1_000_000)
  }

  private def timed[T](f: => T): (T, Long) = {
    val start  = System.nanoTime()
    val result = f
    (result, (System.nanoTime() - start) / 1_000_000)
  }

  "with Kafka unreachable" should {
    "keep serving the API, since the audit is fire-and-forget" in {
      containers.pauseKafka()
      try {
        val (created, millis) = timed(api.createCustomer())
        created.status shouldBe 201
        millis should be < 5000L

        val id = created.string("id")
        api.subscribe(id, "FREE_TIER").status shouldBe 201
        api.consume(id, "API_CALLS_MONTHLY", 1).status shouldBe 200
        api.get("/health/live").status shouldBe 200
      } finally containers.unpauseKafka()
    }

    "report the audit as DOWN on /health/ready, then recover" in {
      awaitReady(200)
      containers.pauseKafka()
      try {
        val (ready, _) = awaitReady(503, timeoutSeconds = 30)
        checks(ready)("audit-verticle") shouldBe "DOWN"
        checks(ready)("customer-verticle") shouldBe "UP"
        api.get("/health/live").status shouldBe 200
      } finally containers.unpauseKafka()

      val (ready, _) = awaitReady(200)
      checks(ready) shouldBe Map("customer-verticle" -> "UP", "audit-verticle" -> "UP")
    }

    "deliver the new events once Kafka is back" in {
      val probe = new AuditProbe(containers.kafkaBootstrap, auditTopic)
      try {
        val id = api.createCustomer().string("id")

        probe.awaitEvents(id, 1, java.time.Duration.ofSeconds(60)).map(_.eventType) shouldBe List("CUSTOMER_CREATED")
      } finally probe.close()
    }
  }

  "with Postgres unreachable" should {
    "report the customer side as DOWN on /health/ready while staying alive, then recover" in {
      awaitReady(200)
      containers.pausePostgres()
      try {
        val (ready, millis) = awaitReady(503, timeoutSeconds = 30)
        checks(ready)("customer-verticle") shouldBe "DOWN"
        checks(ready)("audit-verticle") shouldBe "UP"
        millis should be < 15000L // the 4 s ping and 5 s check timeouts, not the 30 s of the requests
        api.get("/health/live").status shouldBe 200
      } finally containers.unpausePostgres()

      awaitReady(200)
    }

    "answer 504 instead of hanging forever, as a problem document" in {
      containers.pausePostgres()
      try {
        // The requests give up after 30 s on the event bus: wait longer than that
        val (response, millis) = timed(
          api.send(HttpMethod.POST, "/customers", Some(JsonObject().put("email", "hang@example.com").encode()), timeoutSeconds = 60)
        )

        response.status shouldBe 504
        response.header("content-type").map(_.takeWhile(_ != ';')) shouldBe Some("application/problem+json")
        response.code shouldBe "TIMEOUT_COMMUNICATION"
        millis should be < 45000L
      } finally containers.unpausePostgres()
    }

    "work again once Postgres is back, with the earlier data intact" in {
      awaitReady(200)
      val before = api.createCustomer("survivor@example.com", Some("Acme"))
      before.status shouldBe 201

      containers.pausePostgres()
      containers.unpausePostgres()

      awaitReady(200)
      val fetched = api.get(s"/customers/${before.string("id")}")
      fetched.status shouldBe 200
      fetched.string("company_name") shouldBe "Acme"
      api.createCustomer().status shouldBe 201
      api.get("/plans").status shouldBe 200
    }
  }
}
