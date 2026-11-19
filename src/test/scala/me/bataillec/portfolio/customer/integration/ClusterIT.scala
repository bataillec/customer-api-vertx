package me.bataillec.portfolio.customer.integration

import io.vertx.core.{Vertx, VerticleBase}
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import me.bataillec.portfolio.customer.Constants.CUSTOMER_ADDRESS
import me.bataillec.portfolio.customer.support.FutureUtils.await
import me.bataillec.portfolio.customer.support.{AppFixture, AuditProbe}
import me.bataillec.portfolio.customer.{AuditVerticle, CustomerVerticle, HttpServerVerticle}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}
import scala.compiletime.uninitialized

/** The app as three clustered Vert.x nodes (Hazelcast, with the `cluster.xml` of the jar): HTTP and audit on one,
  * a CustomerVerticle on each of the two others. It checks the load spreading and the failover that the README
  * describes. The nodes join through the fixed ports 5701 to 5703 of `cluster.xml`, so these must be free.
  *
  * The tests run in order and the last ones stop nodes for good.
  */
class ClusterIT extends AnyWordSpec with Matchers with AppFixture {

  /** A clustered Vert.x node that counts the messages delivered to the `customer` address it hosts. */
  private final class Node(val vertx: Vertx) {
    val handled        = new AtomicInteger(0)
    private var closed = false

    vertx.eventBus().addInboundInterceptor[Object] { delivery =>
      if (delivery.message().address() == CUSTOMER_ADDRESS) handled.incrementAndGet()
      delivery.next()
    }

    def deploy(verticle: VerticleBase): Unit = {
      await(vertx.deployVerticle(verticle), 90)
      ()
    }

    def stop(): Unit = synchronized {
      if (!closed) {
        closed = true
        await(vertx.close(), 60)
      }
    }
  }

  private var edge: Node      = uninitialized
  private var customer1: Node = uninitialized
  private var customer2: Node = uninitialized

  private def startNode(): Node =
    new Node(await(Vertx.builder().withClusterManager(new HazelcastClusterManager()).buildClustered(), 90))

  override protected def startApp(port: Int): Unit = {
    edge = startNode()
    edge.deploy(new AuditVerticle)
    edge.deploy(new HttpServerVerticle)
    customer1 = startNode()
    customer1.deploy(new CustomerVerticle)
    customer2 = startNode()
    customer2.deploy(new CustomerVerticle)
  }

  override protected def stopApp(): Unit =
    // The edge goes last, since the others only need the cluster to leave it
    List(customer2, customer1, edge).filter(_ != null).foreach(_.stop())

  private def newCustomerId(): String = {
    val created = api.createCustomer()
    created.status shouldBe 201
    created.string("id")
  }

  "the cluster" should {
    "report the app ready once the three nodes have joined" in {
      val ready = api.get("/health/ready")

      ready.status shouldBe 200
      ready.json.getString("status") shouldBe "UP"
    }

    "spread the requests over the customer verticles" in {
      val before = (customer1.handled.get, customer2.handled.get)

      (1 to 40).foreach(_ => api.get("/plans").status shouldBe 200)

      val served1 = customer1.handled.get - before._1
      val served2 = customer2.handled.get - before._2
      served1 + served2 shouldBe 40
      served1 should be >= 10
      served2 should be >= 10
    }

    "keep a quota exact when both customer verticles write at the same time" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.consume(id, "API_CALLS_MONTHLY", 970).status shouldBe 200 // 30 left
      val before = (customer1.handled.get, customer2.handled.get)

      val pool = Executors.newFixedThreadPool(16)
      val statuses =
        try (1 to 60).map(_ => pool.submit(() => api.consume(id, "API_CALLS_MONTHLY", 1).status)).map(_.get(90, TimeUnit.SECONDS).intValue()).toList
        finally pool.shutdown()

      statuses.count(_ == 200) shouldBe 30
      statuses.count(_ == 429) shouldBe 30
      api.get(s"/customers/$id/entitlements").jsonArray.getJsonObject(0).getLong("consumed_quota") shouldBe 1000L
      // Both nodes really took part, otherwise this would not test the cluster
      (customer1.handled.get - before._1) should be > 0
      (customer2.handled.get - before._2) should be > 0
    }

    "deliver the audit events from the customer nodes to the audit node" in {
      val probe = new AuditProbe(containers.kafkaBootstrap, auditTopic)
      try {
        val id = newCustomerId()

        probe.awaitEvents(id, 1).map(_.eventType) shouldBe List("CUSTOMER_CREATED")
      } finally probe.close()
    }

    "keep serving from the remaining customer verticle when one node leaves" in {
      customer1.stop()
      val before = customer2.handled.get

      (1 to 20).foreach(_ => api.get("/plans").status shouldBe 200)
      newCustomerId() should not be empty

      customer2.handled.get - before shouldBe 21
    }

    "answer 503 NO_HANDLERS and report not ready when no customer verticle is left" in {
      customer2.stop()

      val response = api.get("/plans")

      response.status shouldBe 503
      response.code shouldBe "NO_HANDLERS"
      response.header("content-type").map(_.takeWhile(_ != ';')) shouldBe Some("application/problem+json")
      val ready = api.get("/health/ready")
      ready.status shouldBe 503
      ready.json.getJsonArray("checks").getJsonObject(0).getString("status") shouldBe "DOWN"
      api.get("/health/live").status shouldBe 200
    }
  }

}
