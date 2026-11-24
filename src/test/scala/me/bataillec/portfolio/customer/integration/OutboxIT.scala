package me.bataillec.portfolio.customer.integration

import io.vertx.core.Future
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.PoolOptions
import me.bataillec.portfolio.customer.audit.{Backlog, OutboxRecorder, PGOutboxStore}
import me.bataillec.portfolio.customer.customer.{CustomerOperations, PGStorage}
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import me.bataillec.portfolio.customer.support.PostgresFixture
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.compiletime.uninitialized

/** The outbox on a real Postgres: what is written with a change, what rolls back with it, and what the relay reads. */
class OutboxIT extends AnyWordSpec with Matchers with PostgresFixture {
  private var storage: PGStorage        = uninitialized
  private var ops: CustomerOperations   = uninitialized
  private val stores                    = ListBuffer.empty[PGOutboxStore]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    storage = new PGStorage(pool)
    ops = new CustomerOperations(storage, new OutboxRecorder(storage))
  }

  override protected def afterEach(): Unit = {
    try stores.foreach(s => await(s.close()))
    finally {
      stores.clear()
      super.afterEach()
    }
  }

  private def newStore(): PGOutboxStore = {
    val options = PgConnectOptions()
      .setHost(containers.pgHost)
      .setPort(containers.pgPort)
      .setDatabase(database)
      .setUser(containers.pgUser)
      .setPassword(containers.pgPassword)
    val store = new PGOutboxStore(vertx, options, PgBuilder.pool().`with`(PoolOptions().setMaxSize(2)).connectingTo(options).using(vertx).build())
    stores += store
    store
  }

  private def query(sql: String): List[String] = {
    val conn = containers.connection(database)
    try {
      val rs  = conn.createStatement().executeQuery(sql)
      val out = ListBuffer.empty[String]
      while (rs.next()) out += rs.getString(1)
      out.toList
    } finally conn.close()
  }

  private def eventTypes(): List[String] = query("SELECT payload->>'event_type' FROM outbox ORDER BY id")

  private def newCustomer(): Customer = await(ops.createCustomer(CreateCustomer(s"${UUID.randomUUID()}@example.com", None)))

  "a change" should {
    "commit with its event" in {
      val customer = newCustomer()

      eventTypes() shouldBe List("CUSTOMER_CREATED")
      query("SELECT customer_id::text FROM outbox") shouldBe List(customer.id.toString)
      query("SELECT (sent_at IS NULL)::text FROM outbox") shouldBe List("true")
    }

    "roll back with its event when the transaction fails after the event was recorded" in {
      val outbox  = new OutboxRecorder(storage)
      val failure = new RuntimeException("commit never happens")

      val result = storage.inTransaction { tx =>
        storage
          .insertCustomer(tx, CreateCustomer("rolled@example.com", None))
          .compose(c => outbox.record(tx, "CUSTOMER_CREATED", c.id))
          .compose(_ => Future.failedFuture[Unit](failure))
      }

      failureOf(result) shouldBe failure
      query("SELECT count(*) FROM customers WHERE email = 'rolled@example.com'") shouldBe List("0")
      query("SELECT count(*) FROM outbox") shouldBe List("0")
    }

    "leave no event when the customer cannot be created" in {
      await(ops.createCustomer(CreateCustomer("dup@example.com", None)))

      failureOf(ops.createCustomer(CreateCustomer("dup@example.com", None))) shouldBe EmailAlreadyUsed("dup@example.com")

      eventTypes() shouldBe List("CUSTOMER_CREATED")
    }

    "leave no event when it is refused for another reason than the quota" in {
      val customer = newCustomer()

      failureOf(ops.consumeEntitlement(customer.id, Consumption("STORAGE_GB", 1))) shouldBe a[DomainError]

      eventTypes() shouldBe List("CUSTOMER_CREATED")
    }

    "keep the QUOTA_EXCEEDED event of a refused consume, although the call itself fails" in {
      val customer = newCustomer()
      await(ops.createSubscription(customer.id, CreateSubscription(PlanType.FREE_TIER)))
      await(ops.consumeEntitlement(customer.id, Consumption("STORAGE_GB", 1)))

      failureOf(ops.consumeEntitlement(customer.id, Consumption("STORAGE_GB", 1))) shouldBe a[QuotaExceeded]

      eventTypes() shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "QUOTA_CONSUMED", "QUOTA_EXCEEDED")
    }

    "write a QUOTA_CONSUMED event with the consumption" in {
      val customer = newCustomer()
      await(ops.createSubscription(customer.id, CreateSubscription(PlanType.FREE_TIER)))

      await(ops.consumeEntitlement(customer.id, Consumption("API_CALLS_MONTHLY", 10)))
      await(ops.consumeEntitlement(customer.id, Consumption("API_CALLS_MONTHLY", 5)))

      eventTypes() shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "QUOTA_CONSUMED", "QUOTA_CONSUMED")
      query("SELECT payload->>'amount' || '/' || (payload->>'consumed_quota') FROM outbox WHERE payload->>'event_type' = 'QUOTA_CONSUMED' ORDER BY id") shouldBe
        List("10/10", "5/15")
      query("SELECT jsonb_typeof(payload->'amount') FROM outbox WHERE payload->>'event_type' = 'QUOTA_CONSUMED' LIMIT 1") shouldBe List("number")
    }

    "number the events of a customer in the order the changes were made" in {
      val customer = newCustomer()
      await(ops.createSubscription(customer.id, CreateSubscription(PlanType.FREE_TIER)))
      await(ops.updateCustomer(customer.id, UpdateCustomer(Some("Acme"), None)))
      await(ops.createSubscription(customer.id, CreateSubscription(PlanType.PRO)))

      eventTypes() shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "CUSTOMER_UPDATED", "SUBSCRIPTION_CHANGED")
    }
  }

  "the relay store" should {
    "let only one instance lead, until it steps down" in {
      val (first, second) = (newStore(), newStore())

      await(first.tryLead()) shouldBe true
      await(second.tryLead()) shouldBe false
      await(first.tryLead()) shouldBe true // it keeps it

      await(first.stepDown())

      await(second.tryLead()) shouldBe true
      await(first.tryLead()) shouldBe false
    }

    "hand the leadership over when the leader is closed" in {
      val (first, second) = (newStore(), newStore())
      await(first.tryLead()) shouldBe true

      await(first.close())

      await(second.tryLead()) shouldBe true
    }

    "read the unsent events in the order they were written, as the JSON that goes to Kafka" in {
      val customer = newCustomer()
      await(ops.createSubscription(customer.id, CreateSubscription(PlanType.FREE_TIER)))
      val store = newStore()
      await(store.tryLead())

      val rows = await(store.fetchUnsent(10))

      rows.map(_.customerId) shouldBe List(customer.id, customer.id)
      rows.map(_.id) shouldBe rows.map(_.id).sorted
      io.vertx.core.json.JsonObject(rows.head.payload).getString("event_type") shouldBe "CUSTOMER_CREATED"
      io.vertx.core.json.JsonObject(rows(1).payload).getString("event_type") shouldBe "SUBSCRIPTION_CHANGED"
    }

    "limit the batch, and not return what is marked sent" in {
      newCustomer(); newCustomer(); newCustomer()
      val store = newStore()
      await(store.tryLead())

      val batch = await(store.fetchUnsent(2))
      batch should have size 2
      await(store.markSent(batch.map(_.id)))

      await(store.fetchUnsent(10)).map(_.id) should not contain atLeastOneOf(batch.head.id, batch(1).id)
      await(store.fetchUnsent(10)) should have size 1
    }

    "refuse to read when it is not the leader" in {
      failureOf(newStore().fetchUnsent(10)) shouldBe a[IllegalStateException]
    }

    "count the backlog, and date the oldest event" in {
      await(newStore().backlog()) shouldBe Backlog(0, None)
      newCustomer(); newCustomer()

      val backlog = await(newStore().backlog())

      backlog.count shouldBe 2
      backlog.oldest.get.isBefore(Instant.now().plusSeconds(1)) shouldBe true
    }

    "prune only the events sent before the cut-off" in {
      newCustomer(); newCustomer(); newCustomer()
      val store = newStore()
      await(store.tryLead())
      val rows = await(store.fetchUnsent(10))
      await(store.markSent(rows.take(2).map(_.id)))
      sql(s"UPDATE outbox SET sent_at = now() - interval '10 days' WHERE id = ${rows.head.id}")

      await(store.pruneSent(Instant.now().minusSeconds(86400), 100)) shouldBe 1

      query("SELECT id FROM outbox ORDER BY id") shouldBe rows.tail.map(_.id.toString)
    }
  }
}
