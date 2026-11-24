package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.audit.*
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, Instant, ZoneOffset}
import java.util.UUID
import scala.collection.mutable.ListBuffer

class OutboxRelaySpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  private val vertx = Vertx.vertx()
  private val now   = Instant.parse("2026-11-24T12:00:00Z")
  private val ca    = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000000")
  private val cb    = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000000")
  private val boom  = new RuntimeException("boom")

  override protected def afterAll(): Unit = {
    await(vertx.close())
    super.afterAll()
  }

  private class FakeStore(var rows: List[OutboxRow] = Nil) extends OutboxStore {
    var leader: Boolean                = false
    var canLead: Boolean               = true
    var leadFailure: Option[Throwable] = None
    var fetchFailure: Option[Throwable] = None
    var markFailure: Option[Throwable]  = None
    val sent: ListBuffer[Long]         = ListBuffer.empty
    val pruned: ListBuffer[(Instant, Int)] = ListBuffer.empty
    var stepDowns                      = 0
    var closed                         = false

    override def tryLead(): Future[Boolean] = leadFailure match {
      case Some(e) => Future.failedFuture(e)
      case None    =>
        if (canLead) leader = true
        Future.succeededFuture(leader)
    }

    override def fetchUnsent(limit: Int): Future[List[OutboxRow]] =
      fetchFailure.fold(Future.succeededFuture(rows.filterNot(r => sent.contains(r.id)).take(limit)))(e => Future.failedFuture(e))

    override def markSent(ids: List[Long]): Future[Unit] =
      markFailure.fold { sent ++= ids; Future.succeededFuture(()) }(e => Future.failedFuture(e))

    override def pruneSent(olderThan: Instant, limit: Int): Future[Int] = { pruned += ((olderThan, limit)); Future.succeededFuture(2) }

    override def backlog(): Future[Backlog] = Future.succeededFuture(Backlog(rows.size.toLong, None))

    override def stepDown(): Future[Unit] = { leader = false; stepDowns += 1; Future.succeededFuture(()) }

    override def close(): Future[Unit] = { closed = true; Future.succeededFuture(()) }
  }

  /** Records what reaches Kafka, in order. A payload in `failing` is refused. */
  private class FakeSink(failing: Set[String] = Set.empty) extends AuditLogger {
    val published: ListBuffer[String] = ListBuffer.empty
    override def audit(msg: String): Future[Void] =
      if (failing.contains(msg)) Future.failedFuture(boom)
      else { published += msg; Future.succeededFuture[Void]() }
    override def close(): Future[Void] = Future.succeededFuture[Void]()
  }

  private def row(id: Long, customer: UUID): OutboxRow = OutboxRow(id, customer, s"event-$id")

  private def relay(store: OutboxStore, sink: AuditLogger, batch: Int = 100, clock: Clock = Clock.fixed(now, ZoneOffset.UTC)) =
    new OutboxRelay(vertx, store, sink, 200, batch, Duration.ofDays(7), clock)

  "a round" should {
    "publish the rows in order and mark them sent" in {
      val store = new FakeStore(List(row(1, ca), row(2, ca), row(3, ca)))
      val sink  = new FakeSink

      val delay = await(relay(store, sink).step())

      sink.published.toList shouldBe List("event-1", "event-2", "event-3")
      store.sent.toList should contain theSameElementsAs List(1L, 2L, 3L)
      delay shouldBe 200L
    }

    "wait for the poll interval when there is nothing to send" in {
      val store = new FakeStore()

      await(relay(store, new FakeSink).step()) shouldBe 200L

      store.sent shouldBe empty
    }

    "go again at once when the batch was full" in {
      val store = new FakeStore(List(row(1, ca), row(2, ca), row(3, ca)))

      await(relay(store, new FakeSink, batch = 2).step()) shouldBe 0L

      store.sent.toList shouldBe List(1L, 2L)
    }

    "keep the order of one customer when an event fails: nothing after it is sent" in {
      val store = new FakeStore(List(row(1, ca), row(2, ca), row(3, ca)))
      val sink  = new FakeSink(failing = Set("event-2"))

      await(relay(store, sink).step()) shouldBe 200L

      sink.published.toList shouldBe List("event-1")
      store.sent.toList shouldBe List(1L)
    }

    "not let the failure of one customer hold back another" in {
      val store = new FakeStore(List(row(1, ca), row(2, cb), row(3, ca), row(4, cb)))
      val sink  = new FakeSink(failing = Set("event-1"))

      await(relay(store, sink).step())

      sink.published.toList shouldBe List("event-2", "event-4")
      store.sent.toList should contain theSameElementsAs List(2L, 4L)
    }

    "not go again at once when part of the batch failed" in {
      val store = new FakeStore(List(row(1, ca), row(2, ca)))

      await(relay(store, new FakeSink(failing = Set("event-2")), batch = 2).step()) shouldBe 200L
    }

    "send the failed event again on the next round" in {
      val store = new FakeStore(List(row(1, ca), row(2, ca)))
      val r     = relay(store, new FakeSink(failing = Set("event-1")))
      await(r.step())

      val sink = new FakeSink
      await(relay(store, sink).step())

      sink.published.toList shouldBe List("event-1", "event-2")
    }

    "send again what was published but not marked, so that the delivery is at-least-once" in {
      val store = new FakeStore(List(row(1, ca)))
      val sink  = new FakeSink
      store.markFailure = Some(boom)
      await(relay(store, sink).step()) shouldBe 2000L // the failure backoff

      store.markFailure = None
      await(relay(store, sink).step())

      sink.published.toList shouldBe List("event-1", "event-1")
      store.sent.toList shouldBe List(1L)
    }
  }

  "the leadership" should {
    "keep a standby instance idle, without reading the outbox" in {
      val store = new FakeStore(List(row(1, ca)))
      store.canLead = false
      val sink = new FakeSink

      await(relay(store, sink).step()) shouldBe 5000L

      sink.published shouldBe empty
      store.sent shouldBe empty
    }

    "be given up and retried later when the database fails" in {
      val store = new FakeStore(List(row(1, ca)))
      store.fetchFailure = Some(boom)

      await(relay(store, new FakeSink).step()) shouldBe 2000L

      store.stepDowns shouldBe 1
    }

    "be retried later when it cannot even be taken" in {
      val store = new FakeStore()
      store.leadFailure = Some(boom)

      await(relay(store, new FakeSink).step()) shouldBe 2000L
    }
  }

  "the pruning" should {
    "delete what was sent before the retention, at most once a minute" in {
      val store = new FakeStore()
      var time  = now
      val clock = new Clock {
        override def getZone                            = ZoneOffset.UTC
        override def withZone(zone: java.time.ZoneId)  = this
        override def instant(): Instant                 = time
      }
      val r = relay(store, new FakeSink, clock = clock)

      await(r.step())
      time = now.plusSeconds(30)
      await(r.step())
      time = now.plusSeconds(61)
      await(r.step())

      store.pruned.toList shouldBe List((now.minus(Duration.ofDays(7)), 1000), (now.plusSeconds(61).minus(Duration.ofDays(7)), 1000))
    }
  }

  "start and stop" should {
    "relay by itself, then stop and close the store" in {
      val store = new FakeStore(List(row(1, ca)))
      val sink  = new FakeSink
      val r     = relay(store, sink)

      r.start()
      val deadline = System.nanoTime() + 5_000_000_000L
      while (store.sent.isEmpty && System.nanoTime() < deadline) Thread.sleep(20)
      await(r.stop())

      store.sent.toList shouldBe List(1L)
      store.closed shouldBe true
    }
  }

  "backlog" should {
    "come from the store" in {
      await(relay(new FakeStore(List(row(1, ca), row(2, ca))), new FakeSink).backlog()) shouldBe Backlog(2, None)
    }
  }
}
