package me.bataillec.portfolio.customer.audit

import io.vertx.core.{CompositeFuture, Future, Vertx}
import org.slf4j.LoggerFactory

import java.time.{Clock, Duration, Instant}
import scala.jdk.CollectionConverters.*

/** Publishes the events of the outbox table to Kafka, in the order they were written, then marks them sent.
  *
  * Delivery is at-least-once: a crash between the Kafka acknowledgement and `markSent` publishes the row again, with
  * the same `event_id`, so consumers deduplicate on it.
  *
  * Only the leader of the [[OutboxStore]] relays. Inside a batch, the rows of one customer are sent one after the
  * other, each acknowledged before the next, and stop at the first failure: a later event can never overtake an
  * earlier one. Different customers go in parallel.
  */
class OutboxRelay(
    vertx: Vertx,
    store: OutboxStore,
    sink: AuditLogger,
    pollIntervalMs: Long,
    batchSize: Int,
    retention: Duration,
    clock: Clock = Clock.systemUTC()
) {
  private val logger = LoggerFactory.getLogger(getClass)

  private var running         = false
  private var timer: Option[Long] = None
  private var lastPrune       = Instant.EPOCH

  def start(): Unit = {
    running = true
    schedule(0)
  }

  /** Stops relaying, gives the leadership up and closes the store. The sink is closed by its owner. */
  def stop(): Future[Unit] = {
    running = false
    timer.foreach(vertx.cancelTimer)
    timer = None
    store.close()
  }

  def backlog(): Future[Backlog] = store.backlog()

  private def schedule(delayMs: Long): Unit = {
    if (running) {
      timer = Some(vertx.setTimer(delayMs.max(1L), _ => { timer = None; run() }))
    }
  }

  private def run(): Unit = {
    if (running) step().onComplete(r => schedule(r.result()))
  }

  /** One round (public so that the tests can drive it): take the leadership if needed, relay a batch, prune now and then. Returns the delay before the next round. */
  def step(): Future[Long] = {
    store
      .tryLead()
      .compose { leading =>
        if (!leading) Future.succeededFuture[Long](OutboxRelay.STANDBY_RETRY_MS)
        else
          relayBatch().compose { full =>
            prune().map(_ => if (full) 0L else pollIntervalMs)
          }
      }
      .recover { e =>
        logger.warn("The outbox relay failed, trying again later: {}", e.getMessage)
        store.stepDown().map(_ => OutboxRelay.FAILURE_BACKOFF_MS)
      }
  }

  /** Relays one batch; true when the batch was full and entirely sent, so there is probably more to send right away. */
  def relayBatch(): Future[Boolean] = {
    store.fetchUnsent(batchSize).compose { rows =>
      if (rows.isEmpty) Future.succeededFuture[Boolean](false)
      else {
        val perCustomer = rows.groupBy(_.customerId).values.map(sendInOrder).toList
        Future.all(perCustomer.asJava).compose { all =>
          val sent = perCustomer.indices.flatMap(i => all.resultAt[List[Long]](i)).toList
          val done = if (sent.isEmpty) Future.succeededFuture(()) else store.markSent(sent)
          done.map(_ => sent.size == rows.size && rows.size == batchSize)
        }
      }
    }
  }

  /** Sends the rows one after the other, and stops at the first failure. Never fails: it returns the ids that were sent. */
  private def sendInOrder(rows: List[OutboxRow]): Future[List[Long]] = rows match {
    case Nil          => Future.succeededFuture(Nil)
    case row :: rest =>
      sink
        .audit(row.payload)
        .map[Boolean](_ => true)
        .recover { e =>
          logger.warn("Failed to publish the outbox event {} of customer {}: {}", row.id, row.customerId, e.getMessage)
          Future.succeededFuture[Boolean](false)
        }
        .compose { sent =>
          if (sent) sendInOrder(rest).map(row.id :: _)
          else Future.succeededFuture[List[Long]](Nil)
        }
  }

  private def prune(): Future[Unit] = {
    val now = clock.instant()
    if (Duration.between(lastPrune, now).compareTo(OutboxRelay.PRUNE_EVERY) < 0) Future.succeededFuture(())
    else {
      lastPrune = now
      store
        .pruneSent(now.minus(retention), OutboxRelay.PRUNE_BATCH)
        .map(n => if (n > 0) logger.info("Pruned {} sent outbox event(s)", n))
        .recover { e =>
          logger.warn("Failed to prune the outbox: {}", e.getMessage)
          Future.succeededFuture(())
        }
    }
  }
}

object OutboxRelay {
  private[audit] val STANDBY_RETRY_MS   = 5000L
  private[audit] val FAILURE_BACKOFF_MS = 2000L
  private[audit] val PRUNE_EVERY        = Duration.ofMinutes(1)
  private[audit] val PRUNE_BATCH        = 1000
}
