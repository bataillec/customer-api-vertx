package me.bataillec.portfolio.customer.audit

import io.vertx.core.{Future, Vertx}
import io.vertx.pgclient.{PgConnectOptions, PgConnection}
import io.vertx.sqlclient.{Pool, SqlConnection, Tuple}

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** An event waiting in the outbox: `payload` is the JSON envelope, as it goes to Kafka. */
final case class OutboxRow(id: Long, customerId: UUID, payload: String)

/** How many events wait to be published, and since when. `count` stops at [[OutboxStore.BACKLOG_CAP]]. */
final case class Backlog(count: Long, oldest: Option[Instant])

/** The relay's view of the outbox table. Only the leader (see `tryLead`) may read and update it. */
trait OutboxStore {

  /** True when this instance is the one relay allowed to publish. Keeps the leadership once it has it. */
  def tryLead(): Future[Boolean]

  /** The oldest unsent events, in the order they were written. */
  def fetchUnsent(limit: Int): Future[List[OutboxRow]]

  def markSent(ids: List[Long]): Future[Unit]

  /** Deletes up to `limit` events sent before `olderThan`; returns how many. */
  def pruneSent(olderThan: Instant, limit: Int): Future[Int]

  def backlog(): Future[Backlog]

  /** Gives the leadership up, e.g. after a failure: the next `tryLead` starts again. */
  def stepDown(): Future[Unit]

  def close(): Future[Unit]
}

/** Several `audit` instances may run: only one holds a Postgres advisory lock, taken on a connection of its own (a
  * session lock lives as long as the connection), so the events of a customer are published by one relay, in order.
  * If the leader dies, its connection closes, the lock is released and another instance takes over.
  */
class PGOutboxStore(vertx: Vertx, options: PgConnectOptions, pool: Pool) extends OutboxStore {
  private var leader: Option[SqlConnection] = None

  override def tryLead(): Future[Boolean] = leader match {
    case Some(_) => Future.succeededFuture(true)
    case None    =>
      PgConnection.connect(vertx, options).compose { conn =>
        conn
          .preparedQuery("SELECT pg_try_advisory_lock($1) AS locked")
          .execute(Tuple.of(Long.box(PGOutboxStore.LEADER_LOCK)))
          .map(_.asScala.head.getBoolean("locked").booleanValue())
          .compose { locked =>
            if (locked) {
              leader = Some(conn)
              Future.succeededFuture(true)
            } else conn.close().map(_ => false)
          }
          .recover(e => conn.close().transform(_ => Future.failedFuture[Boolean](e)))
      }
  }

  /** Runs on the leader's connection. Any failure drops the leadership: the connection may be gone, and with it the lock. */
  private def asLeader[T](f: SqlConnection => Future[T]): Future[T] = leader match {
    case None       => Future.failedFuture(new IllegalStateException("Not the outbox leader"))
    case Some(conn) => f(conn).recover(e => stepDown().transform(_ => Future.failedFuture[T](e)))
  }

  override def fetchUnsent(limit: Int): Future[List[OutboxRow]] = asLeader { conn =>
    conn
      .preparedQuery("SELECT id, customer_id, payload::text AS payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT $1")
      .execute(Tuple.of(Int.box(limit)))
      .map(_.asScala.map(r => OutboxRow(r.getLong("id"), r.getUUID("customer_id"), r.getString("payload"))).toList)
  }

  override def markSent(ids: List[Long]): Future[Unit] = asLeader { conn =>
    conn
      .preparedQuery("UPDATE outbox SET sent_at = now() WHERE id = ANY($1::bigint[])")
      .execute(Tuple.tuple().addArrayOfLong(ids.map(Long.box).toArray))
      .mapEmpty[Unit]()
  }

  override def pruneSent(olderThan: Instant, limit: Int): Future[Int] = asLeader { conn =>
    conn
      .preparedQuery("DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE sent_at IS NOT NULL AND sent_at < $1 LIMIT $2)")
      .execute(Tuple.of(olderThan.atOffset(ZoneOffset.UTC), Int.box(limit)))
      .map(_.rowCount())
  }

  // On the pool, so that a standby instance can report it too. Capped: counting a huge backlog on every probe would be slow.
  override def backlog(): Future[Backlog] = {
    pool
      .preparedQuery(
        s"""SELECT count(*) AS n, min(created_at) AS oldest
           |FROM (SELECT created_at FROM outbox WHERE sent_at IS NULL LIMIT ${OutboxStore.BACKLOG_CAP}) unsent""".stripMargin
      )
      .execute()
      .map { rows =>
        val row = rows.asScala.head
        Backlog(row.getLong("n"), Option(row.getOffsetDateTime("oldest")).map(_.toInstant))
      }
  }

  override def stepDown(): Future[Unit] = leader match {
    case None       => Future.succeededFuture(())
    case Some(conn) =>
      leader = None
      // Closing the connection ends the session, and releases the lock with it
      conn.close().transform(_ => Future.succeededFuture(()))
  }

  override def close(): Future[Unit] = stepDown().compose(_ => pool.close().mapEmpty[Unit]())
}

object OutboxStore {
  val BACKLOG_CAP = 10000
}

object PGOutboxStore {
  /** The key of the advisory lock the relays compete for. */
  val LEADER_LOCK = 7204021L
}
