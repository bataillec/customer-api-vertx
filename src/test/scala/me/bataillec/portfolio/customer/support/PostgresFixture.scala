package me.bataillec.portfolio.customer.support

import io.vertx.core.Vertx
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.{Pool, PoolOptions}
import org.flywaydb.core.Flyway
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.compiletime.uninitialized

/** A migrated database of its own for the suite, on the shared Postgres container, with a Vert.x pool on it.
  * The data tables are emptied before each test; `plan_quotas`, seeded by the migrations, is kept.
  */
trait PostgresFixture extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  protected var database: String = uninitialized
  protected var vertx: Vertx     = uninitialized
  protected var pool: Pool       = uninitialized

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    database = ContainersSupport.newDatabase(getClass.getSimpleName.toLowerCase)
    Flyway
      .configure()
      .dataSource(ContainersSupport.jdbcUrl(database), ContainersSupport.pgUser, ContainersSupport.pgPassword)
      .load()
      .migrate()
    vertx = Vertx.vertx()
    pool = PgBuilder
      .pool()
      .`with`(PoolOptions().setMaxSize(16))
      .connectingTo(
        PgConnectOptions()
          .setHost(ContainersSupport.pgHost)
          .setPort(ContainersSupport.pgPort)
          .setDatabase(database)
          .setUser(ContainersSupport.pgUser)
          .setPassword(ContainersSupport.pgPassword)
      )
      .using(vertx)
      .build()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("TRUNCATE customers CASCADE") // subscriptions and entitlements follow through their foreign keys
  }

  override protected def afterAll(): Unit = {
    try {
      if (pool != null) FutureUtils.await(pool.close())
      if (vertx != null) FutureUtils.await(vertx.close())
    } finally super.afterAll()
  }

  /** Runs a statement over plain JDBC, to arrange or inspect the data behind the code under test. */
  protected def sql(statement: String): Unit = {
    val conn = ContainersSupport.connection(database)
    try conn.createStatement().execute(statement)
    finally conn.close()
    ()
  }
}
