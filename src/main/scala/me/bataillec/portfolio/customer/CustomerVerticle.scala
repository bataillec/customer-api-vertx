package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.*
import me.bataillec.portfolio.customer.config.CustomerConfig
import me.bataillec.portfolio.customer.customer.{CustomerOperations, CustomerOps, PGStorage}
import me.bataillec.portfolio.customer.audit.OutboxRecorder
import me.bataillec.portfolio.customer.eventbus.CustomerHandlerImpl
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.output.MigrateResult
import org.slf4j.LoggerFactory

import scala.compiletime.uninitialized

class CustomerVerticle extends VerticleBase {
  private val logger                   = LoggerFactory.getLogger(getClass)
  private var pool: Pool               = uninitialized
  private var operations: CustomerOps  = uninitialized

  override def start(): Future[Unit] = {
    val config = CustomerConfig.fromEnv()

    for {
      _ <- vertx.executeBlocking {
             () => migrate(config)
             pool = PgBuilder
               .pool()
               .`with`(PoolOptions().setMaxSize(config.pgPoolSize))
               .connectingTo(
                 PgConnectOptions()
                   .setHost(config.pgHost)
                   .setPort(config.pgPort)
                   .setDatabase(config.pgDatabase)
                   .setUser(config.pgUser)
                   .setPassword(config.pgPassword)
               )
               .using(vertx)
               .build()
             val storage = new PGStorage(pool)
             operations = new CustomerOperations(storage, new OutboxRecorder(storage))
           }
      _ <- new CustomerHandlerImpl(vertx, operations).handle()
    } yield {
      logger.info("Customer verticle is running")
    }
  }

  override def stop(): Future[Unit] = {
    if (pool != null) {
      logger.info("Closing the database pool connection...")
      pool.close().mapEmpty()
    } else {
      Future.succeededFuture()
    }
  }

  private def migrate(conf: CustomerConfig): Unit = {
    val migrationResult = runMigrations(conf)
    logger.info("Applied {} migration(s)", migrationResult.migrationsExecuted)
  }

  protected def runMigrations(conf: CustomerConfig): MigrateResult =
    Flyway
      .configure()
      .dataSource(conf.jdbcUrl, conf.pgUser, conf.pgPassword)
      .load()
      .migrate()
}
