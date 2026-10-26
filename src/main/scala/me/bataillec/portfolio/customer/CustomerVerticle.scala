package me.bataillec.portfolio.customer

import io.vertx.core.{Future, VerticleBase}
import io.vertx.pgclient.{PgBuilder, PgConnectOptions}
import io.vertx.sqlclient.*
import me.bataillec.portfolio.customer.config.CustomerConfig
import me.bataillec.portfolio.customer.eventbus.CustomerHandler
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

class CustomerVerticle extends VerticleBase {
  private val logger     = LoggerFactory.getLogger(getClass)
  private var pool: Pool = _

  override def start(): Future[_] = {
    val config = CustomerConfig.fromEnv()

    for {
      _ <- vertx.executeBlocking { () =>
             migrate(config)
           }
      _  = pool = PgBuilder
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
      _ <- new CustomerHandler(vertx).handle()
    } yield {
      logger.info("Customer verticle is running")
    }
  }

  override def stop(): Future[_] = {
    if (pool != null) {
      logger.info("Closing the database pool connection...")
      pool.close()
    } else {
      Future.succeededFuture()
    }
  }

  private def migrate(conf: CustomerConfig): Unit = {
    val migrationResult = Flyway
      .configure()
      .dataSource(conf.jdbcUrl, conf.pgUser, conf.pgPassword)
      .load()
      .migrate()
    logger.info("Applied {} migration(s)", migrationResult.migrationsExecuted)
  }
}
