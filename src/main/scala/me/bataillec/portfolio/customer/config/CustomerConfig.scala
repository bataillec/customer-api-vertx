package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

case class CustomerConfig(
    pgHost: String,
    pgPort: Int,
    pgDatabase: String,
    pgUser: String,
    pgPassword: String,
    pgPoolSize: Int,
    jdbcUrl: String
)

object CustomerConfig {
  def fromEnv(env: Map[String, String] = sys.env): CustomerConfig = {
    val pgHost     = env.str("PG_HOST", "localhost")
    val pgPort     = env.int("PG_PORT", 5432)
    val pgDatabase = env.str("PG_DATABASE", "customers")
    CustomerConfig(
      pgHost = pgHost,
      pgPort = pgPort,
      pgDatabase = pgDatabase,
      pgUser = env.str("PG_USER", "customers"),
      pgPassword = env.str("PG_PASSWORD", "customers"),
      pgPoolSize = env.int("PG_POOL_SIZE", 16),
      jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"
    )
  }
}
