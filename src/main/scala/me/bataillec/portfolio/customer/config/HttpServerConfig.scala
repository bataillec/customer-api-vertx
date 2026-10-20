package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

final case class HttpServerConfig(
    httpPort: Int,
    httpInstances: Int
)

object HttpServerConfig {
  private val defaultPort      = 8080
  private val defaultInstances = 1

  def fromEnv(env: Map[String, String] = sys.env): HttpServerConfig = {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    val nbInstances         = env.int("HTTP_INSTANCES", defaultInstances)
    HttpServerConfig(
      httpPort = env.int("HTTP_PORT", defaultPort),
      httpInstances = if (nbInstances <= availableProcessors) nbInstances else defaultInstances
    )
  }
}
