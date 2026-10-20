package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

final case class HttpServerConfig(
    httpPort: Int
)

object HttpServerConfig {
  private val defaultPort = 8080

  def fromEnv(env: Map[String, String] = sys.env): HttpServerConfig = {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    HttpServerConfig(
      httpPort = env.int("HTTP_PORT", defaultPort)
    )
  }
}
