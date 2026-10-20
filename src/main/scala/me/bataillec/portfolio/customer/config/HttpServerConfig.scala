package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

final case class HttpServerConfig(
    httpPort: Int
)

object HttpServerConfig {
  def fromEnv(env: Map[String, String] = sys.env): HttpServerConfig = {
    HttpServerConfig(
      httpPort = env.int(HTTP_PORT, HTTP_PORT_DEFAULT_VALUE)
    )
  }
}
