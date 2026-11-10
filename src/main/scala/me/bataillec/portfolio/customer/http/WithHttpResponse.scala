package me.bataillec.portfolio.customer.http

import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.json.JsonSupport

trait WithHttpResponse {
  def toHttpResponse(
      statusCode: Int = DEFAULT_STATUS_CODE,
      contentType: String = DEFAULT_CONTENT_TYPE,
      headers: Map[String, String] = Map.empty
  ): HttpResponse =
    HttpResponse(
      statusCode = statusCode,
      body = JsonSupport.encode(this), // FIXME JsonSupport.encode only if contentType == "application/json"
      contentType = contentType,
      headers = headers
    )
}
