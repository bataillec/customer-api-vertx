package me.bataillec.portfolio.customer.http

import me.bataillec.portfolio.customer.Constants.*

trait WithHttpResponse {
  def toHttpResponse(statusCode: Int = DEFAULT_STATUS_CODE, contentType: String = DEFAULT_CONTENT_TYPE, headers: Map[String, String] = Map.empty): HttpResponse
}
