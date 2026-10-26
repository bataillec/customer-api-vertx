package me.bataillec.portfolio.customer.domain

import me.bataillec.portfolio.customer.http.{HttpResponse, WithHttpResponse}

case class Customer() extends WithHttpResponse {

  override def toHttpResponse(statusCode: Int, contentType: String, headers: Map[String, String]): HttpResponse = ???
}
