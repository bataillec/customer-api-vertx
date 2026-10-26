package me.bataillec.portfolio.customer.domain

import io.vertx.core.json.{JsonArray, JsonObject}
import me.bataillec.portfolio.customer.http.{HttpResponse, WithHttpResponse}

case class ListCustomers(
    items: List[Customer],
    next_cursor: Option[String]
) extends WithHttpResponse {

  override def toHttpResponse(statusCode: Int, contentType: String, headers: Map[String, String]): HttpResponse = {
    val items_json = new JsonArray()
    items.foreach { c =>
      items_json.add(JsonObject.mapFrom(c))
    }
    val json       = new JsonObject()
      .put("items", items_json)
    next_cursor.foreach(v => json.put("next_cursor", v))

    HttpResponse(
      statusCode = statusCode,
      body = json.encode(),
      contentType = contentType,
      headers = headers
    )
  }
}
