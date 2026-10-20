package me.bataillec.portfolio.customer.http

import io.vertx.core.json.{Json, JsonObject}
import me.bataillec.portfolio.customer.Constants.*

import scala.jdk.CollectionConverters.*

case class HttpResponse(
    statusCode: Int,
    body: String = DEFAULT_BODY,
    contentType: String = DEFAULT_CONTENT_TYPE,
    headers: Map[String, String] = Map.empty
)

object HttpResponse {
  def fromJsonString(s: String): HttpResponse = {
    val json = JsonObject(s)

    HttpResponse(
      statusCode = json.getInteger(STATUS_CODE_FIELD, DEFAULT_STATUS_CODE).intValue(),
      body = Json.encode(json.getValue(BODY_FIELD, JsonObject())),
      contentType = Option(json.getString(CONTENT_TYPE_FIELD)).getOrElse(DEFAULT_CONTENT_TYPE),
      headers = json.getJsonObject(HEADERS_FIELD, JsonObject()).getMap.asScala.map((k, v) => k -> v.toString).toMap
    )
  }
}
