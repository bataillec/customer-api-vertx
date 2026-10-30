package me.bataillec.portfolio.customer.http

import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.Constants.*

import scala.jdk.CollectionConverters.*

final case class HttpResponse(
    statusCode: Int,
    body: String = DEFAULT_BODY,
    contentType: String = DEFAULT_CONTENT_TYPE,
    headers: Map[String, String] = Map.empty
) {
  def toJson: JsonObject = {
    val json        = new JsonObject()
    val headersJson = new JsonObject()
    headers.foreach((k, v) => headersJson.put(k, v))
    json
      .put(STATUS_CODE_FIELD, statusCode)
      .put(CONTENT_TYPE_FIELD, contentType)
      .put(BODY_FIELD, body)
      .put(HEADERS_FIELD, headersJson)
  }
}

object HttpResponse {
  def fromJsonString(s: String): HttpResponse = {
    val json = JsonObject(s)

    HttpResponse(
      statusCode = json.getInteger(STATUS_CODE_FIELD, DEFAULT_STATUS_CODE).intValue(),
      body = json.getString(BODY_FIELD, DEFAULT_BODY),
      contentType = Option(json.getString(CONTENT_TYPE_FIELD)).getOrElse(DEFAULT_CONTENT_TYPE),
      headers = json.getJsonObject(HEADERS_FIELD, JsonObject()).getMap.asScala.map((k, v) => k -> v.toString).toMap
    )
  }
}
