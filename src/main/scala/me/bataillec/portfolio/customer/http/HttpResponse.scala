package me.bataillec.portfolio.customer.http

import io.vertx.core.json.JsonObject

import scala.jdk.CollectionConverters.*

case class HttpResponse(
    statusCode: Int,
    body: String = "",
    contentType: String = "application/json",
    headers: Map[String, String] = Map.empty
)

object HttpResponse {
  def fromJsonString(s: String): HttpResponse = {
    val json = JsonObject(s)

    HttpResponse(
      statusCode = Option(json.getInteger("statusCode").intValue()).getOrElse(200),
      body = json.getJsonObject("body", JsonObject()).encode(),
      contentType = Option(json.getString("contentType")).getOrElse("application/json"),
      headers = json.getJsonObject("headers", JsonObject()).getMap.asScala.map((k, v) => k -> v.toString).toMap
    )
  }
}
