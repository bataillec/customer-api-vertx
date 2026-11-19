package me.bataillec.portfolio.customer.support

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.WebClient
import me.bataillec.portfolio.customer.support.FutureUtils.await

import java.util.UUID

/** What the API answered: the status, the headers (case-insensitive lookup) and the raw body. */
final case class ApiResponse(status: Int, headers: Map[String, String], body: String) {
  def header(name: String): Option[String] = headers.get(name.toLowerCase)
  def json: JsonObject                     = JsonObject(body)
  def jsonArray: JsonArray                 = JsonArray(body)
  def string(field: String): String        = json.getString(field)
  def code: String                         = json.getString("code")
}

/** A thin blocking wrapper over the HTTP API of the app under test, with one helper per operation. */
class ApiClient(vertx: Vertx, port: Int) {
  private val client = WebClient.create(vertx)

  def send(method: HttpMethod, path: String, body: Option[String] = None, headers: Map[String, String] = Map.empty): ApiResponse = {
    val request = client.request(method, port, "localhost", path)
    headers.foreach((k, v) => request.putHeader(k, v))
    val sent = body match {
      case Some(b) =>
        request.putHeader("Content-Type", "application/json")
        request.sendBuffer(Buffer.buffer(b))
      case None    => request.send()
    }
    val response = await(sent, 20)
    val received = response.headers().entries().toArray.map(e => e.asInstanceOf[java.util.Map.Entry[String, String]]).map(e => e.getKey.toLowerCase -> e.getValue).toMap
    ApiResponse(response.statusCode(), received, Option(response.bodyAsString()).getOrElse(""))
  }

  def get(path: String, headers: Map[String, String] = Map.empty): ApiResponse = send(HttpMethod.GET, path, headers = headers)

  def post(path: String, body: JsonObject): ApiResponse = send(HttpMethod.POST, path, Some(body.encode()))

  def patch(path: String, body: JsonObject): ApiResponse = send(HttpMethod.PATCH, path, Some(body.encode()))

  def createCustomer(email: String = s"${UUID.randomUUID()}@example.com", companyName: Option[String] = None): ApiResponse = {
    val body = JsonObject().put("email", email)
    companyName.foreach(body.put("company_name", _))
    post("/customers", body)
  }

  def subscribe(customerId: String, planType: String): ApiResponse =
    post(s"/customers/$customerId/subscriptions", JsonObject().put("plan_type", planType))

  def consume(customerId: String, featureKey: String, amount: Long): ApiResponse =
    post(s"/customers/$customerId/entitlements/consume", JsonObject().put("feature_key", featureKey).put("amount", Long.box(amount)))

  def setStatus(customerId: String, status: String): ApiResponse =
    patch(s"/customers/$customerId", JsonObject().put("status", status))

  def close(): Unit = client.close()
}
