package me.bataillec.portfolio.customer.unit.http

import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.http.HttpResponse
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HttpResponseSpec extends AnyWordSpec with Matchers {
  "HttpResponse" should {
    val defaultResponse = HttpResponse(statusCode = 200)
    val response        = HttpResponse(statusCode = 400, body = "body", contentType = "text/plain", headers = Map("header1" -> "value1"))

    // language=JSON
    val defaultJson = """{}"""

    // language=JSON
    val responseJson = """{"statusCode": 400, "body":  "body", "contentType":  "text/plain", "headers":  {"header1": "value1"}}"""

    "be built" when {
      "default values" in {
        inside(defaultResponse) { case HttpResponse(statusCode, body, contentType, headers) =>
          statusCode shouldBe 200
          body shouldBe ""
          contentType shouldBe "application/json"
          headers shouldBe Map.empty
        }
      }

      "given values" in {
        inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
          statusCode shouldBe 400
          body shouldBe "body"
          contentType shouldBe "text/plain"
          headers should have size 1
          headers should contain("header1" -> "value1")
        }
      }

      "from JSON string" in {
        HttpResponse.fromJsonString(defaultJson) shouldBe defaultResponse
        HttpResponse.fromJsonString(responseJson) shouldBe response
      }
    }

    "become a JsonObject" in {
      defaultResponse.toJson shouldBe JsonObject().put("statusCode", 200).put("contentType", "application/json").put("body", "").put("headers", JsonObject())
      response.toJson shouldBe JsonObject()
        .put("statusCode", 400)
        .put("contentType", "text/plain")
        .put("body", "body")
        .put("headers", JsonObject().put("header1", "value1"))
    }
  }
}
