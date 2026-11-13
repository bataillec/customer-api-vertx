package me.bataillec.portfolio.customer.unit.eventbus

import io.vertx.core.Future
import io.vertx.core.eventbus.*
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.{DecodeException, JsonObject}
import io.vertx.ext.web.{RequestBody, RoutingContext}
import me.bataillec.portfolio.customer.eventbus.EventbusUtils.*
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

class EventbusUtilsSpec extends AnyWordSpec with Matchers {
  "getBodyAsJson" should {
    "valid JSON body" in {
      val rc          = mock(classOf[RoutingContext])
      val requestBody = mock(classOf[RequestBody])

      when(rc.body()).thenReturn(requestBody)
      when(requestBody.asJsonObject()).thenReturn(JsonObject().put("key1", "value1"))

      getBodyAsJson(rc) shouldBe JsonObject().put("key1", "value1")
    }

    "empty body" in {
      val rc          = mock(classOf[RoutingContext])
      val requestBody = mock(classOf[RequestBody])

      when(rc.body()).thenReturn(requestBody)
      when(requestBody.asJsonObject()).thenReturn(null)

      getBodyAsJson(rc) shouldBe JsonObject()
    }

    "unparseable body" in {
      val rc          = mock(classOf[RoutingContext])
      val requestBody = mock(classOf[RequestBody])

      when(rc.body()).thenReturn(requestBody)
      when(requestBody.asJsonObject()).thenThrow(new DecodeException("bad json"))

      val ex = the[Exception] thrownBy getBodyAsJson(rc)
      ex.getMessage shouldBe "The given request body could not be parsed as JSON"
    }

    "other exception" in {
      val rc          = mock(classOf[RoutingContext])
      val requestBody = mock(classOf[RequestBody])

      when(rc.body()).thenReturn(requestBody)
      when(requestBody.asJsonObject()).thenThrow(new RuntimeException("other exception"))

      val ex = the[Exception] thrownBy getBodyAsJson(rc)
      ex.getMessage shouldBe "other exception"
    }
  }

  "addPathParam" should {
    "present" in {
      val rc = mock(classOf[RoutingContext])
      when(rc.pathParam("my_param")).thenReturn("my_value")

      val options = new DeliveryOptions()

      val res = addPathParam(options, rc, "my_param")

      res should be theSameInstanceAs options
      res.getHeaders should have size 1
      res.getHeaders.get("my_param") shouldBe "my_value"
    }

    "absent" in {
      val rc = mock(classOf[RoutingContext])
      when(rc.pathParam("my_param")).thenReturn(null)

      val options = new DeliveryOptions()

      val ex = the[Exception] thrownBy addPathParam(options, rc, "my_param")

      ex.getMessage shouldBe "The path param 'my_param' is not found in the path"
      options.getHeaders shouldBe null
    }

    "keep pre-existing headers" in {
      val rc = mock(classOf[RoutingContext])
      when(rc.pathParam("my_param")).thenReturn("my_value")

      val options = new DeliveryOptions()
      options.addHeader("old_key", "old_value")
      options.addHeader("my_param", "old_value")

      val res = addPathParam(options, rc, "my_param")

      res should be theSameInstanceAs options
      res.getHeaders should have size 2
      res.getHeaders.get("old_key") shouldBe "old_value"
      res.getHeaders.getAll("my_param") should contain theSameElementsInOrderAs List("old_value", "my_value")
    }
  }

  "addQueryParam" should {
    "present" in {
      val rc      = mock(classOf[RoutingContext])
      val request = mock(classOf[HttpServerRequest])
      when(rc.request).thenReturn(request)
      when(request.getParam("my_param")).thenReturn("my_value")

      val options = new DeliveryOptions()

      val res = addQueryParam(options, rc, "my_param")

      res should be theSameInstanceAs options
      res.getHeaders should have size 1
      res.getHeaders.get("my_param") shouldBe "my_value"
    }

    "absent" in {
      val rc      = mock(classOf[RoutingContext])
      val request = mock(classOf[HttpServerRequest])
      when(rc.request).thenReturn(request)
      when(request.getParam("my_param")).thenReturn(null)

      val options = new DeliveryOptions()

      val ex = the[Exception] thrownBy addQueryParam(options, rc, "my_param")

      ex.getMessage shouldBe "The query param 'my_param' is not found in the request"
      options.getHeaders shouldBe null
    }

    "keep pre-existing headers" in {
      val rc      = mock(classOf[RoutingContext])
      val request = mock(classOf[HttpServerRequest])
      when(rc.request).thenReturn(request)
      when(request.getParam("my_param")).thenReturn("my_value")

      val options = new DeliveryOptions()
      options.addHeader("old_key", "old_value")
      options.addHeader("my_param", "old_value")

      val res = addQueryParam(options, rc, "my_param")

      res should be theSameInstanceAs options
      res.getHeaders should have size 2
      res.getHeaders.get("old_key") shouldBe "old_value"
      res.getHeaders.getAll("my_param") should contain theSameElementsInOrderAs List("old_value", "my_value")
    }
  }

  "addOptionalQueryParam" should {
    val table = Table(
      ("str", "param", "default", "expected", "size"),
      ("Value - No default", Some("5"), None, "5", 1),
      ("Value - Default", Some("5"), Some("10"), "5", 1),
      ("No value - Default", None, Some("10"), "10", 1)
    )

    forAll(table) { case (str: String, param: Option[String], default: Option[String], expected: String, s: Int) =>
      s"$str" in {
        val rc      = mock(classOf[RoutingContext])
        val request = mock(classOf[HttpServerRequest])
        when(rc.request).thenReturn(request)
        when(request.getParam("my_param")).thenReturn(param.orNull)

        val options = new DeliveryOptions()

        val res = addOptionalQueryParam(options, rc, "my_param", default)

        res should be theSameInstanceAs options
        res.getHeaders should have size s
        res.getHeaders.get("my_param") shouldBe expected
      }
    }

    "Value - No default (omitted)" in {
      val rc      = mock(classOf[RoutingContext])
      val request = mock(classOf[HttpServerRequest])
      when(rc.request).thenReturn(request)
      when(request.getParam("my_param")).thenReturn("5")

      val options = new DeliveryOptions()

      val res = addOptionalQueryParam(options, rc, "my_param")

      res should be theSameInstanceAs options
      res.getHeaders should have size 1
      res.getHeaders.get("my_param") shouldBe "5"
    }

    "No value - No default" in {
      val rc      = mock(classOf[RoutingContext])
      val request = mock(classOf[HttpServerRequest])
      when(rc.request).thenReturn(request)
      when(request.getParam("my_param")).thenReturn(null)

      val options = new DeliveryOptions()

      val res = addOptionalQueryParam(options, rc, "my_param", None)

      res should be theSameInstanceAs options
      res.getHeaders shouldBe null
    }
  }

  "MyEventBus sendFuture" should {
    val address = "my.address"

    def stubbedEventBus(result: Future[Message[String]]): EventBus = {
      val eb = mock(classOf[EventBus])
      when(eb.request[String](any[String](), any[AnyRef](), any[DeliveryOptions]())).thenReturn(result)
      eb
    }

    "send payload" in {
      val reply = mock(classOf[Message[String]])
      val eb    = stubbedEventBus(Future.succeededFuture(reply))

      val result = eb.sendFuture(address, Some("""{"key":"value"}"""), DeliveryOptions())

      result.succeeded() shouldBe true
      result.result() shouldBe reply

      verify(eb).request[String](eqTo(address), eqTo("""{"key":"value"}"""), any[DeliveryOptions]())
    }

    "send no payload" in {
      val reply  = mock(classOf[Message[String]])
      val eb     = stubbedEventBus(Future.succeededFuture(reply))
      val captor = ArgumentCaptor.forClass(classOf[AnyRef])

      val result = eb.sendFuture(address, None, DeliveryOptions())

      result.succeeded() shouldBe true
      result.result() shouldBe reply

      verify(eb).request[String](eqTo(address), captor.capture(), any[DeliveryOptions]())
      captor.getValue shouldBe null
    }

    "force timeout to 30s" in {
      val reply   = mock(classOf[Message[String]])
      val eb      = stubbedEventBus(Future.succeededFuture(reply))
      val options = DeliveryOptions().setSendTimeout(1)
      val captor  = ArgumentCaptor.forClass(classOf[DeliveryOptions])

      val result = eb.sendFuture(address, None, options)

      result.succeeded() shouldBe true
      result.result() shouldBe reply

      verify(eb).request[String](eqTo(address), any[AnyRef](), captor.capture())
      captor.getValue.getSendTimeout shouldBe 30000
    }

    "same delivery options are forwarded" in {
      val reply   = mock(classOf[Message[String]])
      val eb      = stubbedEventBus(Future.succeededFuture(reply))
      val options = DeliveryOptions().addHeader("action", "get")
      val captor  = ArgumentCaptor.forClass(classOf[DeliveryOptions])

      val result = eb.sendFuture(address, None, options)

      result.succeeded() shouldBe true
      result.result() shouldBe reply

      verify(eb).request[String](eqTo(address), any[AnyRef](), captor.capture())
      captor.getValue should be theSameInstanceAs options
      captor.getValue.getHeaders.get("action") shouldBe "get"
    }

    "failure is propagated" in {
      val failure = new ReplyException(ReplyFailure.TIMEOUT, "timeout")
      val eb      = stubbedEventBus(Future.failedFuture[Message[String]](failure))

      val result = eb.sendFuture[String](address, None, DeliveryOptions())

      result.failed() shouldBe true
      result.cause() shouldBe failure
    }
  }
}
