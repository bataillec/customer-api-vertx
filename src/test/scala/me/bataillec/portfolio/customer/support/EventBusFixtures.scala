package me.bataillec.portfolio.customer.support

import io.vertx.core.eventbus.{EventBus, Message, MessageConsumer}
import io.vertx.core.{Future, Handler, MultiMap, Vertx}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*

import scala.collection.mutable

/** A mocked [[Vertx]] / [[EventBus]] pair that records every `consumer(address, handler)` registration.
  *
  * Each address gets its own mocked [[MessageConsumer]], whose `completion()` is `defaultCompletion` unless overridden with [[completeWith]] (to be called
  * before the registration happens).
  */
class EventBusFixture(defaultCompletion: Future[Void] = Future.succeededFuture[Void](null)) {
  val vertx: Vertx = mock(classOf[Vertx])
  val eb: EventBus = mock(classOf[EventBus])

  private val consumers = mutable.LinkedHashMap.empty[String, MessageConsumer[String]]
  private val order     = mutable.ListBuffer.empty[String]

  when(vertx.eventBus()).thenReturn(eb)
  when(eb.consumer[String](any[String](), any[Handler[Message[String]]]())).thenAnswer { invocation =>
    val address = invocation.getArgument[String](0)
    order += address
    consumer(address)
  }

  /** The mocked consumer returned for `address` (created on first use). */
  def consumer(address: String): MessageConsumer[String] =
    consumers.getOrElseUpdate(
      address, {
        val c = mock(classOf[MessageConsumer[String]])
        when(c.completion()).thenReturn(defaultCompletion)
        c
      }
    )

  /** Overrides the registration result of `address`. */
  def completeWith(address: String, completion: Future[Void]): Unit =
    when(consumer(address).completion()).thenReturn(completion)

  /** The addresses a consumer was registered on, in registration order. */
  def registeredAddresses: List[String] = order.toList

  /** The handler registered on `address`. */
  def registeredHandler(address: String): Handler[Message[String]] = {
    val captor = ArgumentCaptor.forClass(classOf[Handler[?]])
    verify(eb).consumer[String](eqTo(address), captor.capture().asInstanceOf[Handler[Message[String]]])
    captor.getValue.asInstanceOf[Handler[Message[String]]]
  }
}

object TestMessages {

  /** A mocked [[Message]] with a body and real (case-insensitive) headers. */
  def message(body: String = null, headers: (String, String)*): Message[String] = {
    val msg = mock(classOf[Message[String]])
    val map = MultiMap.caseInsensitiveMultiMap()
    headers.foreach((k, v) => map.add(k, v))
    when(msg.body()).thenReturn(body)
    when(msg.headers()).thenReturn(map)
    msg
  }
}
