package me.bataillec.portfolio.customer.support

import io.vertx.core.json.JsonObject
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

/** An audit event as read from the topic: the Kafka coordinates and the JSON envelope. */
final case class AuditEvent(key: String, partition: Int, offset: Long, json: JsonObject) {
  def eventType: String                  = json.getString("event_type")
  def customerId: String                 = json.getString("customer_id")
  def field(name: String): Option[String] = Option(json.getString(name))
}

/** Reads the audit topic from its beginning, without a consumer group, to assert on what the app published.
  * The writes are fire-and-forget, so the callers poll with a timeout rather than assert straight after a request.
  */
class AuditProbe(bootstrapServers: String, topic: String) extends AutoCloseable {
  private val consumer = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("enable.auto.commit", "false")
    new KafkaConsumer[String, String](props)
  }
  private val seen = ListBuffer.empty[AuditEvent]

  private val partitions = consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(topic, p.partition())).toList
  consumer.assign(partitions.asJava)
  consumer.seekToBeginning(partitions.asJava)

  private def poll(wait: Duration): Unit =
    consumer.poll(wait).asScala.foreach { r =>
      seen += AuditEvent(r.key(), r.partition(), r.offset(), JsonObject(r.value()))
    }

  private def eventsOf(customerId: String): List[AuditEvent] = seen.filter(_.customerId == customerId).toList

  /** Polls until `count` events of the customer have arrived, and returns them in topic order per partition.
    * Fails with what was received when the timeout expires.
    */
  def awaitEvents(customerId: String, count: Int, timeout: Duration = Duration.ofSeconds(30)): List[AuditEvent] = {
    val deadline = System.nanoTime() + timeout.toNanos
    poll(Duration.ofMillis(100))
    while (eventsOf(customerId).size < count && System.nanoTime() < deadline) poll(Duration.ofMillis(500))
    val events = eventsOf(customerId)
    if (events.size < count)
      throw new AssertionError(
        s"Expected $count audit event(s) for $customerId within $timeout but got ${events.size}: " +
          events.map(e => s"${e.eventType}@${e.partition}/${e.offset}").mkString("[", ", ", "]") +
          s". The topic holds ${seen.size} event(s) in total."
      )
    events
  }

  /** What the topic holds for the customer after `settle`: for asserting that something was NOT published. */
  def eventsAfter(customerId: String, settle: Duration): List[AuditEvent] = {
    val deadline = System.nanoTime() + settle.toNanos
    while (System.nanoTime() < deadline) poll(Duration.ofMillis(300))
    eventsOf(customerId)
  }

  override def close(): Unit = consumer.close()
}

object AuditProbe {
  def createTopic(bootstrapServers: String, topic: String, partitions: Int): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    val admin = Admin.create(props)
    try admin.createTopics(java.util.List.of(new NewTopic(topic, partitions, 1.toShort))).all().get()
    finally admin.close()
    ()
  }
}
