package me.bataillec.portfolio.customer.support

import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer

import java.sql.{Connection, DriverManager}
import java.util.UUID

/** A Postgres and a Kafka container, each started on first use. The images match `compose.yaml`, so the tests run
  * against what is deployed.
  *
  * The containers are never stopped explicitly: Ryuk removes them when it is enabled, and Testcontainers' own JVM
  * hook does when it is disabled (rootless Podman here).
  */
class Containers {
  private val PostgresImage = "postgres:18.7"
  private val KafkaImage    = "apache/kafka:4.4.0"

  private val AdminDatabase = "postgres"

  lazy val postgres: PostgreSQLContainer = {
    val container = new PostgreSQLContainer(PostgresImage)
    container.start()
    container
  }

  lazy val kafka: KafkaContainer = {
    val container = new KafkaContainer(KafkaImage)
    container.start()
    container
  }

  def pgHost: String         = postgres.getHost
  def pgPort: Int            = postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
  def pgUser: String         = postgres.getUsername
  def pgPassword: String     = postgres.getPassword
  def kafkaBootstrap: String = kafka.getBootstrapServers

  def jdbcUrl(database: String): String = s"jdbc:postgresql://$pgHost:$pgPort/$database"

  /** Creates an empty database, so that a suite can migrate from scratch without disturbing the others. */
  def newDatabase(prefix: String = "it"): String = {
    val name = s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"
    val conn = connection(AdminDatabase)
    try conn.createStatement().execute(s"""CREATE DATABASE "$name"""")
    finally conn.close()
    name
  }

  def connection(database: String): Connection = DriverManager.getConnection(jdbcUrl(database), pgUser, pgPassword)

  // Pausing freezes the container but keeps its mapped ports, which a restart would change. Clients see a service
  // that hangs, not one that refuses connections.
  def pausePostgres(): Unit   = pause(postgres)
  def unpausePostgres(): Unit = unpause(postgres)
  def pauseKafka(): Unit      = pause(kafka)
  def unpauseKafka(): Unit    = unpause(kafka)

  private def pause(container: org.testcontainers.containers.GenericContainer[?]): Unit = {
    container.getDockerClient.pauseContainerCmd(container.getContainerId).exec()
    ()
  }

  private def unpause(container: org.testcontainers.containers.GenericContainer[?]): Unit = {
    container.getDockerClient.unpauseContainerCmd(container.getContainerId).exec()
    ()
  }
}

/** The containers shared by every integration suite of the JVM. */
object ContainersSupport extends Containers
