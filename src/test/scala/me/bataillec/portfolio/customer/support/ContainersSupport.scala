package me.bataillec.portfolio.customer.support

import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer

import java.sql.{Connection, DriverManager}
import java.util.UUID

/** The containers shared by every integration suite of the JVM, started once on first use. The images match
  * `compose.yaml`, so the tests run against what is deployed.
  *
  * Ryuk is disabled on this machine (rootless Podman), so a JVM shutdown hook stops the containers.
  */
object ContainersSupport {
  private val PostgresImage = "postgres:18.7"
  private val KafkaImage    = "apache/kafka:4.4.0"

  private val AdminDatabase = "postgres"

  lazy val postgres: PostgreSQLContainer = {
    val container = new PostgreSQLContainer(PostgresImage)
    container.start()
    sys.addShutdownHook(container.stop())
    container
  }

  lazy val kafka: KafkaContainer = {
    val container = new KafkaContainer(KafkaImage)
    container.start()
    sys.addShutdownHook(container.stop())
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
}
