package me.bataillec.portfolio.customer.support

import io.vertx.core.Vertx
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer

import java.nio.file.Paths
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CompletableFuture, Future}

/** One container of the app image: the command picks the verticle, like in `compose.yaml`. */
final class AppContainer(image: Future[String]) extends GenericContainer[AppContainer](image)

/** The whole platform as it is deployed: Postgres, Kafka (KRaft), two `customer` nodes, one `audit` node and one
  * `http` node, each in a container of its own on a shared network, the app ones built from the project's
  * `Dockerfile`. It mirrors `compose.yaml` without pgAdmin and Kafka UI.
  *
  * Unlike the integration tests, nothing runs inside the test JVM: this is what the packaging, the entrypoint and
  * `docker/cluster.xml` are checked with.
  *
  * Set the system property `e2e.image` to the name of an already built image to skip the build of the Dockerfile.
  */
final class PlatformStack {
  private val VerticlePackage = "me.bataillec.portfolio.customer"
  private val HttpPort        = 8080

  /** The names the containers are known by on the network. The members of `docker/cluster.xml` are these names. */
  val CustomerNodes: List[String] = List("customer-1", "customer-2")

  val auditTopic: String = s"e2e.customer.events.${UUID.randomUUID().toString.take(8)}"

  private val network = Network.newNetwork()

  // Built once, when the first container needs it, from the same files as `podman compose build`
  private val image: Future[String] = sys.props.get("e2e.image").filter(_.nonEmpty) match {
    case Some(name) => CompletableFuture.completedFuture(name)
    case None       =>
      new ImageFromDockerfile("customer-api-vertx-e2e", false)
        .withFileFromPath("Dockerfile", Paths.get("Dockerfile"))
        .withFileFromPath("pom.xml", Paths.get("pom.xml"))
        .withFileFromPath("src", Paths.get("src"))
        .withFileFromPath("docker", Paths.get("docker"))
  }

  private val postgres = new PostgreSQLContainer("postgres:18.7")
    .withDatabaseName("customers")
    .withUsername("customers")
    .withPassword("customers")
    .withNetwork(network)
    .withNetworkAliases("postgres")

  // The broker advertises its host name to the other containers: it must be the name they dial
  private val kafka = new KafkaContainer("apache/kafka:4.4.0")
    .withNetwork(network)
    .withNetworkAliases("kafka")
    .withCreateContainerCmdModifier { cmd => cmd.withHostName("kafka"); () }

  private def app(name: String, verticle: String, env: Map[String, String]): AppContainer = {
    val container = new AppContainer(image)
      .withNetwork(network)
      .withNetworkAliases(name)
      .withCommand(s"$VerticlePackage.$verticle")
    env.foreach((k, v) => container.withEnv(k, v))
    container
  }

  private val databaseEnv = Map("PG_HOST" -> "postgres", "PG_DATABASE" -> "customers", "PG_USER" -> "customers", "PG_PASSWORD" -> "customers")

  private val customers = CustomerNodes.map(name => name -> app(name, "CustomerVerticle", databaseEnv)).toMap

  private val audit = app("audit", "AuditVerticle", databaseEnv ++ Map("KAFKA_BOOTSTRAP_SERVERS" -> "kafka:9093", "AUDIT_TOPIC" -> auditTopic))

  // Ready once the customer and the audit nodes answer the readiness check, which proves that they found each other
  private val http = app("http", "HttpServerVerticle", Map.empty)
    .withExposedPorts(HttpPort)
    .waitingFor(Wait.forHttp("/health/ready").forPort(HttpPort).forStatusCode(200).withStartupTimeout(Duration.ofMinutes(4)))

  private val containers: Map[String, GenericContainer[?]] =
    customers ++ Map("postgres" -> postgres, "kafka" -> kafka, "audit" -> audit, "http" -> http)

  private var vertx: Vertx = scala.compiletime.uninitialized

  /** Talks to the platform through its published HTTP port, like any client would. */
  lazy val api: ApiClient = new ApiClient(vertx, http.getMappedPort(HttpPort))

  /** The address of the broker for the test JVM. The containers use `kafka:9093` on the network. */
  def kafkaBootstrap: String = kafka.getBootstrapServers

  def start(): Unit = {
    vertx = Vertx.vertx()
    image.get() // build the app image up front, instead of racing for it when the containers start together
    Startables.deepStart(java.util.List.of(postgres, kafka)).get()
    AuditProbe.createTopic(kafkaBootstrap, auditTopic, 3)
    Startables.deepStart(java.util.List.of[org.testcontainers.lifecycle.Startable](audit, customers("customer-1"), customers("customer-2"))).get()
    http.start()
  }

  def stopAll(): Unit = {
    try if (vertx != null) vertx.close().toCompletionStage.toCompletableFuture.get()
    finally {
      containers.values.foreach(c => try c.stop() catch { case _: Exception => () })
      try network.close() catch { case _: Exception => () }
    }
  }

  private def container(name: String): GenericContainer[?] =
    containers.getOrElse(name, throw new IllegalArgumentException(s"No container named $name, expected one of ${containers.keys.toList.sorted}"))

  def isRunning(name: String): Boolean = {
    val c = container(name)
    c.getDockerClient.inspectContainerCmd(c.getContainerId).exec().getState.getRunning
  }

  /** Stops the container, as an orchestrator would on a scale down or a deploy. The container is kept, so it can start again. */
  def stop(name: String): Unit = {
    val c = container(name)
    c.getDockerClient.stopContainerCmd(c.getContainerId).withTimeout(20).exec()
    ()
  }

  def startAgain(name: String): Unit = {
    val c = container(name)
    c.getDockerClient.startContainerCmd(c.getContainerId).exec()
    ()
  }

  /** Freezes the container without closing its connections: the clients see a service that hangs, not one that refuses. */
  def pause(name: String): Unit = {
    val c = container(name)
    c.getDockerClient.pauseContainerCmd(c.getContainerId).exec()
    ()
  }

  def unpause(name: String): Unit = {
    val c = container(name)
    c.getDockerClient.unpauseContainerCmd(c.getContainerId).exec()
    ()
  }

  def isPaused(name: String): Boolean = {
    val c = container(name)
    c.getDockerClient.inspectContainerCmd(c.getContainerId).exec().getState.getPaused
  }

  /** The last lines of the container's output, to explain a failure. */
  def logs(name: String): String = container(name).getLogs
}

/** The platform shared by every scenario of the run: it takes minutes to start, so it starts once, on first use. */
object PlatformStack {
  private var stack: Option[PlatformStack] = None
  private var failure: Option[Throwable]    = None

  def get: PlatformStack = synchronized {
    failure.foreach(e => throw new IllegalStateException("The platform failed to start earlier in this run", e))
    stack.getOrElse {
      val started = new PlatformStack
      stack = Some(started) // registered before the start, so that a failed start still gets cleaned up
      Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown(), "platform-stack-shutdown"))
      try started.start()
      catch {
        case e: Throwable =>
          failure = Some(e) // every later scenario fails at once, instead of waiting for another start
          shutdown()
          throw e
      }
      started
    }
  }

  def shutdown(): Unit = synchronized {
    stack.foreach(_.stopAll())
    stack = None
  }
}
