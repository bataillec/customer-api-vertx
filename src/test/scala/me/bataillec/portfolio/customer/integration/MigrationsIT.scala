package me.bataillec.portfolio.customer.integration

import me.bataillec.portfolio.customer.support.ContainersSupport
import org.flywaydb.core.Flyway
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.sql.{Connection, SQLException}
import scala.collection.mutable.ListBuffer

/** Runs the real Flyway migrations on a real Postgres, which the unit specs cannot do. */
class MigrationsIT extends AnyWordSpec with Matchers {

  private def flyway(database: String, target: Option[String] = None): Flyway = {
    val config = Flyway
      .configure()
      .dataSource(ContainersSupport.jdbcUrl(database), ContainersSupport.pgUser, ContainersSupport.pgPassword)
    target.foreach(config.target)
    config.load()
  }

  private def withConnection[T](database: String)(f: Connection => T): T = {
    val conn = ContainersSupport.connection(database)
    try f(conn)
    finally conn.close()
  }

  private def query[T](conn: Connection, sql: String)(read: java.sql.ResultSet => T): List[T] = {
    val rs  = conn.createStatement().executeQuery(sql)
    val out = ListBuffer.empty[T]
    while (rs.next()) out += read(rs)
    out.toList
  }

  private def update(conn: Connection, sql: String): Unit = {
    conn.createStatement().execute(sql)
    ()
  }

  "the migrations" should {
    "apply V1 to V6 on an empty database" in {
      val db     = ContainersSupport.newDatabase("migrate")
      val result = flyway(db).migrate()

      result.success shouldBe true
      result.migrationsExecuted shouldBe 6

      withConnection(db) { conn =>
        val tables = query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")(
          _.getString(1)
        )
        tables should contain allOf ("customers", "subscriptions", "entitlements", "plan_quotas")
      }
    }

    "seed the quotas of every plan, STORAGE_GB being the only one that never resets" in {
      val db = ContainersSupport.newDatabase("seed")
      flyway(db).migrate()

      withConnection(db) { conn =>
        val rows = query(conn, "SELECT plan_type::text, feature_key, quota, resets_monthly FROM plan_quotas") { rs =>
          (rs.getString(1), rs.getString(2), rs.getLong(3), rs.getBoolean(4))
        }
        rows should contain theSameElementsAs List(
          ("FREE_TIER", "API_CALLS_MONTHLY", 1000L, true),
          ("FREE_TIER", "STORAGE_GB", 1L, false),
          ("PRO", "API_CALLS_MONTHLY", 100000L, true),
          ("PRO", "STORAGE_GB", 50L, false),
          ("ENTERPRISE", "API_CALLS_MONTHLY", 10000000L, true),
          ("ENTERPRISE", "STORAGE_GB", 1000L, false)
        )
      }
    }

    "make the email unique whatever its case" in {
      val db = ContainersSupport.newDatabase("email")
      flyway(db).migrate()

      withConnection(db) { conn =>
        update(conn, "INSERT INTO customers (email) VALUES ('Jane@Example.com')")

        val error = intercept[SQLException] {
          update(conn, "INSERT INTO customers (email) VALUES ('jane@example.COM')")
        }
        error.getSQLState shouldBe "23505" // unique_violation
      }
    }

    "create the index of the customers pagination" in {
      val db = ContainersSupport.newDatabase("index")
      flyway(db).migrate()

      withConnection(db) { conn =>
        val indexes = query(conn, "SELECT indexname FROM pg_indexes WHERE tablename = 'customers'")(_.getString(1))
        indexes should contain("customers_created_at_id_idx")
        indexes should contain("customers_email_lower_key")
      }
    }

    "cancel the active subscriptions of the customers deleted before V4" in {
      val db = ContainersSupport.newDatabase("v4")
      flyway(db, target = Some("3")).migrate()

      withConnection(db) { conn =>
        update(
          conn,
          """INSERT INTO customers (id, email, status) VALUES
            |  ('00000000-0000-0000-0000-000000000001', 'deleted@example.com', 'DELETED'),
            |  ('00000000-0000-0000-0000-000000000002', 'active@example.com', 'ACTIVE')""".stripMargin
        )
        update(
          conn,
          """INSERT INTO subscriptions (customer_id, plan_type) VALUES
            |  ('00000000-0000-0000-0000-000000000001', 'PRO'),
            |  ('00000000-0000-0000-0000-000000000002', 'PRO')""".stripMargin
        )
      }

      flyway(db).migrate().migrationsExecuted shouldBe 3

      withConnection(db) { conn =>
        val statuses = query(
          conn,
          "SELECT c.email, s.status::text, s.end_date IS NOT NULL FROM subscriptions s JOIN customers c ON c.id = s.customer_id"
        )(rs => (rs.getString(1), rs.getString(2), rs.getBoolean(3))).toSet

        statuses shouldBe Set(("deleted@example.com", "CANCELED", true), ("active@example.com", "ACTIVE", false))
      }
    }

    "stop the usage of STORAGE_GB from resetting, by clearing the reset date of existing entitlements (V3)" in {
      val db = ContainersSupport.newDatabase("v3")
      flyway(db, target = Some("2")).migrate()

      withConnection(db) { conn =>
        update(conn, "INSERT INTO customers (id, email) VALUES ('00000000-0000-0000-0000-000000000003', 'c@example.com')")
        update(
          conn,
          """INSERT INTO entitlements (customer_id, feature_key, allocated_quota, reset_date) VALUES
            |  ('00000000-0000-0000-0000-000000000003', 'STORAGE_GB', 1, now()),
            |  ('00000000-0000-0000-0000-000000000003', 'API_CALLS_MONTHLY', 1000, now())""".stripMargin
        )
      }

      flyway(db).migrate()

      withConnection(db) { conn =>
        val resets = query(conn, "SELECT feature_key, reset_date IS NULL FROM entitlements")(rs =>
          (rs.getString(1), rs.getBoolean(2))
        ).toMap
        resets shouldBe Map("STORAGE_GB" -> true, "API_CALLS_MONTHLY" -> false)
      }
    }

    "be idempotent when run again" in {
      val db = ContainersSupport.newDatabase("again")
      flyway(db).migrate()

      flyway(db).migrate().migrationsExecuted shouldBe 0
    }
  }
}
