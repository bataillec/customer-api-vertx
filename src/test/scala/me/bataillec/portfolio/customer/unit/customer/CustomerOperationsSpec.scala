package me.bataillec.portfolio.customer.unit.customer

import io.vertx.core.Future
import io.vertx.sqlclient.SqlClient
import me.bataillec.portfolio.customer.customer.CustomerCursor.encodeCursor
import me.bataillec.portfolio.customer.customer.{CustomerOperations, Storage}
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.audit.Outbox
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneOffset}
import java.util.UUID
import scala.collection.mutable.ListBuffer

class CustomerOperationsSpec extends AnyWordSpec with Matchers {
  private val id      = UUID.fromString("11111111-2222-3333-4444-555555555555")
  private val subId   = UUID.fromString("66666666-7777-8888-9999-000000000000")
  private val now     = Instant.parse("2026-01-15T10:00:00Z")
  private val created = Instant.parse("2026-01-02T03:04:05Z")

  private def customerOf(n: Int): Customer =
    Customer(UUID.fromString(f"00000000-0000-0000-0000-$n%012d"), s"c$n@b.c", None, CustomerStatus.ACTIVE, created.plusSeconds(n), created)

  private val customer     = Customer(id, "a@b.c", Some("ACME"), CustomerStatus.ACTIVE, created, created)
  private val subscription = Subscription(subId, id, PlanType.PRO, SubscriptionStatus.ACTIVE, created, None)
  private val entitlement  = Entitlement("api_calls", 100, 10, None)
  private val boom         = new RuntimeException("boom")

  // region Helpers

  private def ok[T](value: T): Future[T] = Future.succeededFuture[T](value)

  private def ko[T](error: Throwable): Future[T]     = Future.failedFuture[T](error)
  private def locked(status: Option[CustomerStatus]) = ok[Option[CustomerStatus]](status)

  /** Keeps what is recorded, with the transaction it was recorded on. A failure can be armed to prove the rollback. */
  private class RecordingOutbox extends Outbox {
    val recorded: ListBuffer[(String, UUID, List[(String, String | Long)])] = ListBuffer.empty
    val transactions: ListBuffer[SqlClient]                                = ListBuffer.empty
    var failure: Option[Throwable]                                         = None

    override def record(tx: SqlClient, eventType: String, customerId: UUID, extra: (String, String | Long)*): Future[Unit] = {
      transactions += tx
      recorded += ((eventType, customerId, extra.toList))
      failure.fold(Future.succeededFuture(()))(e => Future.failedFuture[Unit](e))
    }
  }

  private class Fixture(clock: Clock = Clock.fixed(now, ZoneOffset.UTC)) {
    val storage: Storage        = mock(classOf[Storage])
    val audit                   = new RecordingOutbox
    val tx: SqlClient           = mock(classOf[SqlClient])
    val ops: CustomerOperations = new CustomerOperations(storage, audit, clock)

    when(storage.inTransaction[Any](any[SqlClient => Future[Any]]())).thenAnswer { invocation =>
      invocation.getArgument[SqlClient => Future[Any]](0)(tx)
    }

    def lock(status: Option[CustomerStatus]): Unit =
      when(storage.lockCustomerForUpdate(tx, id)).thenReturn(locked(status))

    def activePlan(plan: Option[PlanType]): Unit =
      when(storage.findActivePlan(tx, id)).thenReturn(ok[Option[PlanType]](plan))
  }

  // endregion

  "the default clock" should {
    "be the system clock" in {
      val storage = mock(classOf[Storage])
      when(storage.listEntitlements(id)).thenReturn(ok(List(Entitlement("f", 10, 5, Some(Instant.EPOCH)))))
      when(storage.findCustomer(id)).thenReturn(ok[Option[Customer]](Some(customer)))

      val result = await(new CustomerOperations(storage, new RecordingOutbox).listEntitlements(id))

      result.head.consumedQuota shouldBe 0
      result.head.resetDate.get.isAfter(Instant.now()) shouldBe true
    }
  }

  "ping" should {
    "delegate to the storage" in {
      val f = new Fixture
      when(f.storage.ping()).thenReturn(ok(()))

      await(f.ops.ping()) shouldBe ()
      verify(f.storage).ping()
    }

    "propagate the failure" in {
      val f = new Fixture
      when(f.storage.ping()).thenReturn(ko[Unit](boom))

      failureOf(f.ops.ping()) shouldBe boom
    }
  }

  "createCustomer" should {
    "store the customer and send a CUSTOMER_CREATED audit" in {
      val f = new Fixture
      when(f.storage.insertCustomer(f.tx, CreateCustomer("a@b.c", Some("ACME")))).thenReturn(ok(customer))

      await(f.ops.createCustomer(CreateCustomer("a@b.c", Some("ACME")))) shouldBe customer

      f.audit.recorded.toList shouldBe List(("CUSTOMER_CREATED", id, List()))
    }

    "record the event on the transaction of the insert, so that both commit or roll back together" in {
      val f = new Fixture
      when(f.storage.insertCustomer(f.tx, CreateCustomer("a@b.c", Some("ACME")))).thenReturn(ok(customer))

      await(f.ops.createCustomer(CreateCustomer("a@b.c", Some("ACME"))))

      f.audit.transactions.toList shouldBe List(f.tx)
    }

    "fail, so that the transaction rolls back, when the event cannot be recorded" in {
      val f = new Fixture
      f.audit.failure = Some(boom)
      when(f.storage.insertCustomer(f.tx, CreateCustomer("a@b.c", Some("ACME")))).thenReturn(ok(customer))

      failureOf(f.ops.createCustomer(CreateCustomer("a@b.c", Some("ACME")))) shouldBe boom
    }

    "propagate the storage failure and not audit" in {
      val f     = new Fixture
      val error = EmailAlreadyUsed("a@b.c")
      when(f.storage.insertCustomer(any[SqlClient](), any[CreateCustomer]())).thenReturn(ko[Customer](error))

      failureOf(f.ops.createCustomer(CreateCustomer("a@b.c", None))) shouldBe error

      f.audit.recorded shouldBe empty
    }
  }

  "listCustomers" should {
    "ask for one more row than the limit and decode the cursor" in {
      val f      = new Fixture
      val cursor = encodeCursor(customer)
      when(f.storage.listCustomers(3, Some((created, id)), Some(CustomerStatus.ACTIVE))).thenReturn(ok(List.empty[Customer]))

      await(f.ops.listCustomers(2, Some(cursor), Some(CustomerStatus.ACTIVE))) shouldBe ListCustomers(Nil, None)

      verify(f.storage).listCustomers(3, Some((created, id)), Some(CustomerStatus.ACTIVE))
    }

    "trim the extra row and return a next cursor when another page exists" in {
      val f    = new Fixture
      val rows = List(customerOf(3), customerOf(2), customerOf(1))
      when(f.storage.listCustomers(2, None, None)).thenReturn(ok(rows))

      val result = await(f.ops.listCustomers(1, None, None))

      result.items shouldBe List(customerOf(3))
      result.nextCursor shouldBe Some(encodeCursor(customerOf(3)))
    }

    "return no next cursor when the page is not full" in {
      val f = new Fixture
      when(f.storage.listCustomers(3, None, None)).thenReturn(ok(List(customerOf(1))))

      await(f.ops.listCustomers(2, None, None)) shouldBe ListCustomers(List(customerOf(1)), None)
    }

    "return no next cursor when the page is exactly full" in {
      val f = new Fixture
      when(f.storage.listCustomers(3, None, None)).thenReturn(ok(List(customerOf(2), customerOf(1))))

      await(f.ops.listCustomers(2, None, None)).nextCursor shouldBe None
    }

    "fail with InvalidCursor when the cursor is not base64" in {
      val f = new Fixture

      failureOf(f.ops.listCustomers(2, Some("!!!"), None)) shouldBe InvalidCursor("!!!")

      verify(f.storage, never()).listCustomers(anyInt(), any[Option[(Instant, UUID)]](), any[Option[CustomerStatus]]())
    }

    "fail with InvalidCursor when the cursor has no separator" in {
      val f      = new Fixture
      val cursor = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString("nothing".getBytes("UTF-8"))

      failureOf(f.ops.listCustomers(2, Some(cursor), None)) shouldBe InvalidCursor(cursor)
    }

    "propagate the storage failure" in {
      val f = new Fixture
      when(f.storage.listCustomers(anyInt(), any[Option[(Instant, UUID)]](), any[Option[CustomerStatus]]())).thenReturn(ko[List[Customer]](boom))

      failureOf(f.ops.listCustomers(2, None, None)) shouldBe boom
    }
  }

  "getCustomer" should {
    "return the customer" in {
      val f = new Fixture
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](Some(customer)))

      await(f.ops.getCustomer(id)) shouldBe customer
    }

    "fail with CustomerNotFound" in {
      val f = new Fixture
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](None))

      failureOf(f.ops.getCustomer(id)) shouldBe CustomerNotFound(id)
    }
  }

  "updateCustomer" should {
    def stubUpdate(f: Fixture, update: UpdateCustomer): Unit = {
      when(f.storage.cancelActiveSubscription(f.tx, id)).thenReturn(ok(()))
      when(f.storage.updateCustomer(f.tx, id, update)).thenReturn(ok(customer))
    }

    "fail with CustomerNotFound when the customer does not exist" in {
      val f = new Fixture
      f.lock(None)

      failureOf(f.ops.updateCustomer(id, UpdateCustomer(None, None))) shouldBe CustomerNotFound(id)

      f.audit.recorded shouldBe empty
    }

    "refuse to rename a deleted customer that stays deleted (no status)" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.DELETED))

      failureOf(f.ops.updateCustomer(id, UpdateCustomer(Some("New"), None))) shouldBe CustomerNotActive(id, CustomerStatus.DELETED)

      verify(f.storage, never()).updateCustomer(any[SqlClient](), any[UUID](), any[UpdateCustomer]())
    }

    "refuse to rename a deleted customer that stays deleted (explicit status)" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.DELETED))

      failureOf(f.ops.updateCustomer(id, UpdateCustomer(Some("New"), Some(CustomerStatus.DELETED)))) shouldBe CustomerNotActive(id, CustomerStatus.DELETED)
    }

    "cancel the active subscription when the status becomes DELETED, and audit it" in {
      val f      = new Fixture
      val update = UpdateCustomer(Some("New"), Some(CustomerStatus.DELETED))
      f.lock(Some(CustomerStatus.ACTIVE))
      stubUpdate(f, update)

      await(f.ops.updateCustomer(id, update)) shouldBe customer

      verify(f.storage).cancelActiveSubscription(f.tx, id)
      f.audit.recorded.toList shouldBe List(("CUSTOMER_UPDATED", id, List("company_name" -> "New", "old_status" -> "ACTIVE", "new_status" -> "DELETED")))
    }

    "cancel the subscription of an already deleted customer that only gets deleted again without a name" in {
      val f      = new Fixture
      val update = UpdateCustomer(None, None)
      f.lock(Some(CustomerStatus.DELETED))
      stubUpdate(f, update)

      await(f.ops.updateCustomer(id, update)) shouldBe customer

      verify(f.storage).cancelActiveSubscription(f.tx, id)
      f.audit.recorded.toList shouldBe List(("CUSTOMER_UPDATED", id, List("company_name" -> "-", "old_status" -> "DELETED", "new_status" -> "-")))
    }

    "reactivate a deleted customer without cancelling anything" in {
      val f      = new Fixture
      val update = UpdateCustomer(Some("Back"), Some(CustomerStatus.ACTIVE))
      f.lock(Some(CustomerStatus.DELETED))
      stubUpdate(f, update)

      await(f.ops.updateCustomer(id, update)) shouldBe customer

      verify(f.storage, never()).cancelActiveSubscription(any[SqlClient](), any[UUID]())
      f.audit.recorded.toList shouldBe List(("CUSTOMER_UPDATED", id, List("company_name" -> "Back", "old_status" -> "DELETED", "new_status" -> "ACTIVE")))
    }

    "not cancel anything when an active customer is suspended" in {
      val f      = new Fixture
      val update = UpdateCustomer(None, Some(CustomerStatus.SUSPENDED))
      f.lock(Some(CustomerStatus.ACTIVE))
      stubUpdate(f, update)

      await(f.ops.updateCustomer(id, update))

      verify(f.storage, never()).cancelActiveSubscription(any[SqlClient](), any[UUID]())
    }

    "propagate a storage failure and not audit" in {
      val f      = new Fixture
      val update = UpdateCustomer(Some("New"), None)
      f.lock(Some(CustomerStatus.ACTIVE))
      when(f.storage.updateCustomer(f.tx, id, update)).thenReturn(ko[Customer](boom))

      failureOf(f.ops.updateCustomer(id, update)) shouldBe boom

      f.audit.recorded shouldBe empty
    }
  }

  "createSubscription" should {
    val create = CreateSubscription(PlanType.PRO)

    def stubChange(f: Fixture): Unit = {
      when(f.storage.cancelActiveSubscription(f.tx, id)).thenReturn(ok(()))
      when(f.storage.insertActiveSubscription(f.tx, id, create)).thenReturn(ok(subscription))
      when(f.storage.applyPlan(f.tx, id, create, now)).thenReturn(ok(List(entitlement)))
    }

    "fail with CustomerNotFound" in {
      val f = new Fixture
      f.lock(None)

      failureOf(f.ops.createSubscription(id, create)) shouldBe CustomerNotFound(id)
    }

    "refuse a suspended customer" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.SUSPENDED))

      failureOf(f.ops.createSubscription(id, create)) shouldBe CustomerNotActive(id, CustomerStatus.SUSPENDED)
    }

    "refuse a deleted customer" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.DELETED))

      failureOf(f.ops.createSubscription(id, create)) shouldBe CustomerNotActive(id, CustomerStatus.DELETED)
    }

    "refuse the plan the customer is already on" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.ACTIVE))
      f.activePlan(Some(PlanType.PRO))

      failureOf(f.ops.createSubscription(id, create)) shouldBe PlanAlreadyActive(id, PlanType.PRO)

      verify(f.storage, never()).insertActiveSubscription(any[SqlClient](), any[UUID](), any[CreateSubscription]())
    }

    "subscribe a customer that has no plan yet" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.ACTIVE))
      f.activePlan(None)
      stubChange(f)

      await(f.ops.createSubscription(id, create)) shouldBe SubscriptionWithEntitlements.fromSubscriptionAndEntitlements(subscription, List(entitlement))

      f.audit.recorded.toList shouldBe List(("SUBSCRIPTION_CHANGED", id, List("plan_type" -> "PRO")))
    }

    "change the plan in order and audit the previous plan" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.ACTIVE))
      f.activePlan(Some(PlanType.FREE_TIER))
      stubChange(f)

      await(f.ops.createSubscription(id, create)).entitlements shouldBe List(entitlement)

      val order = org.mockito.Mockito.inOrder(f.storage)
      order.verify(f.storage).cancelActiveSubscription(f.tx, id)
      order.verify(f.storage).insertActiveSubscription(f.tx, id, create)
      order.verify(f.storage).applyPlan(f.tx, id, create, now)
      f.audit.recorded.toList shouldBe List(("SUBSCRIPTION_CHANGED", id, List("plan_type" -> "PRO", "previous_plan_type" -> "FREE_TIER")))
    }

    "propagate PlanNotConfigured and not audit" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.ACTIVE))
      f.activePlan(None)
      stubChange(f)
      when(f.storage.applyPlan(f.tx, id, create, now)).thenReturn(ko[List[Entitlement]](PlanNotConfigured(PlanType.PRO)))

      failureOf(f.ops.createSubscription(id, create)) shouldBe PlanNotConfigured(PlanType.PRO)

      f.audit.recorded shouldBe empty
    }
  }

  "listPlans" should {
    "delegate to the storage" in {
      val f     = new Fixture
      val plans = List(Plan(PlanType.PRO, scala.collection.immutable.ListMap("f" -> PlanQuota(1, resetsMonthly = true))))
      when(f.storage.listPlans()).thenReturn(ok(plans))

      await(f.ops.listPlans()) shouldBe plans
    }
  }

  "listSubscriptions" should {
    "fail with CustomerNotFound" in {
      val f = new Fixture
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](None))

      failureOf(f.ops.listSubscriptions(id)) shouldBe CustomerNotFound(id)

      verify(f.storage, never()).listSubscriptions(any[UUID]())
    }

    "list the subscriptions of an existing customer" in {
      val f = new Fixture
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](Some(customer)))
      when(f.storage.listSubscriptions(id)).thenReturn(ok(List(subscription)))

      await(f.ops.listSubscriptions(id)) shouldBe List(subscription)
    }
  }

  "listEntitlements" should {
    "fail with CustomerNotFound" in {
      val f = new Fixture
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](None))

      failureOf(f.ops.listEntitlements(id)) shouldBe CustomerNotFound(id)
    }

    "return the effective entitlements as of now" in {
      val f       = new Fixture
      val expired = Entitlement("expired", 10, 7, Some(Instant.parse("2026-01-10T00:00:00Z")))
      val current = Entitlement("current", 10, 7, Some(Instant.parse("2026-02-01T00:00:00Z")))
      when(f.storage.findCustomer(id)).thenReturn(ok[Option[Customer]](Some(customer)))
      when(f.storage.listEntitlements(id)).thenReturn(ok(List(expired, current)))

      await(f.ops.listEntitlements(id)) shouldBe List(Entitlement("expired", 10, 0, Some(Instant.parse("2026-02-01T00:00:00Z"))), current)
    }
  }

  "consumeEntitlement" should {
    val consumption = Consumption("api_calls", 5)
    val result      = ConsumeResult("api_calls", 100, 15, 85)

    def activeCustomer(f: Fixture): Unit = f.lock(Some(CustomerStatus.ACTIVE))

    "fail with CustomerNotFound" in {
      val f = new Fixture
      f.lock(None)

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe CustomerNotFound(id)
    }

    "refuse a suspended customer" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.SUSPENDED))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe CustomerNotActive(id, CustomerStatus.SUSPENDED)
    }

    "refuse a deleted customer" in {
      val f = new Fixture
      f.lock(Some(CustomerStatus.DELETED))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe CustomerNotActive(id, CustomerStatus.DELETED)
    }

    "fail with EntitlementNotFound" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.consume(any(), any(), any(), any())).thenReturn(ok[Option[ConsumeResult]](None))
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](None))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe EntitlementNotFound(id, "api_calls")
    }

    "fail with NoActiveSubscription" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.consume(any(), any(), any(), any())).thenReturn(ok[Option[ConsumeResult]](None))
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](Some(entitlement)))
      f.activePlan(None)

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe NoActiveSubscription(id)
    }

    "return the result of the consumption" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](Some(entitlement)))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](Some(result)))

      await(f.ops.consumeEntitlement(id, consumption)) shouldBe result

      f.audit.recorded shouldBe empty
      // The hot path is the guarded UPDATE alone
      verify(f.storage, never()).findEntitlement(any(), any(), any())
      verify(f.storage, never()).findActivePlan(any(), any())
    }

    "give no retry delay when the request exceeds the whole allocation" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.findEntitlement(f.tx, id, "api_calls"))
        .thenReturn(ok[Option[Entitlement]](Some(entitlement.copy(allocatedQuota = 3, resetDate = Some(now.plusSeconds(600))))))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](None))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe QuotaExceeded("api_calls", None)
    }

    "fail with QuotaExceeded, audit it and tell when to retry" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.findEntitlement(f.tx, id, "api_calls"))
        .thenReturn(ok[Option[Entitlement]](Some(entitlement.copy(resetDate = Some(Instant.parse("2026-01-15T10:01:00Z"))))))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](None))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe QuotaExceeded("api_calls", Some(60L))

      f.audit.recorded.toList shouldBe List(("QUOTA_EXCEEDED", id, List("feature_key" -> "api_calls")))
    }

    "record QUOTA_EXCEEDED on the transaction and refuse only after it, so that the event is committed" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](Some(entitlement)))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](None))
      var committed = false
      doAnswer { invocation =>
        invocation.getArgument[SqlClient => Future[Any]](0)(f.tx).map { result => committed = true; result }
      }.when(f.storage).inTransaction[Any](any[SqlClient => Future[Any]]())

      val refusal = f.ops.consumeEntitlement(id, consumption)

      // The transaction ended with a value, not a failure: a failed future would have rolled the event back
      committed shouldBe true
      f.audit.transactions.toList shouldBe List(f.tx)
      failureOf(refusal) shouldBe QuotaExceeded("api_calls", None)
    }

    "retry in at least one second when the reset is imminent" in {
      val f = new Fixture
      activeCustomer(f)
      // The reset date is still in the future (by 500 ms), so Duration.between(...).getSeconds is 0.
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](Some(entitlement.copy(resetDate = Some(now.plusMillis(500))))))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](None))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe QuotaExceeded("api_calls", Some(1L))
    }

    "give no retry delay when the quota never resets" in {
      val f = new Fixture
      activeCustomer(f)
      when(f.storage.findEntitlement(f.tx, id, "api_calls")).thenReturn(ok[Option[Entitlement]](Some(entitlement)))
      f.activePlan(Some(PlanType.PRO))
      when(f.storage.consume(f.tx, id, consumption, now)).thenReturn(ok[Option[ConsumeResult]](None))

      failureOf(f.ops.consumeEntitlement(id, consumption)) shouldBe QuotaExceeded("api_calls", None)
    }
  }
}
