package me.bataillec.portfolio.customer.customer

import io.vertx.core.Future
import me.bataillec.portfolio.customer
import me.bataillec.portfolio.customer.customer.CustomerCursor.*
import me.bataillec.portfolio.customer.domain
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.audit.Outbox
import org.slf4j.LoggerFactory

import java.time.{Clock, Duration}
import java.util.UUID
import scala.util.{Failure, Success, Try}

class CustomerOperations(storage: Storage, outbox: Outbox, clock: Clock = Clock.systemUTC()) extends CustomerOps {

  private type Outcome = Either[DomainError, ConsumeResult]

  private val logger = LoggerFactory.getLogger(getClass)

  override def ping(): Future[Unit] = storage.ping()

  override def createCustomer(createCustomer: CreateCustomer): Future[Customer] = {
    logger.debug("Creating a new customer: {}", createCustomer)
    storage.inTransaction { tx =>
      for {
        c <- storage.insertCustomer(tx, createCustomer)
        _ <- outbox.record(tx, "CUSTOMER_CREATED", c.id)
      } yield c
    }
  }

  override def listCustomers(limit: Int, maybeCursor: Option[String], maybeStatus: Option[CustomerStatus]): Future[ListCustomers] = {
    logger.debug("Listing customers: limit={}, cursor={}, status={}", limit, maybeCursor, maybeStatus)
    Try(maybeCursor.map(decodeCursor)) match {
      case Failure(_)      => Future.failedFuture(InvalidCursor(maybeCursor.getOrElse("")))
      case Success(cursor) =>
        // Fetch one extra row to know whether another page exists.
        storage.listCustomers(limit + 1, cursor, maybeStatus).map { rows =>
          val items      = rows.take(limit)
          val nextCursor = if (rows.size > limit) items.lastOption.map(encodeCursor) else None
          ListCustomers(items, nextCursor)
        }
    }
  }

  override def getCustomer(customerId: UUID): Future[Customer] = {
    logger.debug("Getting the customer with id = {}", customerId.toString)
    storage.findCustomer(customerId).compose {
      case Some(customer) => Future.succeededFuture(customer)
      case None           => Future.failedFuture(CustomerNotFound(customerId))
    }
  }

  override def updateCustomer(customerId: UUID, updateCustomer: UpdateCustomer): Future[Customer] = {
    logger.debug("Updating the customer {} using {}", customerId, updateCustomer)
    storage.inTransaction { tx =>
      storage.lockCustomerForUpdate(tx, customerId).compose {
        case None    => Future.failedFuture(CustomerNotFound(customerId))
        case Some(s) =>
          val newStatus = updateCustomer.status.getOrElse(s)
          if (s == CustomerStatus.DELETED && newStatus == CustomerStatus.DELETED && updateCustomer.companyName.isDefined) {
            Future.failedFuture(CustomerNotActive(customerId, s))
          } else {
            // The entitlements stay, so that deleting then reactivating a customer can't be used to bypass a quota.
            val cancelling = if (newStatus == CustomerStatus.DELETED) storage.cancelActiveSubscription(tx, customerId) else Future.succeededFuture(())
            cancelling
              .compose(_ => storage.updateCustomer(tx, customerId, updateCustomer))
              .compose(r =>
                outbox
                  .record(
                    tx,
                    "CUSTOMER_UPDATED",
                    customerId,
                    "company_name" -> updateCustomer.companyName.getOrElse("-"),
                    "old_status"   -> s.toString,
                    "new_status"   -> updateCustomer.status.map(_.toString).getOrElse("-")
                  )
                  .map(_ => r)
              )
          }
      }
    }
  }

  override def createSubscription(customerId: UUID, createSubscription: CreateSubscription): Future[SubscriptionWithEntitlements] = {
    logger.debug("Creating a new subscription for the customer id = {} and subscription = {}", customerId, createSubscription)
    storage.inTransaction { tx =>
      storage.lockCustomerForUpdate(tx, customerId).compose {
        case None         => Future.failedFuture(CustomerNotFound(customerId))
        case Some(status) =>
          status match {
            case CustomerStatus.ACTIVE                             =>
              storage.findActivePlan(tx, customerId).compose {
                case Some(plan) if plan == createSubscription.planType => Future.failedFuture(PlanAlreadyActive(customerId, plan))
                case previousPlan                                      =>
                  for {
                    _            <- storage.cancelActiveSubscription(tx, customerId)
                    subscription <- storage.insertActiveSubscription(tx, customerId, createSubscription)
                    now          = clock.instant()
                    // Same view as GET /entitlements: a period that has already ended starts anew
                    entitlements <- storage.applyPlan(tx, customerId, createSubscription, now).map(_.map(_.effective(now)))
                    _            <- outbox.record(
                                      tx,
                                      "SUBSCRIPTION_CHANGED",
                                      customerId,
                                      ("plan_type" -> createSubscription.planType.toString) +: previousPlan.map(p => "previous_plan_type" -> p.toString).toSeq*
                                    )
                  } yield {
                    SubscriptionWithEntitlements.fromSubscriptionAndEntitlements(subscription, entitlements)
                  }
              }
            case CustomerStatus.SUSPENDED | CustomerStatus.DELETED =>
              Future.failedFuture(CustomerNotActive(customerId, status))
          }
      }
    }
  }

  override def listPlans(): Future[List[Plan]] = {
    logger.debug("Listing the configured plans")
    storage.listPlans()
  }

  override def listSubscriptions(customerId: UUID): Future[List[Subscription]] = {
    logger.debug("Listing the subscriptions for customer with id = {}", customerId)
    storage.findCustomer(customerId).compose {
      case None    => Future.failedFuture(CustomerNotFound(customerId))
      case Some(_) => storage.listSubscriptions(customerId)
    }
  }

  override def listEntitlements(customerId: UUID): Future[List[Entitlement]] = {
    logger.debug("Listing entitlements for customer with id = {}", customerId)
    storage.findCustomer(customerId).compose {
      case None    => Future.failedFuture(CustomerNotFound(customerId))
      case Some(_) =>
        val now = clock.instant()
        storage.listEntitlements(customerId).map(_.map(_.effective(now)))
    }
  }

  override def consumeEntitlement(customerId: UUID, consumption: Consumption): Future[ConsumeResult] = {
    logger.debug("Consuming for customer with id = {}, consumption = {}", customerId, consumption)
    // A refused consume must still commit its QUOTA_EXCEEDED event: the refusal leaves the transaction as a value, and
    // only becomes a failure once the transaction has committed (a failed future would roll it back).
    storage
      .inTransaction[Outcome] { tx =>
        storage.lockCustomerForUpdate(tx, customerId).compose {
          case None         => Future.failedFuture(CustomerNotFound(customerId))
          case Some(status) =>
            status match {
              case CustomerStatus.ACTIVE                             =>
                val now = clock.instant()
                // Hot path: one guarded UPDATE (it also requires an active subscription); the lookups only explain a miss
                storage.consume(tx, customerId, consumption, now).compose {
                  case Some(result) => Future.succeededFuture[Outcome](Right(result))
                  case None         =>
                    storage.findEntitlement(tx, customerId, consumption.featureKey).compose {
                      case None              => Future.failedFuture(EntitlementNotFound(customerId, consumption.featureKey))
                      case Some(entitlement) =>
                        storage.findActivePlan(tx, customerId).compose {
                          case None    => Future.failedFuture(NoActiveSubscription(customerId))
                          case Some(_) =>
                            // Waiting never helps when the request is larger than the whole allocation
                            val retryAfter =
                              if consumption.amount > entitlement.allocatedQuota then None
                              else entitlement.effective(now).resetDate.map(Duration.between(now, _).getSeconds.max(1L))
                            outbox
                              .record(tx, "QUOTA_EXCEEDED", customerId, "feature_key" -> consumption.featureKey)
                              .map[Outcome](_ => Left(QuotaExceeded(consumption.featureKey, retryAfter)))
                        }
                    }
                }
              case CustomerStatus.SUSPENDED | CustomerStatus.DELETED =>
                Future.failedFuture(CustomerNotActive(customerId, status))
            }
        }
      }
      .compose {
        case Right(result) => Future.succeededFuture(result)
        case Left(refusal) => Future.failedFuture(refusal)
      }
  }
}
