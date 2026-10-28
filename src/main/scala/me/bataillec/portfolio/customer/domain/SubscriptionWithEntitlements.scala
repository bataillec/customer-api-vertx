package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.time.Instant
import java.util.UUID

final case class SubscriptionWithEntitlements(
    id: UUID,
    customerId: UUID,
    planType: PlanType,
    status: SubscriptionStatus,
    startDate: Instant,
    endDate: Option[Instant],
    entitlements: List[Entitlement]
) extends WithHttpResponse

object SubscriptionWithEntitlements {
  def fromSubscriptionAndEntitlements(subscription: Subscription, entitlements: List[Entitlement]): SubscriptionWithEntitlements = {
    SubscriptionWithEntitlements(
      id = subscription.id,
      customerId = subscription.customerId,
      planType = subscription.planType,
      status = subscription.status,
      startDate = subscription.startDate,
      endDate = subscription.endDate,
      entitlements = entitlements
    )
  }
}
