package me.bataillec.portfolio.customer.domain

import java.time.Instant
import java.util.UUID

final case class Subscription(
    id: UUID,
    customerId: UUID,
    planType: PlanType,
    status: SubscriptionStatus,
    startDate: Instant,
    endDate: Option[Instant]
)
