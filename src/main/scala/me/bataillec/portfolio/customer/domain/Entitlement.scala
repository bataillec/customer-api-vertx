package me.bataillec.portfolio.customer.domain

import java.time.Instant

final case class Entitlement(
    featureKey: String,
    allocatedQuota: Long,
    consumedQuota: Long,
    resetDate: Instant
)
