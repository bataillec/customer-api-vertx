package me.bataillec.portfolio.customer.domain

import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.util.UUID
import scala.util.control.NoStackTrace

sealed abstract class DomainError(message: String, val status: Int, val code: String, val title: String)
    extends Exception(message)
    with NoStackTrace
    with WithHttpResponse

object DomainError {
  final case class UnknownError(msg: String) extends DomainError(s"Unknown error occurred: $msg", 500, "UNKOWN_ERROR", "Unkown error")

  final case class CustomerNotFound(id: UUID) extends DomainError(s"Customer '$id' does not exist", 404, "CUSTOMER_NOT_FOUND", "Customer not found")

  final case class EmailAlreadyUsed(email: String)
      extends DomainError(s"A customer with email '$email' already exists", 409, "EMAIL_ALREADY_USED", "Email already used")

  final case class QuotaExceeded(featureKey: String, retryAfterSeconds: Long)
      extends DomainError(s"Quota for '$featureKey' is exhausted. Retry in $retryAfterSeconds seconds", 429, "QUOTA_EXCEEDED", "Quota exceeded")

  final case class EntitlementNotFound(customerId: UUID, featureKey: String)
      extends DomainError(s"Customer '$customerId' has no entitlement '$featureKey'", 404, "ENTITLEMENT_NOT_FOUND", "Entitlement not found")

  final case class PlanNotConfigured(planType: PlanType)
      extends DomainError(s"No quotas are configured for plan '$planType'", 500, "PLAN_NOT_CONFIGURED", "Plan not configured")
}
