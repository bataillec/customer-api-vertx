package me.bataillec.portfolio.customer.domain

import java.util.UUID
import scala.util.control.NoStackTrace

sealed abstract class DomainError(message: String, val status: Int, val code: String, val title: String) extends Exception(message) with NoStackTrace

object DomainError {
  final case class CustomerNotFound(id: UUID) extends DomainError(s"Customer '$id' does not exist", 404, "CUSTOMER_NOT_FOUND", "Customer not found")

  final case class EmailAlreadyUsed(email: String)
      extends DomainError(s"A customer with email '$email' already exists", 409, "EMAIL_ALREADY_USED", "Email already used")

  final case class QuotaExceeded(featureKey: String, retryAfterSeconds: Long)
      extends DomainError(s"Quota for '$featureKey' is exhausted. Retry in $retryAfterSeconds seconds", 429, "QUOTA_EXCEEDED", "Quota exceeded")

  final case class EntitlementNotFound(customerId: UUID, featureHey: String)
      extends DomainError(s"Customer '$customerId' has no entitlement '$featureHey'", 404, "ENTITLEMENT_NOT_FOUND", "Entitlement not found")
}
