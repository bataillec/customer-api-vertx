package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.time.Instant
import java.util.UUID

final case class Subscription(
    id: UUID,
    customerId: UUID,
    planType: PlanType,
    status: SubscriptionStatus,
    startDate: Instant,
    endDate: Option[Instant]
) extends WithHttpResponse

object Subscription {
  def fromRow(r: Row): Subscription = {
    Subscription(
      id = r.getUUID("id"),
      customerId = r.getUUID("customer_id"),
      planType = PlanType.valueOf(r.getString("plan_type")),
      status = SubscriptionStatus.valueOf(r.getString("status")),
      startDate = r.getOffsetDateTime("start_date").toInstant,
      endDate = Option(r.getOffsetDateTime("end_date")).map(_.toInstant)
    )
  }
}
