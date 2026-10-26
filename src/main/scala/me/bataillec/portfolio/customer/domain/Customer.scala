package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.time.Instant
import java.util.UUID

final case class Customer(
    id: UUID,
    email: String,
    companyName: Option[String],
    status: CustomerStatus,
    createdAt: Instant,
    updatedAt: Instant
) extends WithHttpResponse

object Customer {
  def fromRow(r: Row): Customer = {
    Customer(
      id = r.getUUID("id"),
      email = r.getString("email"),
      companyName = Option(r.getString("company_name")),
      status = CustomerStatus.valueOf(r.getString("status")),
      createdAt = r.getOffsetDateTime("created_at").toInstant,
      updatedAt = r.getOffsetDateTime("updated_at").toInstant
    )
  }
}
