package me.bataillec.portfolio.customer.customer

import me.bataillec.portfolio.customer.domain.Customer

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.{Base64, UUID}

object CustomerCursor {
  // Opaque cursor: base64url("<createdAt>|<id>") of the last item of the previous page.
  def encodeCursor(c: Customer): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(s"${c.createdAt}|${c.id}".getBytes(UTF_8))

  def decodeCursor(cursor: String): (Instant, UUID) = {
    val Array(createdAt, id) = new String(Base64.getUrlDecoder.decode(cursor), UTF_8).split('|')
    (Instant.parse(createdAt), UUID.fromString(id))
  }
}
