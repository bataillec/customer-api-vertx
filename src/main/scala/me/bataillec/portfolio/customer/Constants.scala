package me.bataillec.portfolio.customer

object Constants {
  val OPERATION_ID = "operationId"

  val OPENAPI_PATH = "openapi.yaml"

  val CONTENT_TYPE         = "Content-Type"
  val DEFAULT_CONTENT_TYPE = "application/json"
  val DEFAULT_BODY         = ""
  val DEFAULT_STATUS_CODE  = 200
  val LIMIT_DEFAULT_VALUE  = "50"

  val RETRY_AFTER_HEADER     = "Retry-After"
  val IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
  val IF_MATCH_HEADER        = "If-Match"

  val ID_PATH_PARAM = "id"

  val LIMIT_QUERY_PARAM  = "limit"
  val CURSOR_QUERY_PARAM = "cursor"
  val STATUS_QUERY_PARAM = "status"

  // region Env parameters

  val HTTP_PORT               = "HTTP_PORT"
  val HTTP_PORT_DEFAULT_VALUE = 8080
  val AUDIT_FILE_PATH         = "AUDIT_FILE_PATH"
  val DEFAULT_AUDIT_FILE_PATH = "./audit.log"

  // endregion

  // region JSON Fields

  val STATUS_CODE_FIELD         = "statusCode"
  val BODY_FIELD                = "body"
  val CONTENT_TYPE_FIELD        = "contentType"
  val HEADERS_FIELD             = "headers"
  val CODE_FIELD                = "code"
  val ID_FIELD                  = "id"
  val EMAIL_FIELD               = "email"
  val FEATURE_KEY_FIELD         = "featureKey"
  val RETRY_AFTER_SECONDS_FIELD = "retryAfterSeconds"
  val TITLE_FIELD               = "title"
  val DETAIL_FIELD              = "detail"
  val TRACEPARENT_FIELD         = "traceparent"
  val TRACE_ID_FIELD            = "trace_id"

  // endregion

  // region Error codes

  val CUSTOMER_NOT_FOUND    = "CUSTOMER_NOT_FOUND"
  val EMAIL_ALREADY_USED    = "EMAIL_ALREADY_USED"
  val QUOTA_EXCEEDED        = "QUOTA_EXCEEDED"
  val ENTITLEMENT_NOT_FOUND = "ENTITLEMENT_NOT_FOUND"
  val UNKOWN_ERROR          = "UNKOWN_ERROR"
  val TIMEOUT_COMMUNICATION = "TIMEOUT_COMMUNICATION"
  val NO_HANDLERS           = "NO_HANDLERS"
  val RECIPIENT_FAILURE     = "RECIPIENT_FAILURE"
  val VALIDATION_FAILED     = "VALIDATION_FAILED"
  val HTTP_ERROR            = "HTTP_ERROR"
  val INTERNAL_ERROR        = "INTERNAL_ERROR"
  val NOT_FOUND             = "NOT_FOUND"

  // endregion

  // region Error titles

  val UUID_NOT_FOUND                                = "UUID not found"
  val MALFORMED_UUID                                = "Malformed UUID"
  val EMAIL_NOT_FOUND                               = "Email not found"
  val FEATURE_KEY_NOT_FOUND                         = "FeatureKey not found"
  val RETRY_AFTER_SECONDS_NOT_FOUND                 = "RetryAfterSeconds not found"
  val FEATURE_KEY_AND_RETRY_AFTER_SECONDS_NOT_FOUND = "FeatureKey and RetryAfterSeconds not found"
  val ID_AND_FEATURE_KEY_NOT_FOUND                  = "Id and FeatureKey not found"
  val UNKNOWN_ERROR                                 = "Unknown error"
  val TIMEOUT_COMMUNICATION_BETWEEN_VERTICLES       = "Timeout communication between verticles"
  val COMMUNICATION_PROBLEM_BETWEEN_VERTICLES       = "Communication problem between verticles"
  val MESSAGES_REJECTED_BY_VERTICLES                = "Messages rejected by verticles"
  val INVALID_REQUEST                               = "Invalid request"
  val REQUEST_FAILED                                = "Request failed"
  val INTERNAL_SERVER_ERROR                         = "Internal server error"

  // endregion

  val HEALTH_CHECK_TIMEOUT    = 10000     // HealthChecks procedure
  val HEALTH_PING_TIMEOUT_MS  = 4000      // http -> customer/audit
  val HEALTH_AUDIT_TIMEOUT_MS = 2000      // customer -> audit
  val BODY_LIMIT: Int         = 64 * 1024 // in bytes

  // region Eventbus addresses

  val CUSTOMER_ADDRESS        = "customer"
  val AUDIT_ADDRESS           = "audit"
  val CUSTOMER_HEALTH_ADDRESS = s"$CUSTOMER_ADDRESS.health"
  val AUDIT_HEALTH_ADDRESS    = s"$AUDIT_ADDRESS.health"

  // endregion

  val HTTPSERVER_VERTICLE = "httpserver-verticle"
  val CUSTOMER_VERTICLE   = "customer-verticle"
  val AUDIT_VERTICLE      = "audit-verticle"

  // region Route / Operation IDs

  val createCustomer_ID     = "createCustomer"
  val listSubscriptions_ID  = "listSubscriptions"
  val updateCustomer_ID     = "updateCustomer"
  val listEntitlements_ID   = "listEntitlements"
  val subscribe_ID          = "subscribe"
  val listCustomers_ID      = "listCustomers"
  val consumeEntitlement_ID = "consumeEntitlement"
  val getCustomer_ID        = "getCustomer"
  val live_ID               = "live"
  val ready_ID              = "ready"

  // endregion
}
