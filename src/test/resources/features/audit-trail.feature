Feature: The audit trail
  Every change to a customer is recorded as an event, so that other systems and people can follow what happened.
  The events of one customer are always kept in the order the changes were made.

  Scenario: The changes of a customer are recorded in order
    Given a registered customer
    And the customer subscribes to the "PRO" plan
    When the company name of the customer is changed to "Acme"
    Then the audit trail of the customer shows, in this order:
      | CUSTOMER_CREATED     |
      | SUBSCRIPTION_CHANGED |
      | CUSTOMER_UPDATED     |

  Scenario: The events of a customer are grouped under the customer
    Given a registered customer
    Then every audit event of the customer is keyed by the customer

  Scenario: A successful consumption is recorded
    Given a registered customer
    And the customer subscribes to the "FREE_TIER" plan
    When the customer consumes 10 of "API_CALLS_MONTHLY"
    Then the audit trail of the customer shows, in this order:
      | CUSTOMER_CREATED     |
      | SUBSCRIPTION_CHANGED |
      | QUOTA_CONSUMED       |

  Scenario: A refused consumption is recorded as a refusal only
    Given a registered customer
    And the customer subscribes to the "FREE_TIER" plan
    And the customer has already consumed 1 of "STORAGE_GB"
    When the customer consumes 1 of "STORAGE_GB"
    Then the audit trail of the customer shows, in this order:
      | CUSTOMER_CREATED     |
      | SUBSCRIPTION_CHANGED |
      | QUOTA_CONSUMED       |
      | QUOTA_EXCEEDED       |
