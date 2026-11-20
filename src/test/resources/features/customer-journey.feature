Feature: A customer's journey
  A company registers, chooses a plan, uses what the plan gives it, and may be suspended or removed.
  The plans give, each month, API_CALLS_MONTHLY (1,000 on FREE_TIER and 100,000 on PRO), and a
  STORAGE_GB allowance that never resets (1 GB on FREE_TIER).

  Scenario: A new customer registers
    When a new customer registers
    Then the customer is registered
    And the customer status is "ACTIVE"

  Scenario: Two customers cannot share the same email
    Given a registered customer
    When another customer registers with the same email
    Then the request is refused because the email is already used

  Scenario: Subscribing to a plan gives its allowances
    Given a registered customer
    When the customer subscribes to the "FREE_TIER" plan
    Then the subscription is active
    And the customer is allocated 1000 of "API_CALLS_MONTHLY"
    And the customer is allocated 1 of "STORAGE_GB"

  Scenario: Using the plan reduces what is left
    Given a registered customer subscribed to the "FREE_TIER" plan
    When the customer consumes 250 of "API_CALLS_MONTHLY"
    Then the consumption is accepted
    And the customer has used 250 of "API_CALLS_MONTHLY"
    And the customer has 750 of "API_CALLS_MONTHLY" left

  Scenario: Nothing more can be used once an allowance is exhausted
    Given a registered customer subscribed to the "FREE_TIER" plan
    And the customer has already consumed 1 of "STORAGE_GB"
    When the customer consumes 1 of "STORAGE_GB"
    Then the request is refused because the quota is exceeded

  Scenario: Upgrading to a bigger plan keeps what was already used
    Given a registered customer subscribed to the "FREE_TIER" plan
    And the customer has already consumed 200 of "API_CALLS_MONTHLY"
    When the customer subscribes to the "PRO" plan
    Then the customer is allocated 100000 of "API_CALLS_MONTHLY"
    And the customer has used 200 of "API_CALLS_MONTHLY"

  Scenario: A suspended customer cannot use the plan
    Given a registered customer subscribed to the "PRO" plan
    When the customer is suspended
    And the customer consumes 1 of "API_CALLS_MONTHLY"
    Then the request is refused because the customer is not active

  Scenario: A deleted customer can come back
    Given a registered customer subscribed to the "PRO" plan
    When the customer is deleted
    Then the customer status is "DELETED"
    When the customer is reactivated
    Then the customer status is "ACTIVE"
