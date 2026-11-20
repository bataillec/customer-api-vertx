Feature: The platform starts and is healthy
  The platform is made of several services that find each other when they start.
  Before trusting a new build, we check that all of them are up and answer.

  Scenario: Every service is up and ready
    When someone checks whether the platform is ready
    Then the platform is ready
    And the customer service is reported as UP
    And the audit service is reported as UP

  Scenario: The platform is alive
    Then the platform reports that it is alive

  Scenario: The three plans are on offer
    When someone lists the plans
    Then the plans on offer are "FREE_TIER", "PRO" and "ENTERPRISE"

  Scenario: The description of the API is published
    Then the platform publishes its API description
