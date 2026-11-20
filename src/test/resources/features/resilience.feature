Feature: Surviving a slow or unreachable dependency
  The platform depends on a database and on a broker for the audit trail.
  When one of them stops responding, the platform must say so, and recover once it is back.

  @destructive
  Scenario: The platform reports the database being unreachable, then recovers
    Given the platform is running with two customer services
    When the database stops responding
    Then the platform reports that it is not ready because the customer service is down
    When the database responds again
    Then the platform becomes ready again

  @destructive
  Scenario: Customers can still be served while the audit broker is unreachable
    Given the platform is running with two customer services
    When the audit broker stops responding
    Then the platform reports that it is not ready because the audit service is down
    And a new customer can still be registered
    When the audit broker responds again
    Then the platform becomes ready again
