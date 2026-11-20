Feature: Losing and recovering a service
  The customer service runs twice, so that the platform keeps working when one copy is lost.
  When no copy is left, the platform says so clearly, and recovers by itself once they are back.

  @destructive
  Scenario: The platform keeps working when one customer service is lost
    Given the platform is running with two customer services
    When the customer service "customer-1" is stopped
    Then plans can still be listed
    And a new customer can still be registered

  @destructive
  Scenario: The platform reports the outage when every customer service is lost, then recovers
    Given the platform is running with two customer services
    When both customer services are stopped
    Then listing the plans is refused because the service is unavailable
    And the platform reports that it is not ready
    When both customer services are started again
    Then the platform becomes ready again
    And plans can still be listed
