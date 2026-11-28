# #165's scenarios, from its acceptance criteria (FR-RANNOCH-131 rev 3: every time rannoch reads
# or waits on goes through rannoch-core's clock and tokio). Each scenario names the test that
# drives it.
Feature: rannoch-store uses rannoch-core's clock and tokio

  @module-rannoch-store @criterion-165-c1
  Scenario: rannoch-store calls no real-time function (clock_c1)
    Given rannoch-store's code and tests
    When they are read
    Then none calls std::thread::sleep, std::thread::spawn, recv_timeout, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7

  @module-rannoch-store @criterion-165-c2
  Scenario: rannoch-core's test that waits on the store's checkpointer runs frozen (an_app_opens_rannoch_over_its_database, in rannoch-core)
    Given rannoch-core's test an_app_opens_rannoch_over_its_database
    When it runs with the clock frozen
    Then it passes

  @module-rannoch-store @criterion-165-c3
  Scenario: Every rannoch-store test runs with the clock frozen (clock_c3)
    Given rannoch-store's tests
    When they are read and run
    Then each runs with #[tokio::test(start_paused = true)] and passes, moving time only with tokio::time::advance or tokio's auto-advance
