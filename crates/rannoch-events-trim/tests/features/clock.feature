# #167's scenarios, from its acceptance criteria (FR-RANNOCH-131 rev 3: every time rannoch reads
# or waits on goes through rannoch-core's clock and tokio). Each scenario names the test that
# drives it.
Feature: rannoch-events-trim uses rannoch-core's clock and tokio

  @module-rannoch-events-trim @criterion-167-c1
  Scenario: rannoch-events-trim calls no real-time function (clock_c1)
    Given rannoch-events-trim's code and tests
    When they are read
    Then none calls std::thread::sleep, std::thread::spawn, recv_timeout, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7

  @module-rannoch-events-trim @criterion-167-c2
  Scenario: Every rannoch-events-trim test runs with the clock frozen (clock_c2)
    Given rannoch-events-trim's tests
    When they are read and run
    Then each runs with #[tokio::test(start_paused = true)] and passes, moving time only with tokio::time::advance or tokio's auto-advance
