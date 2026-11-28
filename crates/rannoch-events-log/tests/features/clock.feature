# #168's scenarios, from FR-RANNOCH-131 (revision 3). Each scenario names the test in
# tests/clock.rs that drives it.
Feature: rannoch-events-log uses rannoch-core's clock and tokio

  @module-rannoch-events-log
  Scenario: The crate's code and tests call no real-time function
    # clock::criterion_1_no_real_time_calls
    Given rannoch-events-log's source and tests
    When they are read for real-time calls
    Then none calls std::thread::sleep, std::thread::spawn, recv_timeout, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7

  @module-rannoch-events-log
  Scenario: Every test runs with the clock frozen and passes
    # clock::criterion_2_every_test_runs_frozen, and every test in tests/scenarios.rs and tests/settings.rs
    Given rannoch-events-log's tests
    When they are read for how each runs
    Then each runs with #[tokio::test(start_paused = true)], none resumes or pauses the clock, and each passes
