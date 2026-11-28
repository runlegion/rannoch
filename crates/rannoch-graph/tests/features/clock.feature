# #166's scenarios, from FR-RANNOCH-131 (revision 3). Each scenario names the test that drives
# it, in tests/scenarios.rs.
Feature: rannoch-graph's time and waiting go through rannoch-core's clock and tokio

  @module-rannoch-graph @criterion-01a220b8-d5bd-7151-baf4-97a3c4b86d45
  Scenario: rannoch-graph calls no real-time function (rannoch_graph_calls_no_real_time_function)
    Given rannoch-graph's code and tests
    Then none calls std::thread::sleep, std::thread::spawn, recv_timeout, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7

  @module-rannoch-graph @criterion-01a220b8-d5bd-7151-baf4-97a3c4b86d45
  Scenario: Every rannoch-graph test runs with the clock frozen (every_rannoch_graph_test_runs_with_the_clock_frozen)
    Given rannoch-graph's tests
    Then each runs as #[tokio::test(start_paused = true)], and none moves time but through tokio::time::advance or tokio's auto-advance
