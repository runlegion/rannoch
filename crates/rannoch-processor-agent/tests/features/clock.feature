# From the acceptance criteria of issue #169, which traces to FR-RANNOCH-131 (revision 3).
Feature: rannoch-processor-agent reads and waits on time only through rannoch-core's clock and tokio

  @module-rannoch-processor-agent @criterion-01a220b8-d5bd-7151-baf4-97a3c4b86d45
  Scenario: rannoch-processor-agent calls no real-time function
    Given rannoch-processor-agent's code and tests
    When they are read for a call to std::thread::sleep, std::thread::spawn, recv_timeout on a channel, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7
    Then none is found, and their time and waiting go through rannoch-core's clock and tokio

  @module-rannoch-processor-agent @criterion-01a220b8-d5bd-7151-baf4-97a3c4b86d45
  Scenario: Every rannoch-processor-agent test runs with the clock frozen
    Given rannoch-processor-agent's tests
    When they are read for how each one runs
    Then each runs with the clock frozen, as #[tokio::test(start_paused = true)], and moves time only with tokio::time::advance or tokio's auto-advance
