# From issue #171, which traces to FR-RANNOCH-131.
Feature: rannoch-processor-retrieve uses rannoch-core's clock and tokio

  @module-rannoch-processor-retrieve @issue-171-c1
  Scenario: rannoch-processor-retrieve calls no real-time function
    Given rannoch-processor-retrieve's code and tests
    When they are read
    Then none calls std::thread::sleep, std::thread::spawn, recv_timeout on a channel, std::time::Instant::now, std::time::SystemTime::now, chrono::Utc::now or uuid::Uuid::now_v7

  @module-rannoch-processor-retrieve @issue-171-c2
  Scenario: Every rannoch-processor-retrieve test runs with the clock frozen
    Given rannoch-processor-retrieve's tests
    When they are read
    Then each runs as #[tokio::test(start_paused = true)], and none moves time but through tokio::time::advance or tokio's auto-advance
