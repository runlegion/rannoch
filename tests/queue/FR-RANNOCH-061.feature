# Generated from legion requirement FR-RANNOCH-061, revision 2026-10-17T01:00:45.604936+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Dropping a graph's queue commits what it holds; close commits before an exit that does not unwind

  @criterion-01a1472c-e54b-7333-bf58-a2c73e0dc937 @module-queue
  Scenario: returning commits the queue
    Given a short-lived process that writes to a graph
    When the process returns
    Then every write it made is committed

  @criterion-01a1472c-e54b-7333-bf58-a2db4b12efb9 @module-queue
  Scenario: an unwinding panic commits the queue
    Given a short-lived process that writes to a graph
    When the process panics with unwinding
    Then every write it made is committed

  @criterion-01a1472c-e54b-7333-bf58-a2e9bf737bc3 @module-queue
  Scenario: close commits before an exit that does not unwind
    Given a short-lived process that writes to a graph
    When the process closes the queue and then exits without unwinding
    Then every write it made is committed
