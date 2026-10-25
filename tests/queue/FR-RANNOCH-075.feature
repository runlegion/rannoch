# Generated from legion requirement FR-RANNOCH-075, revision 2026-10-17T01:34:11.558199+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A write that fails at commit is reported with every write that depended on it, never dropped in silence

  @criterion-01a1475f-bad3-7853-9b0e-6478531acdd9 @module-queue
  Scenario: a failed final commit is reported
    Given a process whose queue holds writes whose final commit will fail
    When the process closes the queue
    Then the process is told the final commit failed

  @criterion-01a1475f-bad3-7853-9b0e-6487f305e161 @module-queue
  Scenario: flush returns failed writes
    Given a queue holding a write that will fail at commit
    When the app flushes
    Then the flush returns that write as failed

  @criterion-01a1475f-bad3-7853-9b0e-649626a2a19c @module-queue
  Scenario: close returns failed writes
    Given a queue holding a write that will fail at commit
    When the app closes the queue
    Then the close returns that write as failed

  @criterion-01a1475f-bad3-7853-9b0e-64ad5e66fdf0 @module-queue
  Scenario: failed writes wait for the app
    Given a queue holding a write that will fail at commit
    When the heartbeat commits and the app takes the queue's failures
    Then the app receives that write as failed

  @criterion-01a1475f-bad3-7853-9b0e-64b2c2f8020b @module-queue
  Scenario: flush returns a failed write with its dependents
    Given a queue holding a node and then a link from it, where the node will fail at commit
    When the app flushes
    Then the flush returns the node and the link as failed

  @criterion-01a1475f-bad3-7853-9b0e-64ce0d5b03be @module-queue
  Scenario: close returns a failed write with its dependents
    Given a queue holding a node and then a link from it, where the node will fail at commit
    When the app closes the queue
    Then the close returns the node and the link as failed
