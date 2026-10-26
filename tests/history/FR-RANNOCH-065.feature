# Generated from legion requirement FR-RANNOCH-065, revision 2026-10-26T18:05:14.099304+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Identical content from the same source is one hearing in every class; another source or other content adds a hearing

  @criterion-01a17963-2809-7c00-a0ce-1df94513d051 @module-history
  Scenario: a source that returns to earlier content is read as saying it again
    Given a class declared without a history key
    And a node written by one source with content v1, then v2, then v1 again, each in its own commit
    When the node is read
    Then its current state is v1
    And it has one hearing of v1 with two occurrences and one hearing of v2

  @criterion-01a17963-2809-7c00-a0ce-1e08d5218fee @module-history
  Scenario: an old log's return to earlier content replays as a return
    Given a log written by rannoch 0.2.0 in which one source wrote a node with content v1, then v2, then v1 again
    When a graph is rebuilt from that log
    Then the node's current state is v1
