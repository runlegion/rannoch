# Generated from legion requirement FR-RANNOCH-064, revision 2026-10-26T19:31:25.174883+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Another process follows commits through the committed watermark, every event exactly once

  @criterion-01a1472c-eab0-7e40-8d3b-29f459eb1719 @module-events
  Scenario: another process sees each committed event once
    Given a writer process and a follower in another process reading the committed watermark
    When the writer commits events
    Then the follower sees each committed event exactly once

  @criterion-01a1472c-eab0-7e40-8d3b-2a03f0045b46 @module-events
  Scenario: another process sees nothing uncommitted
    Given a writer process with accepted writes not yet committed, and a follower in another process
    When the follower reads the committed watermark
    Then the follower sees none of the uncommitted writes

  @criterion-01a1752a-e492-7081-8c98-ec9ba280baef @module-events
  Scenario: a rebuild from a snapshot keeps the watermark at the snapshot
    Given a graph with committed events and a snapshot at the cursor of its last event
    When the graph is rebuilt from the snapshot alone
    Then the rebuilt graph's committed watermark equals the snapshot's cursor

  @criterion-01a1752a-e493-76c0-9891-ae5ae67bfbbd @module-events
  Scenario: a rebuild from a snapshot and later events reports the last event
    Given a graph with a snapshot at a cursor and three committed events after it
    When the graph is rebuilt from the snapshot and the events after its cursor
    Then the rebuilt graph's committed watermark is the cursor of the third event

  @criterion-01a17946-87c3-7f82-a15c-be74f60924ba @module-events
  Scenario: a purge does not move the committed watermark back
    Given a graph whose last committed event is a hearing a purge rule matches, at cursor c
    When the purge runs
    Then the committed watermark is the purge event's cursor, which is greater than c

  @criterion-01a179b2-36c8-7672-ba13-e014d48b9504 @module-events
  Scenario: a snapshot at a cursor with no event there keeps the cursor
    Given a graph whose last event before cursor c was purged, so no event is at c
    When a snapshot is taken at c and a graph is rebuilt from it with no later events
    Then the rebuilt graph's committed watermark is at least c
