# Generated from legion requirement FR-RANNOCH-064, revision 2026-10-22T18:39:37.641207+00:00.
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
