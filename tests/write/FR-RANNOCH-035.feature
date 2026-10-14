# Generated from legion requirement FR-RANNOCH-035, revision 2026-10-14T19:39:19.843952+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A write commits whole units in pieces; each piece is one transaction, and its events land only if it lands

  @criterion-01a13963-0943-7e80-b73e-8515204f0ce5 @module-write
    # fixture: T5's shape, statements with their voices and reply links, arriving shuffled within windows of 200 so a reply can arrive before its parent
  Scenario: the log gains exactly the events the write returned
    Given an open graph and the app's cursor at the end of its events
    When the app writes a reply stream in one write
    Then reading the events since the cursor returns exactly the events the write returned

  @criterion-01a13bec-8b96-78e4-acff-f2615bc981c6 @module-write
    # fixture: a graph file whose writes fail at the third piece of a write
  Scenario: a failed piece keeps the pieces before it
    Given a write of five pieces whose third piece fails
    When the app writes it
    Then the write returns an error holding the events of pieces one and two
    And the event log gains exactly those events
    And no unit of pieces three to five is in the graph
