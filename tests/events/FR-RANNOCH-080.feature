# Generated from legion requirement FR-RANNOCH-080, revision 2026-10-21T20:58:08.182755+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A fact can name the events it was derived from, and each side finds the other

  @criterion-01a16041-a4fb-72c3-bc47-06be68c20303 @module-events
  Scenario: a fact keeps the events it came from
    Given a graph with two events
    When the app writes a node derived from both events
    Then the node reads back naming both events as its source

  @criterion-01a16041-a4fb-72c3-bc47-06cd5251065f @module-events
  Scenario: an event finds what it became
    Given a graph with a node derived from an event
    When the app asks what was derived from that event
    Then the answer names that node

  @criterion-01a16041-a4fb-72c3-bc47-06dbf100ba8c @module-events
  Scenario: a source the graph does not hold is refused
    Given a graph with events
    When the app writes a node derived from an event id the graph does not hold
    Then the write is refused when it is written

  @criterion-01a16041-a4fb-72c3-bc47-06e832c35416 @module-events
  Scenario: the source survives a rebuild
    Given a graph with a node derived from an event and a snapshot after it
    When the graph is rebuilt from the snapshot and the events after its cursor
    Then the node reads back naming the same event as its source
