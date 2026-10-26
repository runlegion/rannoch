# Generated from legion requirement FR-RANNOCH-079, revision 2026-10-25T22:30:23.929175+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: What every registered follower has read is one mark the graph reports

  @criterion-01a16041-a1e0-73b1-9e5c-a067fba3c58b @module-events
  Scenario: the mark is the slowest follower's cursor
    Given a graph with events and two registered followers
    When one follower stores a cursor after the third event and the other after the fifth
    Then the graph's processed mark is the cursor after the third event

  @criterion-01a16041-a1e0-73b1-9e5c-a0784e550546 @module-events
  Scenario: the mark moves when the slowest follower catches up
    Given a graph whose processed mark is held by the slower of two registered followers
    When the slower follower stores a cursor past the faster one
    Then the graph's processed mark is the faster follower's cursor

  @criterion-01a16041-a1e0-73b1-9e5c-a08c6fc98119 @module-events
  Scenario: a new follower holds the mark at the start
    Given a graph with events and a registered follower that has read them all
    When a second follower registers without storing a cursor
    Then the graph's processed mark is the start of the events

  @criterion-01a16041-a1e0-73b1-9e5c-a09ae50ba8a7 @module-events
  Scenario: unregistering releases the mark
    Given a graph whose processed mark is held by one of two registered followers
    When that follower unregisters
    Then the graph's processed mark is the remaining follower's cursor

  @criterion-01a1752f-8b40-7474-80ea-7dbcb6617178 @module-events
  Scenario: a rebuild keeps a follower's cursor below the snapshot
    Given a graph with a registered follower whose stored cursor is after the second event, and a snapshot at the cursor of the fifth event
    When the graph is rebuilt from the snapshot
    Then the follower's stored cursor in the rebuilt graph is the cursor after the second event

  @criterion-01a1752f-8b41-7598-b529-a6a83942c5bd @module-events
  Scenario: a follower can advance to the snapshot's cursor after a rebuild
    Given a graph rebuilt from a snapshot, with a registered follower whose stored cursor is before the snapshot's cursor
    When the app advances the follower to the snapshot's cursor
    Then the advance is accepted and the follower's stored cursor is the snapshot's cursor
