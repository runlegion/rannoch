# Generated from legion requirement FR-RANNOCH-062, revision 2026-10-21T20:46:52.340452+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: The store keeps every event; compaction is a snapshot that deletes nothing

  @criterion-01a1472c-e715-76f3-a46a-32e6f6e4f98d @module-events
  Scenario: a snapshot deletes nothing
    Given a graph with events
    When the app takes a snapshot at a cursor
    Then every event before the cursor is still in the store

  @criterion-01a1472c-e715-76f3-a46a-32f85584948a @module-events
  Scenario: a rebuild from a snapshot matches live state
    Given a graph with a snapshot at a cursor and events after it
    When the graph is rebuilt from the snapshot and the events after its cursor
    Then the rebuilt graph holds every node and link the live graph holds

  @criterion-01a14736-3f12-70d2-943c-ab940ac0d4a7 @module-events
  Scenario: a rebuild from a snapshot keeps the declarations
    Given a graph whose class was declared before a snapshot's cursor
    When the graph is rebuilt from the snapshot and the app writes a node of that class
    Then the write is accepted

  @criterion-01a14736-3f12-70d2-943c-aba97ffeed3f @module-events
  Scenario: a rebuild from a snapshot keeps the history axis
    Given a graph whose class was declared with its history axis before a snapshot's cursor
    When the graph is rebuilt from the snapshot
    Then the class's history axis reads back from the rebuilt graph's ontology

  @criterion-01a14749-d8cf-71d2-89d2-68efaa6b3852 @module-events
  Scenario: a rebuild from a snapshot keeps declared types
    Given a graph whose class declared a property as a number before a snapshot's cursor
    When the graph is rebuilt from the snapshot and the app writes a node of that class with text for the property
    Then the write is refused

  @criterion-01a1472c-e715-76f3-a46a-33011a84daa4 @module-events
  Scenario: a new reader starts from the snapshot
    Given a graph with a snapshot at a cursor and events after it
    When a new reader starts from the snapshot
    Then the reader receives only the events after the snapshot's cursor
