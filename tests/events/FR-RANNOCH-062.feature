# Generated from legion requirement FR-RANNOCH-062, revision 2026-10-26T08:42:09.319737+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: The store keeps every event; a snapshot keeps every version row and every event id, and deletes nothing

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

  @criterion-01a16046-ccaf-77f0-adf5-9a99473b1df5 @module-events
  Scenario: a snapshot keeps each node's fate
    Given a graph with one node forgotten and one node replaced before a snapshot's cursor
    When the graph is rebuilt from the snapshot and the events after its cursor
    Then the forgotten node resolves as forgotten and the replaced node resolves to its replacement, as in the live graph

  @criterion-01a1752f-2144-7adb-aa82-a0ed9e00ec8f @module-events
  Scenario: a snapshot answers the past
    Given a graph of a time-keyed class whose nodes and links have several versions each, some arriving after newer ones, and a snapshot at a cursor
    When the graph is rebuilt from the snapshot alone
    Then for every node and link, a read of its state at each point before the snapshot's newest point returns the same answer in both graphs

  @criterion-01a1752f-2145-773b-8018-cfc32fab5f05 @module-events
  Scenario: late events after a snapshot are replayed
    Given a graph of a time-keyed class with a snapshot at a cursor
    And events after the cursor, some at points before the snapshot's newest point
    When the graph is rebuilt from the snapshot and the events after its cursor
    Then every read of state at a point, of first and last heard, and as of a valid time returns the same answer in both graphs

  @criterion-01a1752f-2146-7050-ad82-f5e341a3321e @module-events
  Scenario: a snapshot keeps every event id that was not purged
    Given a graph with events, none of them purged, and a snapshot after them
    When the graph is rebuilt from the snapshot and the app writes, for each event before the cursor, a node derived from it
    Then every write is accepted
