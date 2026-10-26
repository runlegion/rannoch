# Generated from legion requirement FR-RANNOCH-080, revision 2026-10-26T08:36:17.124509+00:00.
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

  @criterion-01a1752f-8b42-7dfe-b75a-c3f1a1dc993f @module-events
  Scenario: a link keeps the events it came from
    Given a graph with two nodes and two events
    When the app writes a link between the nodes derived from both events
    Then the link reads back naming both events as its source

  @criterion-01a1752f-8b43-7b63-8e03-271fffc04e3d @module-events
  Scenario: an event finds the link it became
    Given a graph with a link derived from two events
    When the app asks what was derived from each of those events
    Then each answer names that link

  @criterion-01a1752f-8b44-7ada-b92f-d8af03e4ca83 @module-events
  Scenario: a rebuilt graph accepts a source from before its snapshot
    Given a graph with a node derived from an event, and a snapshot after that event
    When the graph is rebuilt from the snapshot and the app writes another node derived from the same event
    Then the write is accepted

  @criterion-01a1752f-8b45-7eb5-81d7-1d7995d06806 @module-events
  Scenario: a link's source survives both rebuilds
    Given a graph with a link derived from an event and a snapshot after it
    When the graph is rebuilt once from its events and once from the snapshot and the events after its cursor
    Then in both rebuilt graphs the link reads back naming the same event as its source

  @criterion-01a17540-2067-7b13-8cfc-64194d9e2d0f @module-events
  Scenario: a purged source is refused as if never held
    Given a graph with a node derived from an event, and a purge that removed that event
    When the app writes a node derived from the purged event's id, and another derived from an id the graph never held
    Then both writes are refused with the same refusal

  @criterion-01a17540-2067-7b13-8cfc-6422363ed459 @module-events
  Scenario: a source named by an alias is accepted in either arrival order
    Given one hearing's content delivered twice, under event ids A and B, A the smaller
    And one new graph given A then B and another given B then A
    When the app writes to each graph a node derived from B
    Then both writes are accepted
