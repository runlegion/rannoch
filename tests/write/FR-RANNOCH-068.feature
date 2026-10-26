# Generated from legion requirement FR-RANNOCH-068, revision 2026-10-26T08:36:16.486741+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link from a source never heard is refused on this store's own write path; a link to a target not yet heard stays open

  @criterion-01a1472c-f1ef-7480-8e30-fa210c7221f2 @module-write
  Scenario: a link from a source never heard is refused
    Given a graph in which a key has never been heard
    When the app writes a link from that key
    Then the write is refused naming that key

  @criterion-01a1472c-f1ef-7480-8e30-fa3d0c1c55c0 @module-write
  Scenario: a source heard in the same write is enough
    Given an empty graph
    When the app writes a node and a link from it in the same write
    Then the link is in the graph

  @criterion-01a14736-4a2e-7e90-811a-ec60036783c1 @module-write
  Scenario: a source earlier in the queue counts as heard
    Given a process that has written a node not yet committed
    When the process writes a link from that node
    Then the link is accepted

  @criterion-01a1472c-f1ef-7480-8e30-fa45d66410f3 @module-write
  Scenario: a link to a target not yet heard stays open
    Given a graph with a heard node and a key never heard
    When the app writes a link from the node to that key
    Then the link is listed as open
