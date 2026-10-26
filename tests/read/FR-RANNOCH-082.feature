# Generated from legion requirement FR-RANNOCH-082, revision 2026-10-25T22:48:46.275222+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A node's or link's current state is its newest version on the class's axis, never the last to arrive

  @criterion-01a1752f-211c-7b1b-a499-9a8c447ea378 @module-read
  Scenario: a late older write does not replace the current state
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a node at 2025-06-01 and then writes the same node, with other properties, at 2024-03-01
    Then find returns the node's state written at 2025-06-01

  @criterion-01a1752f-211d-776c-a1a7-c50c8e3b7161 @module-read
  Scenario: the larger integer key is current
    Given a class whose history is ordered by an integer key the writer supplies
    When the app writes a node with key 3 and then writes the same node, with other properties, with key 2
    Then find returns the node's state written with key 3

  @criterion-01a1752f-211e-7f5e-85c0-fc4ea87818e0 @module-read
  Scenario: a class with no key reads its last recorded version as current
    Given a class declared without a history key
    When the app writes a node, waits at least one millisecond, and writes the same node with other properties
    Then find returns the node's state from the second write

  @criterion-01a1752f-211f-7967-aaf8-bf891f1a591e @module-read
  Scenario: current state does not depend on arrival order
    Given the events of a graph in which some versions of its nodes and links arrived after newer ones
    When two new graphs are given those events, one in recorded order and one in reverse order
    Then find returns the same result in both graphs for every node and link

  @criterion-01a1752f-2120-7d18-9bd0-d84816a94cc6 @module-read
  Scenario: a late version is kept
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a node at 2025-06-01 and then writes the same node at 2024-03-01
    Then a read of the node's state at 2024-06-01 returns the version written at 2024-03-01

  @criterion-01a17540-0251-7140-9865-fa892576537d @module-read
  Scenario: two writes in one millisecond read the second as current
    Given a class declared without a history key
    When the app writes a node and, from the same process within the same millisecond, writes the same node with other properties
    Then find returns the node's state from the second write
