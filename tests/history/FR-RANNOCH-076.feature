# Generated from legion requirement FR-RANNOCH-076, revision 2026-10-25T22:48:18.899997+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: First and last heard run on the declared axis, ties are broken by data, first recorded stays in arrival order, and first true is the earliest valid time

  @criterion-01a1752f-2130-716d-9f41-12c3cba95a69 @module-history
  Scenario: a tie at one point is broken by data
    Given a class whose history is ordered by a time the writer supplies
    And two writes of one node at point 2025-06-01 with different properties
    When one graph is given the two writes in one order and another graph in the other order
    Then find returns the same state for the node in both graphs

  @criterion-01a1752f-2131-7c91-af5c-4c66ebf83bcc @module-history
  Scenario: first recorded belongs to the store
    Given a class whose history is ordered by a time the writer supplies
    And the events of a graph in which a node was heard at 2025-06 and then at 2024-03
    When a new graph is given the same events in reverse order
    Then the first graph's first recorded is the 2025-06 hearing and the new graph's first recorded is the 2024-03 hearing
