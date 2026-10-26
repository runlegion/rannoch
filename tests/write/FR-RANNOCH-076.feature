# Generated from legion requirement FR-RANNOCH-076, revision 2026-10-25T22:48:18.899997+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: First and last heard run on the declared axis, ties are broken by data, first recorded stays in arrival order, and first true is the earliest valid time

  @criterion-01a1475f-edfb-76a0-a1b2-1479079ff73f @module-write
  Scenario: first heard is the earliest on the axis
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a hearing of a node at 2025-06 and then one at 2024-03
    Then the node's first heard is 2024-03

  @criterion-01a1475f-edfb-76a0-a1b2-14889a2f741c @module-write
  Scenario: last heard is the latest on the axis
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a hearing of a node at 2025-06 and then one at 2024-03
    Then the node's last heard is 2025-06

  @criterion-01a1475f-edfb-76a0-a1b2-149514c0ce13 @module-write
  Scenario: first recorded stays in arrival order
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a hearing of a node at 2025-06 and then one at 2024-03
    Then the node's first recorded is the 2025-06 write

  @criterion-01a1475f-edfb-76a0-a1b2-14a2eb0cdb45 @module-write
  Scenario: first true is the earliest valid time
    Given a node written with a valid time of 2025-06
    When the app rewrites it with a valid time of 2024-03
    Then the node's first true is 2024-03

  @criterion-01a1475f-edfb-76a0-a1b2-14b269462848 @module-write
  Scenario: first true is kept against a later valid time
    Given a node written with a valid time of 2024-03
    When the app rewrites it with a valid time of 2025-06
    Then the node's first true is 2024-03
