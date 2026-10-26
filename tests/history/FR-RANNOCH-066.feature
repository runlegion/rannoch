# Generated from legion requirement FR-RANNOCH-066, revision 2026-10-26T11:06:00.742202+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link's ending is a new fact with its own point and, when known, its own valid time; a link found wrong is a correction

  @criterion-01a177e2-0e9c-70e6-9a4d-c9c9cbc7ccab @module-history
  Scenario: a link heard after its ending is live again
    Given a class whose history is ordered by a time the writer supplies
    And a link written at point 10 that ended at point 20, and another link written at point 10 that ended at point 30
    When the app writes the first link again at point 30 and the second link again at point 20
    Then the first link reads as ended at point 25 and as live at point 35
    And the second link reads as ended at point 35
