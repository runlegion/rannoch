# Generated from legion requirement FR-RANNOCH-085, revision 2026-10-27T18:45:03.773651+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An app writes its existing history through the write API, and every link and ending keeps its real time

  @criterion-01a17ead-e5f1-7123-8e7a-4d756d02b573 @module-write
  Scenario: an ending before a later hearing says the link is still live
    Given a class whose history is ordered by a time the writer supplies
    And a link written at point 10 and written again at point 30
    When the app ends the link at point 20
    Then the ending is recorded at point 20
    And the result says the link reads as live from point 30
