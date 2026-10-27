# Generated from legion requirement FR-RANNOCH-085, revision 2026-10-27T18:45:03.773651+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An app writes its existing history through the write API, and every link and ending keeps its real time

  @criterion-01a17ead-e5ef-7efb-ab20-9238d70e6632 @module-history
  Scenario: a link takes its unit's time
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a unit at point 10 that holds a link
    Then the link's hearing is at point 10

  @criterion-01a17ead-e5f0-7e62-aed7-4a308a654149 @module-history
  Scenario: history written long after it happened keeps its endings
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a unit at point 10 that holds a link, and then ends the link at point 20
    And both are recorded at a time later than point 20
    Then the link reads as live at point 15 and as ended at point 25

  @criterion-01a17ead-e5f2-70a9-9a46-f9aa831cbed8 @module-history
  Scenario: a history written out of order reads the same
    Given a class whose history is ordered by a time the writer supplies
    And a history of three units holding links and two endings
    When one graph is written in that order and another in reverse
    Then every link reads the same at every point in both graphs
