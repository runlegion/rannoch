# Generated from legion requirement FR-RANNOCH-066, revision 2026-10-26T19:16:25.991989+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link's ending is a new fact with its own point and, when known, its own valid time; a link found wrong is a correction

  @criterion-01a177e2-0e9c-70e6-9a4d-c9c9cbc7ccab @module-history
  Scenario: a link heard after its ending is live again
    Given a class whose history is ordered by a time the writer supplies
    And a link written at point 10 that ended at point 20, and another link written at point 10 that ended at point 30
    When the app writes the first link again at point 30 and the second link again at point 20
    Then the first link reads as ended at point 25 and as live at point 35
    And the second link reads as ended at point 35

  @criterion-01a179a4-7de1-7621-9e06-f635113ad05a @module-history
  Scenario: an ending that arrives before the link is held
    Given a graph rebuilt from events in which a link's ending arrives before any live hearing of that link
    When the link's state is read at a point after the ending
    Then the link reads as ended at the ending's point

  @criterion-01a179a4-7de1-7621-9e06-f64f6b003fe5 @module-history
  Scenario: an earlier ending and a later live hearing
    Given a link ended at point 10 and first heard alive at point 20
    When its state is read at point 15 and at point 25
    Then it reads as ended at point 15 and live at point 25
