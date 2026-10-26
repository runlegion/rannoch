# Generated from legion requirement FR-RANNOCH-066, revision 2026-10-17T01:34:10.126686+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link that ends is a new fact with its own time; a link found wrong is a correction

  @criterion-01a1472c-ee54-75e0-b620-4fa3ed736e78 @module-write
  Scenario: an ending has its own time
    Given a link that has been heard
    When the app ends the link at a time
    Then the link's history holds an ending at that time

  @criterion-01a1472c-ee54-75e0-b620-4fb9e8a1ab0d @module-write
  Scenario: an ended link keeps its history
    Given a link that has been heard more than once
    When the app ends the link
    Then the link's history still holds every hearing

  @criterion-01a1472c-ee54-75e0-b620-4fcae5fd9a34 @module-write
  Scenario: a link found wrong is a correction
    Given a link that has been heard
    When the app finds the link wrong
    Then the link does not read as ended
