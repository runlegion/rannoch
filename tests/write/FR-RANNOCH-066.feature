# Generated from legion requirement FR-RANNOCH-066, revision 2026-10-26T19:16:25.991989+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link's ending is a new fact with its own point and, when known, its own valid time; a link found wrong is a correction

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

  @criterion-01a176d0-4d37-7ef1-8568-1570dd53c93d @module-write
  Scenario: an ending can carry a valid time
    Given a link that has been heard, valid from 2024-01-01
    When the app ends the link at a time, with a valid time of 2025-06-01
    Then the link's history holds an ending at that time with a valid time of 2025-06-01
