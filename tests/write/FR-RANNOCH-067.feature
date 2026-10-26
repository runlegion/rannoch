# Generated from legion requirement FR-RANNOCH-067, revision 2026-10-17T00:05:06.958557+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Replacing a node carries its history and links to its new key

  @criterion-01a1472c-f01f-76a0-873a-e148fb3d6dd2 @module-write
  Scenario: links follow a replace
    Given a node with links
    When the app replaces it by a node under a new key
    Then its links are on the new key

  @criterion-01a1472c-f01f-76a0-873a-e154ee300130 @module-write
  Scenario: hearings follow a replace
    Given a node that has been heard more than once
    When the app replaces it by a node under a new key
    Then the new key holds every hearing the old key had
