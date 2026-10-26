# Generated from legion requirement FR-RANNOCH-070, revision 2026-10-26T08:36:16.596078+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: First heard, last heard and when it ended come back in one read

  @criterion-01a1472c-f591-7d93-9dcb-387eff424047 @module-history
  Scenario: one read answers first and last heard
    Given a node that has been heard from more than one source
    When the app reads it once
    Then the read returns its first heard and its last heard

  @criterion-01a1472c-f591-7d93-9dcb-38834a4337eb @module-history
  Scenario: one read answers when a link ended
    Given a link that has been heard from more than one source and has ended
    When the app reads it once
    Then the read returns its first heard, its last heard and when it ended
