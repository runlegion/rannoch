# Generated from legion requirement FR-RANNOCH-036, revision 2026-10-14T20:03:12.218582+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: One node per class and natural key

  @criterion-01a13963-0a9b-7101-9f7a-40bf30791908 @module-write
  Scenario: the same key twice is one node
    Given class voice is declared with a natural key
    When the app writes voice v1 and then writes voice v1 again
    Then counting voices with key v1 gives 1

  @criterion-01a13bec-8b96-7aa4-bd10-c20f5f2f0074 @module-write
  Scenario: an identical rewrite is a no-op
    Given voice v1 written once
    When the app writes voice v1 again with the same properties, source and valid time
    Then the number of events is unchanged
