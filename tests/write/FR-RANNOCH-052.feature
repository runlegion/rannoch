# Generated from legion requirement FR-RANNOCH-052, revision 2026-10-14T19:39:51.140833+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Replacing a node is its own event, naming the old key and the new

  @criterion-01a13bec-ffe3-706a-8c1b-a5719d9b9fec @module-write
  Scenario: a replacement is one event
    Given statements s8 and s9
    When the app replaces s8 by s9
    Then the event log gains one event naming class statement, s8 and s9
    And describing s8 answers replaced by s9
