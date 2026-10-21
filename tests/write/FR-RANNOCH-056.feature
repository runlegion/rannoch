# Generated from legion requirement FR-RANNOCH-056, revision 2026-10-21T07:19:36.238295+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A write that does not carry what its class declares is refused at once

  @criterion-01a14760-1a5d-7073-b2a6-1ee4877a1467 @module-write
  Scenario: a write missing a declared property is refused at once
    Given a class that declares a property
    When the app writes a node of that class without the property
    Then the write is refused when it is written

  @criterion-01a1472c-dc39-7232-a674-d6479a646bf1 @module-write
  Scenario: a write with a wrongly typed value is refused at once
    Given a class with a property declared as an integer
    When the app writes a node of that class with text for the property
    Then the write is refused when it is written
