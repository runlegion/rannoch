# Generated from legion requirement FR-RANNOCH-055, revision 2026-10-25T23:16:17.976731+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A class declares each property's value type and the key its history is ordered by, and every write carries its point

  @criterion-01a1752f-2116-704e-9361-5d1903ee6fc4 @module-write
  Scenario: a time-keyed write without its time is refused
    Given a class declared with a time key the writer supplies
    When the app writes a node of that class without a time
    Then the write is refused when it is written, naming the class and the missing key

  @criterion-01a1752f-2117-7f7e-8fa7-d36c99432cfd @module-write
  Scenario: an integer-keyed write without its key is refused
    Given a class declared with an integer key the writer supplies
    When the app writes a node of that class without an integer key
    Then the write is refused when it is written, naming the class and the missing key
