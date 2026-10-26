# Generated from legion requirement FR-RANNOCH-066, revision 2026-10-26T19:16:25.991989+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link's ending is a new fact with its own point and, when known, its own valid time; a link found wrong is a correction

  @criterion-01a176d0-4d37-7ef1-8568-158188e3703a @module-read
  Scenario: a read as of a valid time reads a link as ended from its ending's valid time
    Given a statement that supports finding f1 by a link valid from 2024-01-01, and the link ended with a valid time of 2025-06-01
    When the app counts statements supporting f1 as of valid time 2024-12-01, as of valid time 2025-06-01 and as of valid time 2025-12-01
    Then the statement is counted as of 2024-12-01 and not as of 2025-06-01 or 2025-12-01

  @criterion-01a176d0-4d37-7ef1-8568-15945b8a15a6 @module-read
  Scenario: an ending of unknown valid time is never read as now
    Given a statement that supports finding f1 by a link valid from 2024-01-01, and the link ended with no valid time
    When the app counts statements supporting f1 as of valid time 2023-06-01, as of valid time 2024-12-01 and as of valid time 2100-01-01
    Then the count as of 2023-06-01 is answered and does not count the statement
    And the counts as of 2024-12-01 and 2100-01-01 are refused, each saying the link's ending's valid time is unknown
