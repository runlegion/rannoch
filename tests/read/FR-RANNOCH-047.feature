# Generated from legion requirement FR-RANNOCH-047, revision 2026-10-14T20:15:42.157435+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: find returns a node with where it came from, when it was true and when it was recorded, as of a valid time

  @criterion-01a13974-6413-7d10-b432-4d6e0205b7ab @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: find returns a fact with its provenance
    Given statement s05 written with source swtor and valid time 2024-05-01
    When the app finds statement s05
    Then the node returned carries source swtor, valid time 2024-05-01 and the time rannoch recorded it

  @criterion-01a13974-6413-7d10-b432-4d8a95f73ff5 @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: a rewrite keeps its valid time
    Given statement s05 valid 2024-05-01, rewritten on 2026-10-13 with corrected text
    When the app finds statement s05 
    Then the node returned carries the corrected text and valid time 2024-05-01

  @criterion-01a13974-6413-7d10-b432-4d9e67b7296d @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: find as of a time before the fact was true
    Given statement s05 valid since 2024-05-01
    When the app finds statement s05 as of 2023-06-30
    Then no node is returned
