# Generated from legion requirement FR-RANNOCH-048, revision 2026-10-14T08:10:42.610455+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Walks and counts read as of a valid time, and never read unknown as now

  @criterion-01a13974-6575-7b20-a831-86c610ff8bd3 @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: a count as of a date counts only what was true by then
    Given T10's finding with 40 supporting statements
    When the app counts the finding's supporting statements as of 2024-12-31
    Then the count equals the number of the 40 that the fixture marks valid on or before 2024-12-31

  @criterion-01a13974-6575-7b20-a831-86dcd67391ea @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario Outline: distinct voices as of a date
    Given T10's finding with 40 supporting statements from 12 voices
    When the app counts the finding's distinct voices as of <date>
    Then the count equals the distinct voices of the statements the fixture marks valid on or before <date>
  
    Examples:
      | date       |
      | 2023-06-30 |
      | 2024-12-31 |
      | 2025-12-31 |
      | 2026-10-14 |

  @criterion-01a13974-6575-7b20-a831-86eb98fa94cf @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: unknown is never read as now
    Given T10's finding with 3 supporting statements of stated-unknown valid time
    When the app reads the finding's supporting statements as of 2023-01-01, as of 2026-10-14 and as of 2100-01-01
    Then none of the 3 statements of unknown valid time is returned

  @criterion-01a13974-6575-7b20-a831-86fea80bcc26 @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: a read with no as_of returns unknowns marked unknown
    Given T10's finding with 3 supporting statements of stated-unknown valid time
    When the app reads the finding's supporting statements with no as_of
    Then each of the 3 is returned marked unknown

  @criterion-01a13974-6575-7b20-a831-870bf230bf6f @module-read
  Scenario: as_of filters links on the path, not only nodes
    Given a statement valid before a date t that supports finding f1 by a link valid after t
    When the app counts statements supporting f1 as of t
    Then the statement is not counted
