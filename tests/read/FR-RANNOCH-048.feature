# Generated from legion requirement FR-RANNOCH-048, revision 2026-10-25T22:48:16.802780+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Reads as of a valid time return the version that held then, and never read unknown as now

  @criterion-01a13974-6575-7b20-a831-86c610ff8bd3 @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13; plus statement s41 (added 2026-10-25), from a 13th voice that no other statement has, supporting the finding, with a version valid 2024-03-01 and a later version valid 2025-06-01, so as of 2024-12-31 a read of current rows leaves s41 and its voice out and a read of versions counts them
  Scenario: a count as of a date counts only what was true by then
    Given T10's finding with 41 supporting statements
    When the app counts the finding's supporting statements as of 2024-12-31
    Then the count equals the number of the 41 that the fixture marks valid on or before 2024-12-31

  @criterion-01a13974-6575-7b20-a831-86dcd67391ea @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13; plus statement s41 (added 2026-10-25), from a 13th voice that no other statement has, supporting the finding, with a version valid 2024-03-01 and a later version valid 2025-06-01, so as of 2024-12-31 a read of current rows leaves s41 and its voice out and a read of versions counts them
  Scenario Outline: distinct voices as of a date
    Given T10's finding with 41 supporting statements from 13 voices
    When the app counts the finding's distinct voices as of <date>
    Then the count equals the distinct voices of the statements the fixture marks valid on or before <date>
  
    Examples:
      | date       |
      | 2023-06-30 |
      | 2024-12-31 |
      | 2025-12-31 |
      | 2026-10-14 |

  @criterion-01a13974-6575-7b20-a831-86eb98fa94cf @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13; plus statement s41 (added 2026-10-25), from a 13th voice that no other statement has, supporting the finding, with a version valid 2024-03-01 and a later version valid 2025-06-01, so as of 2024-12-31 a read of current rows leaves s41 and its voice out and a read of versions counts them
  Scenario: unknown is never read as now
    Given T10's finding with 3 supporting statements of stated-unknown valid time
    When the app reads the finding's supporting statements as of 2023-01-01, as of 2026-10-14 and as of 2100-01-01
    Then none of the 3 statements of unknown valid time is returned

  @criterion-01a13974-6575-7b20-a831-86fea80bcc26 @module-read
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13; plus statement s41 (added 2026-10-25), from a 13th voice that no other statement has, supporting the finding, with a version valid 2024-03-01 and a later version valid 2025-06-01, so as of 2024-12-31 a read of current rows leaves s41 and its voice out and a read of versions counts them
  Scenario: a read with no as_of returns unknowns marked unknown
    Given T10's finding with 3 supporting statements of stated-unknown valid time
    When the app reads the finding's supporting statements with no as_of
    Then each of the 3 is returned marked unknown

  @criterion-01a13974-6575-7b20-a831-870bf230bf6f @module-read
  Scenario: as_of filters links on the path, not only nodes
    Given a statement valid before a date t that supports finding f1 by a link none of whose versions was valid on or before t
    When the app counts statements supporting f1 as of t
    Then the statement is not counted

  @criterion-01a1752f-212d-7f56-b740-380d057e071a @module-read
  Scenario: an earlier version answers as of a valid time before the current one
    Given a node written with a valid time of 2024-01-01 and then rewritten with a valid time of 2025-06-01
    When the app reads the node as of valid time 2024-12-01
    Then the read returns the version valid from 2024-01-01

  @criterion-01a1752f-212e-7588-b10b-d59eaf4ee1dc @module-read
  Scenario: what was true then, as known now and as known at a point
    Given a class whose history is ordered by a time the writer supplies
    And a node with a version heard at point 10 with valid time 2023-01-01 and a version heard at point 40 with valid time 2024-01-01
    When the app reads the node as of valid time 2024-06-01 as known now, and as of valid time 2024-06-01 as known at point 30
    Then the first read returns the version heard at point 40 and the second returns the version heard at point 10

  @criterion-01a1752f-212f-7b71-88f9-9b254c61eccb @module-read
  Scenario: a count as of a valid time follows each link's version then
    Given a statement that supports finding f1 by a link valid from 2024-01-01, and the link rewritten to end with a valid time of 2025-06-01
    When the app counts statements supporting f1 as of valid time 2024-12-01 and as of valid time 2025-12-01
    Then the statement is counted as of 2024-12-01 and not as of 2025-12-01
