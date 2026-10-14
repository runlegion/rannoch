# Generated from legion requirement FR-RANNOCH-037, revision 2026-10-14T08:08:28.590039+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Every fact says where it came from and when it was true, apart from when it was recorded

  @criterion-01a13963-0bf9-7c83-ad60-20b72a6cb72f @module-write
  Scenario: valid time stays apart from recorded time
    Given an open graph
    When the app writes a statement with valid time 2019-10 today
    Then the statement reports valid time 2019-10
    And it reports a recorded time of today

  @criterion-01a13963-0bf9-7c83-ad60-20ce5ab8082e @module-write
  Scenario: a node and its link each keep their own valid time
    Given an open graph
    When the app writes in one write a statement valid at 2019-10 and a said link valid at 2026-02
    Then the statement reports valid time 2019-10
    And the said link reports valid time 2026-02

  @criterion-01a13963-0bf9-7c83-ad60-20d81fe2a952 @module-write
  Scenario: a fact says where it came from
    Given an open graph
    When the app writes a statement with source swtor
    Then the statement reports source swtor

  @criterion-01a13974-5730-7110-bb50-47ae72aaf13f @module-write
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: unknown valid time is stated, not defaulted
    Given an open graph
    When the app writes a statement whose valid time is stated unknown
    Then the statement reports its valid time as unknown

  @criterion-01a13974-5730-7110-bb50-47bb80bdbb1b @module-write
    # fixture: T10's synthetic set (t10-as-of, 2026-10-14): 1 finding, 40 supporting statements from 12 voices with valid times from 2023 to 2026, 3 of them with stated-unknown valid time; statement s05 valid 2024-05-01, rewritten on 2026-10-13
  Scenario: a rewritten old fact keeps when it was true
    Given statement s05 valid since 2024-05-01
    When the app rewrites s05 on 2026-10-13
    Then s05 reports valid time 2024-05-01
