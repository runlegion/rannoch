# Generated from legion requirement FR-RANNOCH-041, revision 2026-10-14T19:39:21.204919+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An exact count by distinct voice, total and group, over many roots in one pass

  @criterion-01a13963-1196-7ee1-aa03-ea3d31f0870a @module-read
  Scenario: one voice repeated counts once
    Given four statements by voice v1 that each support finding f1
    When the app counts distinct voices with the total from f1
    Then the count gives 1 distinct voice
    And a total of 4 statements

  @criterion-01a13963-1196-7ee1-aa03-ea43c2a0f84f @module-read
    # fixture: T4's size, 100,000 voices with T4's per-voice statement histogram carried as a constant table, 1,000 findings, 5 places
  Scenario: distinct, total and groups in one pass
    Given a graph of 100,000 voices and the largest finding's distinct voices, statements and per-place counts from its source
    When the app counts that finding's distinct voices with the total, grouped by place
    Then the count returns those distinct voices, that total and those per-place counts

  @criterion-01a13963-1196-7ee1-aa03-ea55fa9dcaa9 @module-read
    # fixture: T4's size, 100,000 voices with T4's per-voice statement histogram carried as a constant table, 1,000 findings, 5 places
  Scenario: many roots in one pass
    Given a graph of 100,000 voices and 1,000 findings
    When the app counts distinct voices from all 1,000 findings in one count
    Then the count gives a result for each of the 1,000 findings

  @criterion-01a13963-1196-7ee1-aa03-ea60d01fdb10 @module-read
  Scenario: a relation on the path filters the count
    Given finding f1 supported by statements s1 and s2 from voice v1
    And relation judged-not-supporting is declared from statement to finding
    And s2 is linked to f1 by judged-not-supporting
    When the app counts statements from f1, leaving out those linked to f1 by judged-not-supporting
    Then the count gives a total of 1 statement
