# Generated from legion requirement FR-RANNOCH-043, revision 2026-10-14T19:39:21.597566+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Compaction up to the app's cursor keeps state

  @criterion-01a13963-1465-70a2-aeab-5fc36298da85 @module-events
  Scenario: compaction removes the events up to the cursor
    Given a graph with events before and after a cursor
    When the app compacts at the cursor
    Then reading the events returns no event up to the cursor
    And it returns every event after the cursor

  @criterion-01a13963-1465-70a2-aeab-5fddaeff968d @module-events
    # fixture: T4's size, 100,000 voices with T4's per-voice statement histogram carried as a constant table, 1,000 findings, 5 places
  Scenario: compaction keeps the state counts stand on
    Given a graph of 100,000 voices and its live count for each of the 1,000 findings
    When the app compacts at a cursor
    Then the count for each of the 1,000 findings equals its count before compaction

  @criterion-01a1398f-e7ba-7019-a35e-2b1da311ca63 @module-events
    # fixture: T4's size, 100,000 voices with T4's per-voice statement histogram carried as a constant table, 1,000 findings, 5 places
  Scenario: a rebuild from the compacted base plus later events equals live
    Given a graph compacted at a cursor, and the count for each of the 1,000 findings taken right after compaction
    When the app writes more statements and folds the events after the cursor onto those counts
    Then its per-finding numbers equal the live counts for all 1,000 findings
