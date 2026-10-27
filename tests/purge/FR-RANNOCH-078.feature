# Generated from legion requirement FR-RANNOCH-078, revision 2026-10-26T09:02:11.328551+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A purge is recorded as an event carrying only the app's rule, and no follower receives what was purged

  @criterion-01a1475f-fdac-7802-a753-f2aabc77c959 @module-purge
  Scenario: a purge event carries the rule
    Given a graph holding a fact
    When the app purges the fact under a rule
    Then the event log gains a purge event carrying the rule's name

  @criterion-01a1475f-fdac-7802-a753-f2ba8fe9590e @module-purge
  Scenario: a purge event names nothing purged
    Given a graph holding a fact
    When the app purges the fact under a rule
    Then the purge event holds no id or content of the fact and no copy of the fact's key

  @criterion-01a1475f-fdac-7802-a753-f2c85cd96e12 @module-purge
  Scenario: a follower receives nothing purged
    Given a follower whose cursor is before a fact was written
    When the app purges the fact and the follower reads since its cursor
    Then the follower receives no id or content of the fact

  @criterion-01a17771-f5bd-7150-b3d7-c2e91fa041f2 @module-purge
  Scenario: a graph rebuilt from its events keeps purging
    Given a fact heard from two sources, and a purge rule for one of them
    When the graph is rebuilt from its events and the app writes a hearing of the fact from the purged source
    Then nothing of that hearing remains in the rebuilt graph, and the fact's first heard, last heard and distinct count are those of the surviving source alone
