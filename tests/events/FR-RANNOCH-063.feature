# Generated from legion requirement FR-RANNOCH-063, revision 2026-10-21T21:45:54.530516+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A follower picks up where it left off, with only the events it asked about

  @criterion-01a1472c-e8e1-7c01-bf2f-c0601294f1f6 @module-events
  Scenario: a restarted follower resumes from its stored cursor
    Given a follower that has stored its cursor
    When the follower restarts from the stored cursor
    Then it reads only the events after that cursor

  @criterion-01a1472c-e8e1-7c01-bf2f-c07cd50e5bac @module-events
  Scenario: since stops at its bound
    Given a graph with events
    When the app reads events since a cursor up to a bound
    Then no event past the bound is returned

  @criterion-01a1472c-e8e1-7c01-bf2f-c087a0a82244 @module-events
  Scenario: since returns only what was asked about
    Given a graph with events of several kinds
    When the app reads events since a cursor with a filter
    Then only events the filter asks about are returned

  @criterion-01a1472c-e8e1-7c01-bf2f-c09ab0f6c8c9 @module-events
  Scenario: a follower behind a snapshot catches up
    Given a follower whose stored cursor is older than a snapshot
    When the follower reads since its stored cursor
    Then it reads every event after its cursor

  @criterion-01a14760-2d96-71a2-bfee-4f6edcc6cc35 @module-events
  Scenario: a stored cursor survives a rebuild
    Given a follower that stored its cursor and a graph that is then rebuilt
    When the follower reads the rebuilt graph's events since its stored cursor
    Then it reads the events that came after that cursor before the rebuild
