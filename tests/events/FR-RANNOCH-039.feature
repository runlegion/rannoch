# Generated from legion requirement FR-RANNOCH-039, revision 2026-10-14T08:38:15.281781+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Events name what they added and removed, and are read from a cursor

  @criterion-01a13963-0ec3-7a91-be73-9fdfc8b46e12 @module-events
  Scenario: an event names what it added by kind and endpoint
    Given an open graph and the app's cursor at the end of its events
    When the app writes statement s1 with a said link from voice v1
    Then the events since the cursor name statement s1 and voice v1 by kind
    And they name the said link with both endpoints v1 and s1
    And each event carries its own id

  @criterion-01a13963-0ec3-7a91-be73-9fe643757369 @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: events read from a cursor in order
    Given a graph whose log holds the events of a graph
    When the app reads the events since a cursor halfway through the log
    Then it receives only the events after the cursor, in the order they were written

  @criterion-01a13963-0ec3-7a91-be73-9ff09374e762 @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: what is folded from the events equals live state
    Given a graph
    When the app folds distinct voices per finding from the events since the start
    Then each finding's distinct voices equal its exact count read from live state
