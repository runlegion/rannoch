# Generated from legion requirement FR-RANNOCH-042, revision 2026-10-14T08:33:35.304281+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Forgetting strips content and keeps ids, kinds and endpoints, through every replay

  @criterion-01a13963-1304-77b0-af60-7d5fbeb7444e @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: no event keeps forgotten content
    Given a graph
    When the app forgets one voice
    Then no event read from the start holds that voice's content

  @criterion-01a13963-1304-77b0-af60-7d6f828aca1a @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: the forget event names what it removed without content
    Given a graph
    When the app forgets one voice
    Then the forget event names the voice by id and kind and holds none of its content

  @criterion-01a13963-1304-77b0-af60-7d7127792460 @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: a forget keeps ids, kinds and endpoints
    Given a graph
    When the app forgets one voice
    Then the earlier events that named it still name its id, its kind and its links' endpoints

  @criterion-01a13963-1304-77b0-af60-7d8483a43363 @module-events
    # fixture: T2's size, 20 findings, 500 voices, 5,000 statements
  Scenario: folded numbers equal live state after a forget
    Given a graph after the app forgets one voice
    When the app folds distinct voices per finding from the events since the start
    Then each finding's distinct voices equal its exact count read from live state
