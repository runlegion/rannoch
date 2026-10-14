# Generated from legion requirement FR-RANNOCH-038, revision 2026-10-14T08:33:33.134494+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link to a key not yet present is open, and closes with exactly one event

  @criterion-01a13963-0d56-76f3-9113-3e1b4bf96787 @module-write
    # fixture: T5's shape, statements with their voices and reply links, arriving shuffled within windows of 200 so a reply can arrive before its parent
  Scenario: the reply links stored equal the reply links the source gave
    Given a reply stream and the set of reply links its source gave
    When the listener writes the whole stream
    Then the graph's answers links are exactly that set
