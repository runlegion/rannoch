# Generated from legion requirement FR-RANNOCH-038, revision 2026-10-14T08:33:33.134494+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link to a key not yet present is open, and closes with exactly one event

  @criterion-01a13963-0d56-76f3-9113-3e2e51754ef8 @module-events
    # fixture: T5's shape, statements with their voices and reply links, arriving shuffled within windows of 200 so a reply can arrive before its parent
  Scenario: each open link closes with exactly one event
    Given a reply stream in which some replies arrive before their parent
    When the listener writes the whole stream
    Then reading the events from the start returns exactly one closing event for each of those replies' links
