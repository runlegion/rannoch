# Generated from legion requirement FR-RANNOCH-038, revision 2026-10-14T08:33:33.134494+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link to a key not yet present is open, and closes with exactly one event

  @criterion-01a13963-0d56-76f3-9113-3e340ba9aef1 @module-read
    # fixture: T5b's shape, as T5's with some parents dropped from the stream so their links never close
  Scenario: a link whose target never arrives joins no relation
    Given a reply stream in which some parents never arrive
    When the listener writes the whole stream
    Then walking answers from each reply to those parents finds no parent
