# Generated from legion requirement FR-RANNOCH-035, revision 2026-10-14T19:39:19.843952+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A write commits whole units in pieces; each piece is one transaction, and its events land only if it lands

  @criterion-01a13963-0943-7e80-b73e-852651d28e73 @module-read
    # fixture: T5's shape, statements with their voices and reply links, arriving shuffled within windows of 200 so a reply can arrive before its parent
  Scenario: no reader sees half a unit
    Given the listener is writing a reply stream as units of a statement, its voice and its links
    When a reader counts statements and their voices during the burst
    Then no read sees a statement without its voice
