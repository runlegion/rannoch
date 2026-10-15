# Generated from legion requirement FR-RANNOCH-034, revision 2026-10-14T19:39:19.277435+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link holds only between nodes of its relation's declared classes

  @criterion-01a13963-07e2-7a80-907d-83a36b0c6b35 @module-write
  Scenario: a link between the wrong classes is refused
    Given relation said is declared from voice to statement
    And a finding and a statement exist
    When the app writes, in one write of several pieces, new statements followed by a said link from the finding to the statement
    Then the write is refused
    And none of the new statements is in the graph
    And the number of events is unchanged
