# Generated from FR-RANNOCH-005 (document 01a1c850-f36c-7970-aff2-7df7bfbfe13c, revision 3). Do not hand-edit.
Feature: The batch: the inbox hands each batch to the events crate

  @criterion-FR-RANNOCH-005-c3
  Scenario: A failed append is tried again
    Given the events crate cannot append
    When an inbox's batch interval passes
    Then a Store error is logged
    And the events are still on the heap
    When the events crate can append again and the next interval passes
    Then the append receives them
