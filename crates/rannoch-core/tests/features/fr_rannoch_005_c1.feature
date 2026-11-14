# Generated from FR-RANNOCH-005 (document 01a1c850-f36c-7970-aff2-7df7bfbfe13c, revision 3). Do not hand-edit.
Feature: The batch: the inbox hands each batch to the events crate

  @criterion-FR-RANNOCH-005-c1
  Scenario: An inbox hands over its batch on its interval
    Given events on an inbox's heap
    When the inbox's batch interval passes
    Then the events crate's append receives them as one batch
    And the heap is empty
