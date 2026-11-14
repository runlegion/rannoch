# Generated from FR-RANNOCH-005 (document 01a1c850-f36c-7970-aff2-7df7bfbfe13c, revision 3). Do not hand-edit.
Feature: The batch: the inbox hands each batch to the events crate

  @criterion-FR-RANNOCH-005-c2
  Scenario: A crash loses the heap
    Given events on an inbox's heap
    When the process stops before the batch is handed over
    Then those events are not in _events
