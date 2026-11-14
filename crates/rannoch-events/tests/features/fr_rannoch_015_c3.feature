# Generated from FR-RANNOCH-015 (document 01a1d1e2-748f-72f1-88f7-1dae940cb8ad, revision 1). Do not hand-edit.
Feature: The events crate appends each batch and signals the queue when it has committed

  @criterion-FR-RANNOCH-015-c3
  Scenario: A failed append
    Given the events crate cannot write
    When it appends a batch
    Then the error is Store
    And the queue is not signalled
