# Generated from FR-RANNOCH-015 (document 01a1d1e2-748f-72f1-88f7-1dae940cb8ad, revision 1). Do not hand-edit.
Feature: The events crate appends each batch and signals the queue when it has committed

  @criterion-FR-RANNOCH-015-c2
  Scenario: The queue is signalled after the commit
    Given a batch of events
    When the events crate appends it
    Then the queue is signalled
    And every event in the batch is in _events when the signal arrives
