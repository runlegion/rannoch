# Generated from FR-RANNOCH-013 (document 01a1d1da-5774-7321-abaf-abf635139e13, revision 17), FR-RANNOCH-014 (document 01a1d1da-57dd-7ee1-9fbc-a99c2389ec86, revision 8) and FR-RANNOCH-015 (document 01a1d1e2-748f-72f1-88f7-1dae940cb8ad, revision 7). Do not hand-edit.
Feature: The events crate keeps each batch it is handed and tells the queue

  @module-rannoch-events @criterion-FR-RANNOCH-013-c2 @criterion-FR-RANNOCH-014-c1 @criterion-FR-RANNOCH-014-c2 @criterion-FR-RANNOCH-015-c1 @criterion-FR-RANNOCH-015-c2 @criterion-FR-RANNOCH-015-c3
  Scenario: A batch handed to the events crate is kept and the queue is told
    Given a batch of accepted events from the app's forgr factory, one of them a status update for another
    When the batch is handed to the events crate over forgr's empty store
    Then each event is in _events as one row with all its fields, in one commit
    And the status update is a new row whose source is its event, and that event's own row is unchanged
    And the queue is signalled after the commit, and not before
    When a batch is handed to the events crate over a store that cannot be written
    Then nothing is kept and the queue is not signalled
