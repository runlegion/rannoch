# Generated from FR-RANNOCH-013 (document 01a1d1da-5774-7321-abaf-abf635139e13, revision 3). Do not hand-edit.
Feature: The events crate owns _events

  @criterion-FR-RANNOCH-013-c2
  Scenario: An event as a row
    Given an accepted event
    When it is written to _events
    Then its row holds its id, source, ty, data, run_at and status
