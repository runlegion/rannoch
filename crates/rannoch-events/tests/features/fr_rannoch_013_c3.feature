# Generated from FR-RANNOCH-013 (document 01a1d1da-5774-7321-abaf-abf635139e13, revision 3). Do not hand-edit.
Feature: The events crate owns _events

  @criterion-FR-RANNOCH-013-c3
  Scenario: Status outside its four values
    When a row with status 3 is written to _events
    Then the write is refused
