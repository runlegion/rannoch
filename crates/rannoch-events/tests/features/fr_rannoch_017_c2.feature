# Generated from FR-RANNOCH-017 (document 01a1d1f7-8b27-7f61-b69e-9d08e6d27d2a, revision 2). Do not hand-edit.
Feature: Plain reads for whoever runs the events

  @criterion-FR-RANNOCH-017-c2
  Scenario: Due events
    Given one waiting event whose run_at has passed and one whose run_at is an hour away
    When the events crate is asked for the waiting events due now
    Then it returns only the one whose run_at has passed
