# Generated from FR-RANNOCH-017 (document 01a1d1f7-8b27-7f61-b69e-9d08e6d27d2a, revision 2). Do not hand-edit.
Feature: Plain reads for whoever runs the events

  @criterion-FR-RANNOCH-017-c1
  Scenario: Waiting events of one type
    Given two events of type Page that nothing has changed, and one event of type Page with a state change to 2
    When the events crate is asked for the waiting events of type Page
    Then it returns the two that nothing has changed
