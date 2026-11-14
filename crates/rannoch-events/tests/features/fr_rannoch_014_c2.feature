# Generated from FR-RANNOCH-014 (document 01a1d1da-57dd-7ee1-9fbc-a99c2389ec86, revision 2). Do not hand-edit.
Feature: A status update is a new row

  @criterion-FR-RANNOCH-014-c2
  Scenario: Back to the root
    Given a root event with two status rows
    When each status row's source is followed
    Then it leads to the root event
