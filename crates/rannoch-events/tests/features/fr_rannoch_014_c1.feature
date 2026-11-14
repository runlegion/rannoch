# Generated from FR-RANNOCH-014 (document 01a1d1da-57dd-7ee1-9fbc-a99c2389ec86, revision 2). Do not hand-edit.
Feature: A status update is a new row

  @criterion-FR-RANNOCH-014-c1
  Scenario: A status update
    Given a root event in _events with status 0
    When its status moves to 2
    Then _events holds a new row with a new id, status 2 and the root's id as its source
    And the root's row still has status 0
