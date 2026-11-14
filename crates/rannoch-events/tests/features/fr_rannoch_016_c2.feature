# Generated from FR-RANNOCH-016 (document 01a1d1f7-8ab5-7251-9528-cc9042cc63c0, revision 2). Do not hand-edit.
Feature: The status function: a root and its tree

  @criterion-FR-RANNOCH-016-c2
  Scenario: The current status
    Given a root event whose status rows are 1 and then 2
    When the status function is asked for the root's id
    Then the root's current status is 2
