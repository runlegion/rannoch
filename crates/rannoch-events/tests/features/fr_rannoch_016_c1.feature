# Generated from FR-RANNOCH-016 (document 01a1d1f7-8ab5-7251-9528-cc9042cc63c0, revision 2). Do not hand-edit.
Feature: The status function: a root and its tree

  @criterion-FR-RANNOCH-016-c1
  Scenario: A root and its tree
    Given a root event with two status rows and one event its flow produced
    When the status function is asked for the root's id
    Then it returns the root, its two status rows and the produced event
