# Generated from FR-RANNOCH-008 (document 01a1cdc9-83ae-70a2-836f-dd4d3d875208, revision 6). Do not hand-edit.
Feature: Schema: every crate's migrations, gathered by the app's build.rs into one committed schema

  @criterion-FR-RANNOCH-008-c5
  Scenario: Another crate tests against the app's schema
    Given an app whose schema.json names a table
    When another crate's test opens the app's empty test store and builds a row for that table through the app's forgr module
    Then the row is in the store
    And that crate has no forgr dependency of its own
