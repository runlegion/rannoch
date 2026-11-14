# Generated from FR-RANNOCH-008 (document 01a1cdc9-83ae-70a2-836f-dd4d3d875208, revision 6). Do not hand-edit.
Feature: Schema: every crate's migrations, gathered by the app's build.rs into one committed schema

  @criterion-FR-RANNOCH-008-c2
  Scenario: A crate's new migration reaches the store
    Given an app built on rannoch whose schema.json is current
    When a developer adds a migration creating a table to one of the app's crates and builds again
    Then the app's committed migrations directory holds the new migration
    And its schema.json names the new table
