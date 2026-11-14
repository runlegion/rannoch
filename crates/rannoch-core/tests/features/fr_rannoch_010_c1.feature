# Generated from FR-RANNOCH-010 (document 01a1cdc9-8477-7b01-9dbf-44be96077c70, revision 3). Do not hand-edit.
Feature: Core applies pending migrations at start, before anything runs

  @criterion-FR-RANNOCH-010-c1
  Scenario: Pending migrations are applied at start
    Given a database missing the newest migration
    When the app opens rannoch
    Then the newest migration is applied
    And rannoch is open
