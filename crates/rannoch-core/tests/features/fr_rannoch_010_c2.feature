# Generated from FR-RANNOCH-010 (document 01a1cdc9-8477-7b01-9dbf-44be96077c70, revision 3). Do not hand-edit.
Feature: Core applies pending migrations at start, before anything runs

  @criterion-FR-RANNOCH-010-c2
  Scenario: A migration that fails to apply
    Given a pending migration that cannot be applied
    When the app opens rannoch
    Then open fails with Store
    And nothing has started
