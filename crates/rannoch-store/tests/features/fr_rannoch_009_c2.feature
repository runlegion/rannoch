# Generated from FR-RANNOCH-009 (document 01a1cdc9-8418-7310-aafd-ae2bf4ea1d2a, revision 2). Do not hand-edit.
Feature: Open: the store opens the app's database, reports pending migrations, and gives the connection

  @criterion-FR-RANNOCH-009-c2
  Scenario: Open a database with migrations pending
    Given a database missing the newest migration
    When the store opens it
    Then it reports that migration as pending
    And the database is unchanged
