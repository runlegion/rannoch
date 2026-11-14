# Generated from FR-RANNOCH-009 (document 01a1cdc9-8418-7310-aafd-ae2bf4ea1d2a, revision 2). Do not hand-edit.
Feature: Open: the store opens the app's database, reports pending migrations, and gives the connection

  @criterion-FR-RANNOCH-009-c3
  Scenario: A database that cannot be opened
    Given a path the store cannot open
    When the store opens it
    Then open fails with Store
