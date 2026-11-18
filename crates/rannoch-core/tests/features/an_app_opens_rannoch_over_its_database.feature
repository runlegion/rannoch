# Generated from FR-RANNOCH-009 (document 01a1cdc9-8418-7310-aafd-ae2bf4ea1d2a, revision 11). Do not hand-edit.
Feature: Open: the store opens the app's database, reports pending migrations, and gives the connection

  @module-rannoch-core @criterion-FR-RANNOCH-009-c1 @criterion-FR-RANNOCH-009-c2 @criterion-FR-RANNOCH-009-c3 @criterion-01a1eba7-0d8b-79a3-af1e-092bba462585
  Scenario: An app opens rannoch over its database
    Given an app's database without its newest migration
    When the app opens rannoch with its store
    Then the newest migration is applied before rannoch accepts an event
    And every connection the store opens has foreign keys on, journal mode WAL, a 64 MiB cache, automatic checkpoints off and fullfsync on
    When the app sends events and then sends nothing for two seconds
    Then the WAL's frames are in the database file
    When the app opens rannoch over a path the store cannot open
    Then open fails with Store and nothing starts
