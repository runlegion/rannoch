# Generated from FR-RANNOCH-011 (document 01a1cdc9-84d2-7d10-abdf-677b39e645c7, revision 4). Do not hand-edit.
Feature: Each crate's SQL lives in one queries.rs, run on the store's connection and checked by forgr

  @criterion-FR-RANNOCH-011-c2
  Scenario: A query drifts from the schema
    Given a crate whose queries all match the app's schema
    When a developer renames a column in a migration and builds again
    Then cargo test fails at the query that still names the old column
