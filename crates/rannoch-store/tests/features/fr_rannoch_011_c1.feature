# Generated from FR-RANNOCH-011 (document 01a1cdc9-84d2-7d10-abdf-677b39e645c7, revision 4). Do not hand-edit.
Feature: Each crate's SQL lives in one queries.rs, run on the store's connection and checked by forgr

  @criterion-FR-RANNOCH-011-c1
  Scenario: A crate's query on the store's connection
    Given an open store
    When a crate runs one of its queries on the connection
    Then the query answers from the store
