# Generated from legion requirement FR-RANNOCH-065, revision 2026-10-26T18:05:14.099304+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Identical content from the same source is one hearing in every class; another source or other content adds a hearing

  @criterion-01a17540-1bc7-7242-a4c9-c039f9ab1de8 @module-open
  Scenario: a graph refuses a key it was not opened with
    Given a graph first opened under one content-identity key, written to and closed
    When the app opens the graph under another key
    Then the open is refused, and the refusal says the key differs from the graph's

  @criterion-01a17559-91f6-7952-8cf2-437e1462e0fe @module-open
  Scenario: a graph refuses to open without its key
    Given a graph first opened under a content-identity key, written to and closed
    When the app opens the graph without a content-identity key
    Then the open is refused, and the refusal says the key is missing
