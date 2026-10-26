# Generated from legion requirement FR-RANNOCH-065, revision 2026-10-25T23:26:13.575528+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Identical content from the same source is one hearing in every class; another source or other content adds a hearing

  @criterion-01a17542-a7dc-70d1-bf8c-48644286c556 @module-events
  Scenario: a rebuild refuses a key the graph was not written under
    Given a graph written under one content-identity key, with a snapshot
    When the graph is rebuilt from its events under another key, and from its snapshot under another key
    Then both rebuilds are refused, and each refusal says the key differs from the graph's
