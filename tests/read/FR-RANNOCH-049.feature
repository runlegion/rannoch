# Generated from legion requirement FR-RANNOCH-049, revision 2026-10-14T08:38:16.921855+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An open-links read lists every link still waiting, with its target kind and key

  @criterion-01a13974-66c3-7873-a73c-a31d73bd7bff @module-read
    # fixture: T5b's shape, as T5's with some parents dropped from the stream so their links never close
  Scenario: the graph says what is missing
    Given a reply stream in which some parents never arrive
    When the listener writes the whole stream and the app reads the open links
    Then the open links listed are exactly the links to the parents that never arrived

  @criterion-01a13974-66c3-7873-a73c-a321694c1b10 @module-read
  Scenario: an open link names what it waits on
    Given an answers link from statement s2 waiting on statement key s1, which is not in the graph
    When the app reads the open links
    Then the link is listed with target kind statement and target key s1
