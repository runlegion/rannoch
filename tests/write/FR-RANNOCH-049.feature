# Generated from legion requirement FR-RANNOCH-049, revision 2026-10-14T08:38:16.921855+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An open-links read lists every link still waiting, with its target kind and key

  @criterion-01a13974-66c3-7873-a73c-a33ccd6d486e @module-write
  Scenario: a wrong-kind arrival does not close the link
    Given an answers link from statement s2 waiting on statement key s1
    When the app writes a voice under key s1
    Then the open links still list the link from s2 waiting on statement s1

  @criterion-01a13974-66c3-7873-a73c-a34c40480609 @module-write
  Scenario: a wrong-kind arrival is reported
    Given an answers link from statement s2 waiting on statement key s1
    When the app writes a voice under key s1
    Then the write's result reports a wrong-kind arrival under key s1, naming the open link
