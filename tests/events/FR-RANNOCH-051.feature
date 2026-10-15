# Generated from legion requirement FR-RANNOCH-051, revision 2026-10-14T08:38:17.300447+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A graph file is rebuilt from its events alone

  @criterion-01a13974-698c-7f83-aa87-fc4f9f15de5d @module-events
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: every event replays
    Given a graph whose stream holds 36 events, 9 of them declarations
    When the app rebuilds a file from those events into an empty path
    Then none of the 36 events fails to land

  @criterion-01a13974-698c-7f83-aa87-fc502ec854b8 @module-events
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: the ontology comes back from the stream
    Given a graph whose stream holds 36 events, 9 of them declarations
    When the app rebuilds a file from those events into an empty path
    Then the rebuilt graph's ontology lists the same classes and relations as the original

  @criterion-01a13974-698c-7f83-aa87-fc673d7a375d @module-events
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: the instances come back from the stream
    Given a graph whose stream holds 36 events, 9 of them declarations
    When the app rebuilds a file from those events into an empty path
    Then the rebuilt graph holds the same nodes and links as the original

  @criterion-01a13974-698c-7f83-aa87-fc768c59acba @module-events
    # fixture: T5b's shape, as T5's with some parents dropped from the stream so their links never close
  Scenario: links come back from the stream
    Given a graph written from a reply stream in which some parents never arrive
    When the app rebuilds a file from its events into an empty path
    Then the rebuilt graph's answers links are the original's

  @criterion-01a13974-698c-7f83-aa87-fc882ba43312 @module-events
    # fixture: T5b's shape, as T5's with some parents dropped from the stream so their links never close
  Scenario: open links come back from the stream
    Given a graph written from a reply stream in which some parents never arrive
    When the app rebuilds a file from its events into an empty path
    Then the rebuilt graph's open links are the original's
