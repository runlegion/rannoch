# Generated from legion requirement FR-RANNOCH-031, revision 2026-10-14T08:08:26.162430+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A graph opens from its file, empty when new

  @criterion-01a13962-efea-7ef0-acde-7b93217b6689 @module-open
  Scenario: a new graph's ontology is empty
    Given a path that holds no graph file
    When the app opens it
    Then the graph's ontology lists no class and no relation

  @criterion-01a13974-4e53-7850-955b-26f79997831a @module-open
  Scenario: a new graph's event log is empty
    Given a path that holds no graph file
    When the app opens it and reads the events from the start
    Then it receives no event

  @criterion-01a13962-efea-7ef0-acde-7ba7ab593599 @module-open
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: the ontology comes from the file's own stream
    Given a graph file whose stream declared 5 classes and 4 relations
    When the app opens it and declares nothing
    Then the graph's ontology lists those 5 classes and 4 relations
