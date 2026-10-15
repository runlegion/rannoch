# Generated from legion requirement FR-RANNOCH-032, revision 2026-10-14T19:39:17.001130+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Declaring a class, relation or property is one event that lands whole or not at all

  @criterion-01a13963-03da-7c12-b97d-78900d012013 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: each declaration is one event
    Given an empty graph
    When the app declares 5 classes and 4 relations
    Then reading the events from the start returns 9 declaration events

  @criterion-01a13974-4fe1-79f2-8448-45f3411a7aa6 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: declared classes and relations appear in the ontology
    Given an empty graph
    When the app declares 5 classes and 4 relations
    Then the graph's ontology lists those 5 classes and 4 relations

  @criterion-01a13974-4fe1-79f2-8448-46043b521953 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: adding a property is one event
    Given a declared class statement with property text
    When the app declares property need on statement
    Then exactly one declaration event is written

  @criterion-01a13963-03da-7c12-b97d-78aea017ddd8 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a property added mid-stream reads absent on older instances
    Given a declared class statement with property text and one statement written
    When the app declares property need on statement
    Then the statement written before reads need as absent

  @criterion-01a13963-03da-7c12-b97d-78be81a5ab9b @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a refused declaration leaves no event behind
    Given a graph with class voice declared with no properties
    When the app declares class voice with property extra
    Then the number of events is unchanged

  @criterion-01a13974-4fe1-79f2-8448-461b6d668137 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a refused declaration leaves the ontology as it was
    Given a graph with class voice declared with no properties
    When the app declares class voice with property extra
    Then the graph's ontology still lists voice with no properties
