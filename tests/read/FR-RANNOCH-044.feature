# Generated from legion requirement FR-RANNOCH-044, revision 2026-10-14T19:39:22.181789+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Reads answer in ontology terms

  @criterion-01a13963-15cf-79d3-ab12-16b4a90383c9 @module-read
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: describe answers class and properties
    Given the T9b graph
    When the app describes statement s1
    Then the answer gives class statement and property text

  @criterion-01a13963-15cf-79d3-ab12-16c68a2b17e1 @module-read
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: describe answers relations both ways with counts
    Given the T9b graph
    When the app describes statement s1
    Then the answer gives relations out inside 1 and supports 1
    And relations in said 1

  @criterion-01a13963-15cf-79d3-ab12-16da970faaa1 @module-read
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: describe answers containment
    Given the T9b graph, with inside and at declared as containment
    When the app describes statement s1
    Then the answer gives s1 part of conversation c1 via inside
    And part of place swtor-tc via at

  @criterion-01a13963-15cf-79d3-ab12-16e21940e49a @module-read
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: describe names no table
    Given the T9b graph
    When the app describes statement s1
    Then no table name and no column name appears in the answer

  @criterion-01a13bec-8b96-7205-a97b-a4b4b51bfebc @module-read
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14)
  Scenario: no read or refusal names a table
    Given the T9b graph
    When the app runs a find, a walk, a count and an open-links read, and writes a link between the wrong classes
    Then no table name and no column name appears in any answer or in the refusal
