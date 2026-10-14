# Generated from legion requirement FR-RANNOCH-033, revision 2026-10-14T08:08:26.897235+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An identical declaration is a no-op; a conflicting one is refused in ontology terms

  @criterion-01a13963-0683-7650-b1f3-5e77c07a881e @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: declaring on every open writes nothing new
    Given a graph whose ontology holds 9 declarations
    When the app opens it and declares the same 9 again
    Then every declaration returns Ok
    And no event is written

  @criterion-01a13974-515e-7fd1-9c86-28f2327c812b @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a class's properties are a set
    Given class statement is declared with properties text and need
    When the app declares class statement with properties need and text
    Then no event is written

  @criterion-01a13974-515e-7fd1-9c86-29030baf42ec @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: identical means identical to the ontology as it is now
    Given class statement is declared with properties text and need, need added mid-stream
    When the app declares class statement with property text alone
    Then the declaration is refused

  @criterion-01a13963-0683-7650-b1f3-5e8eaade037c @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a conflicting class is refused in ontology terms
    Given class voice is declared with no properties
    When the app declares class voice with property extra
    Then the declaration is refused with a reason naming class voice, its properties and the declared properties
    And the reason contains no table name and no SQL text

  @criterion-01a13963-0683-7650-b1f3-5e9271c25299 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a conflicting relation is refused in ontology terms
    Given relation said is declared from voice to statement
    When the app declares relation said from statement to voice
    Then the declaration is refused with a reason naming relation said, its ends and the declared ends
    And the reason contains no table name and no SQL text

  @criterion-01a13974-515e-7fd1-9c86-291a3c729d03 @module-write
    # fixture: the T9b declarations and instances (t9b-direct, 2026-10-14): 5 classes voice, statement, finding, place, conversation; 4 relations said, supports, inside, at; 36 events
  Scenario: a relation to a class that does not exist is refused
    Given no class tweet is declared
    When the app declares relation quotes from statement to tweet
    Then the declaration is refused with a reason naming class tweet
