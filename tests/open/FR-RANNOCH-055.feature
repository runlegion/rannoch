# Generated from legion requirement FR-RANNOCH-055, revision 2026-10-25T23:16:17.976731+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A class declares each property's value type and the key its history is ordered by, and every write carries its point

  @criterion-01a1472c-da6e-7db3-9deb-520bff55e0c3 @module-open
  Scenario: a class keeps its declared history key
    Given an empty graph
    When the app declares a class whose history is ordered by an integer key the writer supplies
    Then the graph's ontology names a writer-supplied integer key as that class's history order

  @criterion-01a15d54-07c5-79f2-b960-954ac2bad444 @module-open
  Scenario: a class with no declared key is ordered by its origin's record time
    Given an empty graph
    When the app declares a class without a history key
    Then the graph's ontology names the origin's record time as that class's history order

  @criterion-01a1472c-da6e-7db3-9deb-5212100deb1d @module-open
  Scenario: an integer stays an integer
    Given a class with a property declared as an integer
    When the app writes a node of that class with 9007199254740993 for the property and reads it back
    Then the property reads back as the integer 9007199254740993

  @criterion-01a15d54-07c5-79f2-b960-955da14e8c78 @module-open
  Scenario: a decimal keeps every digit
    Given a class with a property declared as a decimal
    When the app writes a node of that class with 12345678901234567890.0000000001 for the property and reads it back
    Then the property reads back as the decimal 12345678901234567890.0000000001

  @criterion-01a15d54-07c5-79f2-b960-9562aa6e1060 @module-open
  Scenario: a float vector keeps its length and values
    Given a class with a property declared as a float vector of length 3
    When the app writes a node of that class with 1.5, -2.25 and 0.125 for the property and reads it back
    Then the property reads back as a float vector of length 3 holding 1.5, -2.25 and 0.125

  @criterion-01a15d54-07c5-79f2-b960-9570acf002f2 @module-open
  Scenario: a store from before value types opens whole
    Given a store written by rannoch 0.1.1 with a class, its properties and nodes
    When the store is opened
    Then every event it held is still read, each of the class's properties reads as text and each hearing reads back a point

  @criterion-01a1752f-211b-7bf6-a321-10b9ef5202bf @module-open
  Scenario: a store from 0.2.0 opens whole
    Given a store written by rannoch 0.2.0 with classes and hearings
    When the store is opened
    Then every event it held is still read and each hearing reads back a point
