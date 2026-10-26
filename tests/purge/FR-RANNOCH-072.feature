# Generated from legion requirement FR-RANNOCH-072, revision 2026-10-26T09:02:11.141355+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A purged hearing is as if never recorded, including one that arrives after its purge rule

  @criterion-01a14736-5ffa-7eb2-980c-c6136e1f320e @module-purge
  Scenario: first heard moves past a purged hearing
    Given a fact heard from more than one source
    When the app purges the hearing earliest on the class's declared axis
    Then the fact's first heard is the earliest surviving hearing on the axis

  @criterion-01a14760-5047-7210-99b0-ac4e28e90288 @module-purge
  Scenario: last heard moves past a purged hearing
    Given a fact heard from more than one source
    When the app purges the hearing latest on the class's declared axis
    Then the fact's last heard is the latest surviving hearing on the axis

  @criterion-01a14736-5ffa-7eb2-980c-c621fe06696c @module-purge
  Scenario: the distinct count drops with a purge
    Given a fact heard by more than one voice, one of them only once
    When the app purges that voice's only hearing and reads the distinct count of voices
    Then the count no longer counts that voice

  @criterion-01a1752f-215c-74b2-afac-6d40960caf18 @module-purge
  Scenario: a purge rule purges what arrives after it
    Given a fact heard from two sources, and a purge rule for one of them
    When the app writes a hearing of the fact from the purged source
    Then nothing of that hearing remains in the store, and the fact's first heard, last heard and distinct count are those of the surviving source alone

  @criterion-01a17540-1ded-7b72-898c-b605fa0d3737 @module-purge
  Scenario: a standing rule keeps matching without keeping its target
    Given a graph holding a node whose key is a unique token
    When the app purges the node, takes a snapshot and closes the queue
    Then a byte search of the store's files, its write-ahead log and snapshots included, finds no copy of the key
    And hashing the key under every key found in the store's files matches nothing in them
    When the graph is reopened and the app writes a hearing of that node
    Then nothing of that hearing remains in the store

  @criterion-01a17759-c272-7e50-8352-69c3266506e7 @module-purge
  Scenario: a graph rebuilt from a snapshot keeps purging
    Given a fact heard from two sources, a purge rule for one of them, and a snapshot taken after the purge
    When the graph is rebuilt from the snapshot and the app writes a hearing of the fact from the purged source
    Then nothing of that hearing remains in the rebuilt graph, and the fact's first heard, last heard and distinct count are those of the surviving source alone

  @criterion-01a17771-f4fa-7bb3-b84e-a79dfc7bf182 @module-purge
  Scenario: first recorded moves past a purged hearing
    Given a fact heard from more than one source
    When the app purges the hearing this store recorded first
    Then the fact's first recorded is the surviving hearing this store recorded first
