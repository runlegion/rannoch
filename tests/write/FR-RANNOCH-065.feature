# Generated from legion requirement FR-RANNOCH-065, revision 2026-10-25T23:26:13.575528+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Identical content from the same source is one hearing in every class; another source or other content adds a hearing

  @criterion-01a14736-441d-7220-af7f-d7c7696e2afd @module-write
  Scenario: restating a node adds a hearing, not a node
    Given a node that has been heard from one source
    When the app restates it from another source
    Then the graph holds one node for it

  @criterion-01a14736-441d-7220-af7f-d7dea85f8463 @module-write
  Scenario: restating a link adds a hearing
    Given a link that has been heard from one source
    When the app restates it from another source
    Then the link has a hearing for each source it was heard from

  @criterion-01a14736-441d-7220-af7f-d7e500eb1b59 @module-write
  Scenario: an identical hearing from another store is held as an alias
    Given a class whose history is ordered by a time the writer supplies
    And one hearing's content delivered twice, under event ids A and B, A the smaller
    When a new graph is given A then B
    Then the node has one hearing, and B is held as an alias of it

  @criterion-01a17559-91f3-7d9b-9fe9-29bd3a9349f3 @module-write
  Scenario: unchanged content written again adds nothing in a class with no key
    Given a class declared without a history key
    And a node heard once from one source
    When the same source writes the node again later with the same valid time and properties
    Then the number of events is unchanged, the node has one hearing at the point it had before, and the second write returns the first write's event id

  @criterion-01a1472c-ec82-72e1-a504-44414ceb8a01 @module-write
  Scenario: a restatement keeps first recorded and first true
    Given a node that has been heard from one source
    When the app restates it from another source with other properties and a later valid time
    Then the node's first recorded and first true are those of the first hearing

  @criterion-01a1752f-2132-7294-9d5a-a08244b09074 @module-write
  Scenario: a source's changed account adds a hearing
    Given a class whose history is ordered by a time the writer supplies
    And a node heard once from one source
    When the app writes the node again from the same source at the same point with one property changed
    Then the node has two hearings

  @criterion-01a1752f-2133-79d3-9df5-7b193ca8f3d8 @module-write
  Scenario: event ids are UUIDv7, not content hashes
    Given two new graphs with the same ontology
    When the app writes the same hearing to each graph
    Then each hearing's event id is a version 7 UUID and the two event ids differ

  @criterion-01a1752f-2134-7611-bc39-7baa7777e23b @module-write
  Scenario: the content identity depends on the key
    Given three new graphs with the same ontology, two with one content-identity key and one with another key
    When the app writes the same hearing to each graph
    Then the two graphs with the same key hold it under the same content identity and the graph with the other key holds it under a different one

  @criterion-01a17540-1bc7-7242-a4c9-c0208e05af0e @module-write
  Scenario: the content-identity key stays out of the store's files
    Given a graph opened under a content-identity key the app supplies
    When the app writes hearings, takes a snapshot and closes the queue
    Then a byte search of the store's files, its write-ahead log and snapshots included, finds no copy of the key
