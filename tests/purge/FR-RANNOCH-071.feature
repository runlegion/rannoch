# Generated from legion requirement FR-RANNOCH-071, revision 2026-10-26T18:27:07.845919+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: An app's forgetting rule purges a fact, a stream, a node and every node declared part of it, or a source, leaving nothing of it

  @criterion-01a1472c-f75d-7ec2-b4ff-18d20aec5b8f @module-purge
  Scenario: a purged fact leaves no event
    Given a graph holding a fact in its events
    When the app purges the fact
    Then no event in the store holds its key, kind, endpoints or properties

  @criterion-01a1472c-f75d-7ec2-b4ff-18e250dc2ab5 @module-purge
  Scenario: a purged fact leaves no snapshot
    Given a graph with a snapshot holding a fact
    When the app purges the fact
    Then no snapshot holds anything of it

  @criterion-01a1472c-f75d-7ec2-b4ff-18fad574c2de @module-purge
  Scenario: a purged stream leaves nothing
    Given a graph holding a stream
    When the app purges the stream
    Then nothing of the stream remains in the store

  @criterion-01a1472c-f75d-7ec2-b4ff-190255895f55 @module-purge
  Scenario: a purged node and its parts leave nothing
    Given a graph holding facts about one node and two nodes declared part of it
    When the app purges everything about that node
    Then nothing about the node or either of its parts remains in the store

  @criterion-01a14736-5af4-7a81-9692-a0f866636f3e @module-purge
  Scenario: a purged source leaves no hearing
    Given a source whose hearings back more than one node and link
    When the app purges that source
    Then no node or link holds a hearing from it

  @criterion-01a1472c-f75d-7ec2-b4ff-191d88d8aa0d @module-purge
  Scenario: a purge holds through a replay
    Given a graph from which the app has purged a fact
    When the graph is rebuilt
    Then the rebuilt graph holds nothing of the purged fact

  @criterion-01a1752f-215b-7ae2-a177-884d010476c1 @module-purge
  Scenario: a purge leaves no byte in the file
    Given a graph holding a fact whose property carries a unique token
    When the app purges the fact and the queue is closed
    Then a byte search of the store's files, its write-ahead log included, finds no copy of the token

  @criterion-01a17540-1d50-7ad0-a4e1-5b6d1c6ee7ce @module-purge
  Scenario: a purge leaves no identity or id of what it purged
    Given a graph holding a hearing written alone, and one write whose event names two hearings, each event id recorded by the test before the purge
    When the app purges the hearing written alone and one of the write's two hearings, and the queue is closed
    Then hashing each purged hearing's exact content, plainly and under every key found in the store's files, matches nothing in them
    And no id in the store's files equals the event id of the hearing written alone
    And a write naming the two-hearing write's event id as its derived-from source is accepted

  @criterion-01a17977-5af1-7df1-9c13-bac2fd04b0d6 @module-purge
  Scenario: purging one source's word about one node leaves the other sources
    Given a node heard from two sources
    When the app purges what the first source said about that node
    Then the node holds no hearing from the first source
    And it still holds every hearing from the second source
