# Generated from legion requirement FR-RANNOCH-084, revision 2026-10-26T11:06:01.133814+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: The same events in any arrival order give the same answers, except first recorded

  @criterion-01a1752f-2126-7ef3-8e7a-1fc5f265519a @module-history
  Scenario: shuffled arrival gives the same answers
    Given a graph holding nodes and links of a time-keyed class and of a class with no key, some of whose versions arrived after newer ones
    When its declarations and then its other events are given to 20 new graphs, the other events in a different random order in each, some links arriving before their ends
    Then every graph returns the same answers as the original to find, state at a point, reads as of a valid time, first and last heard, and counts, for every node and link

  @criterion-01a17540-2219-7e60-ad01-4420aefe6364 @module-history
  Scenario: a hearing delivered twice is named by its smaller id in any order
    Given a class whose history is ordered by a time the writer supplies
    And one hearing's content delivered twice, under event ids A and B, A the smaller
    When one new graph is given A then B and another new graph is given B then A
    Then both graphs name the hearing by A

  @criterion-01a17540-2219-7e60-ad01-4434d39dc772 @module-history
  Scenario: a tie does not depend on which delivery arrived first
    Given a class whose history is ordered by a time the writer supplies
    And one hearing's content delivered twice, under event ids A and B, A the smaller
    And another version of the same node at the same point, with other properties, under an event id between A and B
    When one new graph is given A, the other version, then B, and another new graph is given B, the other version, then A
    Then find returns the other version as the node's state in both graphs

  @criterion-01a17759-c275-706d-ae91-072c9950dc21 @module-history
  Scenario: when a link ended and first true do not depend on arrival
    Given a graph of a time-keyed class holding links that have ended, one of them ended twice at the same point under different event ids, and nodes heard with different valid times, some versions arriving after newer ones
    When its declarations and then its other events are given to 20 new graphs, the other events in a different random order in each
    Then every graph returns the same answer as the original to when each link ended and to each node's and link's first true

  @criterion-01a17771-f692-7a92-8518-e5e663c774ae @module-history
  Scenario: a node's replacement fate does not depend on arrival
    Given a graph of a time-keyed class holding one node replaced twice at the same point under different event ids, and another node replaced twice at different points
    When its declarations and then its other events are given to 20 new graphs, the other events in a different random order in each
    Then every graph returns the same fate as the original for each node

  @criterion-01a177e2-0e9c-7b95-b42b-27c49294a64d @module-history
  Scenario: a link ended twice in a row ended at the earlier ending
    Given a class whose history is ordered by a time the writer supplies
    And a link written at point 10, ended at point 20, written again at point 30, and ended at point 40 and again at point 50
    When one new graph is given these events in that order and another new graph is given them in the opposite order
    Then both graphs return point 40 as when the link ended
