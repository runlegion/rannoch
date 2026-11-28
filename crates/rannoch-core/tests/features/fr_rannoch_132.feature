# Generated from FR-RANNOCH-132 (revision 8). Do not hand-edit.
Feature: Events move through lanes: the inbox stamps, the broker makes each lane durable in batches, and each lane has its own cursor

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5ead86437b09
  Scenario: Ids from one Rannoch keep their order
    Given two Rannochs in one process, each in its own frozen test running in parallel
    When each sends events in the same frozen millisecond
    Then each Rannoch's ids strictly increase in the order its events were sent

  @module-rannoch-core @criterion-01a2216c-9039-7ec3-b6d9-0c700d1299b1
  Scenario: Ids from new_id keep their order across frozen clocks
    Given two paused runtimes in one process, one clock an hour ahead of the other
    When they take turns making ids with clock::new_id in one frozen millisecond each
    Then all the ids, in the order they were made, strictly increase
