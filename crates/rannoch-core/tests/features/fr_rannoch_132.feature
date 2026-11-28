# Generated from FR-RANNOCH-132 (revision 6). Do not hand-edit.
Feature: Events move through lanes: the inbox stamps, the broker makes each lane durable in batches, and each lane has its own cursor

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5ead86437b09
  Scenario: Ids from one Rannoch keep their order
    Given two Rannochs in one process, each in its own frozen test running in parallel
    When each sends events in the same frozen millisecond
    Then each Rannoch's ids strictly increase in the order its events were sent
