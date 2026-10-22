# Generated from legion requirement FR-RANNOCH-058, revision 2026-10-17T00:36:22.189071+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A heartbeat's batch commits as one transaction, with each write able to roll back alone

  @criterion-01a1472c-dfda-75d3-a411-ee28d2863ef7 @module-write
  Scenario: one failing write rolls back alone
    Given an open graph with a batch of writes in its queue, one of which fails when committed
    When the heartbeat commits the batch
    Then every other write in the batch is committed

  @criterion-01a1472c-dfda-75d3-a411-ee38f49ce675 @module-write
  Scenario: a large batch lands whole
    Given an open graph with more than 1,000 changes in its queue
    When the heartbeat commits the batch while a reader reads
    Then the reader sees either none of the batch or all of it

  @criterion-01a14749-c42e-75e3-b47d-f02aa89e1ea5 @module-write
  Scenario: a link fails with the source it depends on
    Given a queue holding a node and then a link from it, where the node will fail at commit
    When the heartbeat commits the batch
    Then the link is not in the graph
