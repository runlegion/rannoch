# Generated from legion requirement FR-RANNOCH-057, revision 2026-10-21T21:20:47.900140+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: Writes go to an in-memory queue that a heartbeat commits, then the committed watermark advances and subscribers fire

  @criterion-01a1472c-de0a-7a72-b7ba-a7382f50781f @module-queue
  Scenario: an accepted write returns before it is committed
    Given an open graph with its queue
    When the app writes a node and the write returns
    Then the committed watermark has not yet passed that write

  @criterion-01a1472c-de0a-7a72-b7ba-a7403bdc9940 @module-queue
  Scenario: the heartbeat commits the queue
    Given an open graph with an accepted write in its queue
    When the next heartbeat runs
    Then the committed watermark has passed that write

  @criterion-01a1472c-de0a-7a72-b7ba-a75b7b80f29e @module-queue
  Scenario: a subscriber sees only committed events
    Given an open graph with a subscriber
    When the app writes a node and the subscriber receives its event
    Then the committed watermark has passed that event

  @criterion-01a1472c-de0a-7a72-b7ba-a7680c0a8dab @module-queue
  Scenario: a flush commits now
    Given an open graph with accepted writes in its queue
    When the app flushes
    Then every write accepted before the flush is committed when the flush returns
