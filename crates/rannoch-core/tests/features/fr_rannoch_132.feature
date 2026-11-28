# Generated from FR-RANNOCH-132 (revision 6). Do not hand-edit.
Feature: Events move through lanes: the inbox stamps, the broker makes each lane durable in batches, and each lane has its own cursor

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5ead86437b09
  Scenario: Ids from one Rannoch keep their order
    Given two Rannochs in one process, each in its own frozen test running in parallel
    When each sends events in the same frozen millisecond
    Then each Rannoch's ids strictly increase in the order its events were sent

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5eb888c8b826
  Scenario: Each event goes to its lane, and run_at events to the scheduled lane
    Given a settings file whose lane table lists ty 12 in lane 'items', a ty 30 that no lane lists, and a ty 12 event with a run_at an hour away
    When events of ty 12 and 30 and the scheduled event are sent, and the clock advances an hour
    Then the ty 12 event is in lane 'items', the ty 30 event in the default lane, and the scheduled event in the scheduled lane, run only once its run_at comes and after the other lanes' due events

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5ecea85b1e45
  Scenario: A lane writes at its limit or heartbeat, alone
    Given lane 'big' with a 1 MB memory limit and lane 'small' with a 50 ms heartbeat
    When a 900 KB event is sent to 'big' and small events to 'small', and the clock advances 50 ms
    Then the small events are in _events, and the big event is written when its lane's limit or heartbeat comes, without delaying the small ones

  @module-rannoch-core @criterion-01a2213c-d71c-7f33-8a94-5edb61993409
  Scenario: A lane's events reach _events in id order
    Given one lane receiving events from several producers
    When its batches are written
    Then the lane's events are in _events in increasing id order

  @module-rannoch-core @criterion-01a2214e-8662-7f93-aff9-aadc64160e4c
  Scenario: Shutdown writes every lane
    Given two lanes, each with events accepted but not yet written, their heartbeats not yet expired
    When rannoch shuts down
    Then every accepted event is in _events when shutdown returns
