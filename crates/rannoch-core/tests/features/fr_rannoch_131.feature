# Generated from FR-RANNOCH-131 (revision 3). Do not hand-edit.
Feature: rannoch has one clock, on tokio's time, and tests freeze it

  @module-rannoch-core @criterion-01a220b8-d5bd-7151-baf4-97805e91b4eb
  Scenario: A frozen clock does not move
    Given a test running with the clock frozen
    When it reads the clock's wall time, sends an event, waits on nothing and reads the wall time and sends an event again
    Then both wall times are equal and both events' ids carry that same time

  @module-rannoch-core @criterion-01a220b8-d5bd-7151-baf4-979f2d9dc536
  Scenario: Advancing the clock moves time exactly and wakes the waits it passes
    Given a frozen clock, and two waits with deadlines 50 ms and 200 ms from now
    When the test advances the clock by 100 ms
    Then the wall time, the elapsed time and a new id's time have each moved by exactly 100 ms, the 50 ms wait has woken and the 200 ms wait has not
