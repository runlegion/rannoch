# Generated from legion requirement FR-RANNOCH-034, revision 2026-10-14T19:39:19.277435+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A link holds only between nodes of its relation's declared classes

  @criterion-01a13963-07e2-7a80-907d-83bb0b71c022 @module-read
    # fixture: T5b's shape, as T5's with some parents dropped from the stream so their links never close
  Scenario: no read sees a dangling link during a burst
    Given the listener is writing a reply stream in which some parents never arrive
    When a reader walks answers links during the burst
    Then no answers link it finds points at a missing statement
