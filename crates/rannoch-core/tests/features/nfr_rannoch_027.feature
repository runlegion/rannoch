# Generated from NFR-RANNOCH-027 (revision 4). Do not hand-edit.
Feature: Flat out, rannoch beats the peak load, with history kept

  @module-rannoch-core @criterion-NFR-RANNOCH-027-c1
  Scenario: A flat-out producer beats the peak load
    Given rannoch is open over a store at rest already holding 50,000 events
    When one producer sends 100,000 events as fast as it can
    Then all 100,000 are in _events at more than 200 a second
