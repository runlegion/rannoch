# Generated from NFR-RANNOCH-026 (revision 5). Do not hand-edit.
Feature: Rannoch keeps up with the peak load, with history kept

  @module-rannoch-core @criterion-NFR-RANNOCH-026-c1
  Scenario: Rannoch keeps up with the peak load
    Given rannoch is open over a store at rest already holding 50,000 events
    When the app sends 2,000 events at 200 a second
    Then all 2,000 are in _events
