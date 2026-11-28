# #153's scenarios, from FR-RANNOCH-130 (settings come from the app's settings file through
# rannoch-settings, each crate taking its own section by name as its own type). Each scenario
# names the test in tests/scenarios.rs that drives it.
Feature: rannoch-core reads the default lane's heartbeat from the [lane.default] section

  @module-rannoch-core @criterion-FR-RANNOCH-130
  Scenario: The [lane.default] section sets the default lane's heartbeat (core_settings_c1)
    Given a settings file whose [lane.default] section sets a heartbeat of an hour
    When Rannoch::open is called with that file's path and an event is sent
    Then the default lane does not hand it over before shutdown

  @module-rannoch-core @criterion-FR-RANNOCH-130
  Scenario: With no lane section, the default heartbeat stands (core_settings_c2)
    Given a settings file with no lane section
    When Rannoch::open is called with that file's path and an event is sent
    Then the default lane hands it over at the default heartbeat of 20 ms, and every other rannoch-core test passes with no settings file
