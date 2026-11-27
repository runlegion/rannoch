# #153's scenarios, from FR-RANNOCH-130 (settings come from the app's settings file through
# rannoch-settings, each crate taking its own section by name as its own type). Each scenario
# names the test in tests/scenarios.rs that drives it.
Feature: rannoch-core reads default_interval_ms from the [core] section

  @module-rannoch-core @criterion-FR-RANNOCH-130
  Scenario: The [core] section sets the default inbox's interval (core_settings_c1)
    Given a settings file whose [core] section sets default_interval_ms, and a Config whose default_interval is an hour
    When Rannoch::open is called with that file's path and an event is sent
    Then the default inbox hands it over at the interval the [core] section gives

  @module-rannoch-core @criterion-FR-RANNOCH-130
  Scenario: With no settings file, or no [core] section, Config's interval stands (core_settings_c2)
    Given a settings file with no [core] section, and a Config whose default_interval is an hour
    When Rannoch::open is called with that file's path and an event is sent
    Then the default inbox does not hand it over before shutdown, and every other rannoch-core test passes unchanged with no settings file
