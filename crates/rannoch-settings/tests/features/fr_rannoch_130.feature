# Generated from FR-RANNOCH-130 (revision 8). Do not hand-edit.
Feature: rannoch-settings reads the app's settings file once at open, and each crate takes its own section

  @module-rannoch-settings @criterion-01a21e58-4ded-7181-9db7-0933cd9045c4
  Scenario: Opened with a settings path, each crate takes its own section
    Given an app whose settings file has a [queue] section and an [app] section, and Rannoch::open with settings set to that file's path
    When rannoch opens and each section is taken by name as its own type
    Then each type holds the values that section of the file gives

  @module-rannoch-settings @criterion-01a21e58-4ded-7181-9db7-09447f0820cf
  Scenario: Opened with no path, rannoch reads rannoch.config.toml beside the binary
    Given a rannoch.config.toml with an [app] section in the directory of the running binary, and Rannoch::open with settings None
    When rannoch opens and the [app] section is taken
    Then it holds the values rannoch.config.toml gives

  @module-rannoch-settings @criterion-01a21e58-4ded-7181-9db7-0951a3d9e4ef
  Scenario: No file or no section gives the defaults
    Given no settings file at all, and then a settings file with no [app] section
    When rannoch opens and the [app] section is taken
    Then it holds that type's defaults both times

  @module-rannoch-settings @criterion-01a21e6f-29db-7660-b7e7-ff16e959f613
  Scenario: A file that does not parse stops the app
    Given a settings file that is not valid TOML
    When Rannoch::open is called with its path
    Then open fails with Error::Config

  @module-rannoch-settings @criterion-01a21e6f-29db-7660-b7e7-ff2597d2588c
  Scenario: Settings loaded before open are the ones used
    Given an app that calls rannoch_settings::load with one file, then Rannoch::open with settings set to a different file
    When a crate takes its section
    Then it holds the values of the file given to load, and open did not load the other file

  @module-rannoch-settings @criterion-01a21e7c-0751-7f70-8575-7faf20c3b6bf
  Scenario: A section that does not read as its type gives the defaults
    Given a settings file whose [app] section holds text where the type has a number
    When rannoch opens and the [app] section is taken
    Then it holds that type's defaults

  @module-rannoch-settings @criterion-01a21e7c-0751-7f70-8575-7fb9f563dcd6
  Scenario: A second load is refused
    Given an app that has loaded its settings, by load or by Rannoch::open
    When it calls rannoch_settings::load again with another file
    Then the call fails with Error::Config, and every section still holds the first file's values
