# #155's scenarios, from FR-RANNOCH-130 (revision 5). Each scenario names the test in
# tests/settings.rs that drives it.
Feature: rannoch-events-log reads its file path from the [events-log] section

  @module-rannoch-events-log
  Scenario: The [events-log] section's path replaces the path passed to Log::new
    # settings::criterion_1_the_section_path_replaces_the_path_passed_in
    Given settings loaded before Log::new whose [events-log] section sets path
    When the log's append commits a batch
    Then each event of the batch is written to that file, and none to the path passed to Log::new

  @module-rannoch-events-log
  Scenario: With no settings loaded, or no [events-log] section, the passed path stands
    # settings::criterion_2_no_section_the_passed_path_stands, and tests/scenarios.rs unchanged
    Given no settings loaded, and then settings loaded with no [events-log] section
    When the log's append commits a batch
    Then each event of the batch is written to the path passed to Log::new
