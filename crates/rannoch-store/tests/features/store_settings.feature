# #154's scenarios, from its acceptance criteria (FR-RANNOCH-130 rev 8: crates built before open
# read their sections when they are built). Each scenario names the test that drives it.
Feature: rannoch-store reads cache_size_kib and checkpoint_every_ms from the [store] section

  @module-rannoch-store @criterion-154-c1
  Scenario: A [store] section sets the page cache and the checkpoint interval (store_settings_c1)
    Given settings loaded before Store::new whose [store] section sets cache_size_kib and checkpoint_every_ms
    When the store is made and opens its connections
    Then every connection has that cache size and the checkpointer runs at that interval

  @module-rannoch-store @criterion-154-c2
  Scenario: No settings, or no [store] section, leaves the store as it was (with_no_settings_loaded_the_store_keeps_todays_values)
    Given no settings loaded, or no [store] section
    When the store is made
    Then it keeps a 64 MiB page cache and checkpoints once a second, and every existing rannoch-store test passes unchanged
