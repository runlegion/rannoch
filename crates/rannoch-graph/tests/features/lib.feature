# #134's scenarios, from FR-RANNOCH-125 (revision 11) and FR-RANNOCH-127 (revision 2). Each
# scenario names the test that drives it: in tests/scenarios.rs, or in src/ for the utils alone.
Feature: rannoch-graph is utils over the app's ontology tables, and every read is an event

  @module-rannoch-graph @criterion-FR-RANNOCH-125-c1
  Scenario: rannoch-graph has no _nodes or _edges tables (fr_rannoch_125_c1)
    Given a store with every one of rannoch's migrations applied
    Then it holds no _nodes or _edges, and rannoch-graph has no migrations to make them

  @module-rannoch-graph @criterion-FR-RANNOCH-125-c2 @criterion-01a216a5-c6f6-7051-82f2-2c64fc1703ed
  Scenario: The app's shape is its ontology's tables, and the utils read them (fr_rannoch_125_c2)
    Given an app whose ontology holds doc_titles and cites
    Then the store's tables beyond rannoch's _events are the ontology's
    When a flow writes cites into the app's table, and a read walks them
    Then the walk reaches what the app's table holds

  @module-rannoch-graph @criterion-FR-RANNOCH-125-c3
  Scenario: why, a capped walk and as-of reads over the app's tables (fr_rannoch_125_c3)
    Given docs whose title was set at 1,000 and corrected at 2,000 by an event whose source is the first
    And d1 cites d2 at 1,000 and d3 at 2,000
    Then as of 1,500 the title is the first, after 2,000 the correction, and before 1,000 none
    And why is the correction's event, then the first
    And a walk over cites and wrote backwards reaches each node once at its fewest hops, and a cap cuts it and says so
    And a walk as of 1,500 follows only the cites written by then
    And d1's edges as of 999 are none, as of 1,500 its cite of d2, and after 2,000 both

  @module-rannoch-graph @criterion-FR-RANNOCH-125-c4
  Scenario: Every read is an event, and that event returns its result (fr_rannoch_125_c4)
    Given an app whose doc d1 was titled by one event and retitled by the next, at 1,000 and 2,000, and which cites d2 from d1 at 1,000
    When the app sends a walk, a why, and as-of reads of a row, a walk and edges, each as an event
    Then each read event returns its result as an event whose source it is

  @module-rannoch-graph @criterion-01a2166f-db5e-76d0-9f75-25c911d0f98c
  Scenario: rannoch-graph reads nothing of its own (criterion_01a2166f_rannoch_graph_reads_nothing_of_its_own)
    Given an app with its ontology
    When a read's walk names a table the store does not have
    Then the read event gets a -1 naming it, and returns nothing

  @module-rannoch-graph @criterion-01a216c8-b0d1-76f2-b5b2-b39d928247e3
  Scenario: The utils work on variable tables and columns (criterion_01a216c8_utils_work_on_variable_tables_and_columns)
    Given two apps whose ontologies hold the same docs under different table and column names
    Then as_of, why and walk give each app the same answers

  @module-rannoch-graph @criterion-FR-RANNOCH-127-c1
  Scenario: Warm hoods are part of rannoch-graph's core (fr_rannoch_127_c1)
    Given an app whose d1 cites d2, read once, which warms its hood
    When d2 cites d3 through the app's flow
    Then the next read returns the new reach
