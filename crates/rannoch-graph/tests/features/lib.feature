# #134's scenarios, from FR-RANNOCH-125 (revision 11) and FR-RANNOCH-127 (revision 2), and
# #135's, from FR-RANNOCH-126 (revision 6), and #136's, from FR-RANNOCH-128 (revision 7). Each
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

  @module-rannoch-graph @criterion-FR-RANNOCH-126-c1
  Scenario: rannoch-graph is bitemporal (fr_rannoch_126_c1)
    Given d1 titled by an event at 1,000, valid from 1,000, and retitled by an event at 2,000, valid from 1,500
    When the app reads d1 as of 1,700 by valid time and by recorded time, each as an event
    Then valid time gives the retitle, and recorded time the title the store held by then

  @module-rannoch-graph @criterion-01a219a7-4cc4-7d50-b375-8fdf0d724ded
  Scenario: An as-of read by recorded time (criterion_01a219a7_as_of_by_recorded_time)
    Given d1 titled by an event at 1,000 and retitled by an event at 2,000
    Then as of its event column, before 1,000 none, from 1,000 to 1,999 the title, and from 2,000 the retitle

  @module-rannoch-graph @criterion-01a219a7-4cc4-7d50-b375-8feda7bb8f31
  Scenario: An as-of read by valid time (criterion_01a219a7_as_of_by_valid_time)
    Given d1 titled valid from 1,000, and retitled at 2,000 valid from 1,500
    Then as of its valid-time column, before 1,000 none, from 1,000 to 1,499 the title, and from 1,500 the retitle

  @module-rannoch-graph @criterion-FR-RANNOCH-128-c1
  Scenario: The scoring tools are part of rannoch-graph by default (fr_rannoch_128_c1)
    Given an app whose d1 cites d2 and d3, and d2 cites d4, each weighing its time
    When the app sends a read by each scorer, each as an event
    Then each read returns the scores of the nodes near d1, highest first

  @module-rannoch-graph @criterion-FR-RANNOCH-128-c2
  Scenario: No part of rannoch-graph stores or uses vectors (fr_rannoch_128_c2)
    Given rannoch-graph's signatures and manifest
    Then it has no tables, no step takes or gives a list of numbers or takes a score from outside the graph, and it does not depend on rannoch-processor-onnx

  @module-rannoch-graph @criterion-01a216c8-b138-7ce1-8599-45d05b451b2c
  Scenario: The scoring tools work on variable tables and columns (criterion_01a216c8_scoring_tools_work_on_variable_tables_and_columns)
    Given two apps whose ontologies hold the same docs under different table and column names
    Then katz, pagerank, shared and learned give each app the same scores
