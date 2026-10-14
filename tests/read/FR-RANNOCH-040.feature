# Generated from legion requirement FR-RANNOCH-040, revision 2026-10-14T19:39:20.618754+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A capped walk from a root, over relations in either direction

  @criterion-01a13963-1032-7462-9c0b-a4cded45ad4e @module-read
    # fixture: T8's largest finding, supported by statements from 13,198 voices
  Scenario: a walk never returns more than its cap
    Given a finding whose 2-hop neighbourhood holds more than 200 nodes
    When the app walks 2 hops from that finding capped at 200
    Then the walk returns 200 nodes

  @criterion-01a13963-1032-7462-9c0b-a4ddfaecc56b @module-read
  Scenario: a relation walks both ways
    Given statement s2 answers statement s1
    When the app walks answers<- from s1 for 1 hop
    Then the walk returns s2
