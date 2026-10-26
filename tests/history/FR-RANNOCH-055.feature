# Generated from legion requirement FR-RANNOCH-055, revision 2026-10-25T23:16:17.976731+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A class declares each property's value type and the key its history is ordered by, and every write carries its point

  @criterion-01a1752f-2118-77ee-ab1c-11cc26e3d657 @module-history
  Scenario Outline: a hearing's point survives a rebuild from events
    Given a graph with hearings of a <kind> class
    When the graph is rebuilt from its events
    Then every hearing reads back the point it had before the rebuild
  
    Examples:
      | kind          |
      | time-keyed    |
      | integer-keyed |
      | keyless       |

  @criterion-01a1752f-2119-7f1c-a77a-78c323dc584f @module-history
  Scenario Outline: a hearing's point survives a rebuild from a snapshot
    Given a graph with hearings of a <kind> class and a snapshot after them
    When the graph is rebuilt from the snapshot
    Then every hearing reads back the point it had before the rebuild
  
    Examples:
      | kind          |
      | time-keyed    |
      | integer-keyed |
      | keyless       |

  @criterion-01a1752f-211a-7ea4-9491-888006eda313 @module-history
  Scenario: a keyless class's points do not depend on arrival order
    Given the events of a graph holding hearings of a class with no key, recorded at different times
    When a new graph is given the same events in reverse order
    Then every hearing reads back the same point in both graphs
