# Generated from legion requirement FR-RANNOCH-083, revision 2026-10-25T22:48:46.389362+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A read returns a node's or link's state at a point on its class's axis

  @criterion-01a1752f-2121-7c77-a789-1b6bca039656 @module-history
  Scenario: state at a point is the version that held then
    Given a class whose history is ordered by a time the writer supplies
    And a node written at points 10, 20 and 30
    When the app reads the node's state at point 25
    Then the read returns the version written at point 20

  @criterion-01a1752f-2122-73c8-b8c1-b2e48340ac5f @module-history
  Scenario: there is no state before the first version
    Given a class whose history is ordered by a time the writer supplies
    And a node written at points 10, 20 and 30
    When the app reads the node's state at point 5
    Then the read returns no version

  @criterion-01a1752f-2123-7854-886f-4d63d4e1b8f8 @module-history
  Scenario: a late version answers at its own point
    Given a class whose history is ordered by a time the writer supplies
    When the app writes a node at points 10 and 30, and then at point 20
    Then a read of the node's state at point 25 returns the version written at point 20

  @criterion-01a1752f-2124-749d-b60d-ba18c5888f97 @module-history
  Scenario: a link's state at a point shows whether it had ended
    Given a class whose history is ordered by a time the writer supplies
    And a link written at point 10 that ended at point 30
    When the app reads the link's state at points 25 and 35
    Then the link reads as live at point 25 and as ended at point 35

  @criterion-01a1752f-2125-7358-bcbb-4bc34653abb4 @module-history
  Scenario: the answer describes the version, not the storage
    Given a class whose history is ordered by a time the writer supplies
    And a node written at point 20 from a source with a valid time
    When the app reads the node's state at point 25
    Then the answer names point 20, that source, that valid time and the version's event id
    And the answer names no table or column of the store

  @criterion-01a17540-2150-7cd0-933a-7ef6367d9e74 @module-history
  Scenario: an integer-keyed class is read at a key
    Given a class whose history is ordered by an integer the writer supplies
    And a node written with keys 1, 2 and 3
    When the app reads the node's state at key 2
    Then the read returns the version written with key 2
