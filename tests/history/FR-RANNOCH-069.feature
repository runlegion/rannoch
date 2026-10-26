# Generated from legion requirement FR-RANNOCH-069, revision 2026-10-25T22:30:26.320556+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: History walks back to first heard, across renames and snapshots, and never disagrees with what was recorded

  @criterion-01a14736-4f8d-7962-878e-23a3c4cca134 @module-history
  Scenario: the walk returns every hearing in axis order
    Given a node heard more than once, not in axis order
    When the app walks its history
    Then the walk returns every hearing in order on the class's declared axis

  @criterion-01a1472c-f3c0-7282-b845-d88d0aafb658 @module-history
  Scenario: the walk crosses a rename
    Given a node heard under one key and then replaced by a new key
    When the app walks the new key's history
    Then the walk reaches first heard under the old key

  @criterion-01a1472c-f3c0-7282-b845-d89af5e4f22f @module-history
  Scenario: the walk crosses a snapshot
    Given a node first heard before a snapshot and heard again from another source after it
    When the app walks its history
    Then the walk reaches the first hearing recorded before the snapshot

  @criterion-01a1472c-f3c0-7282-b845-d8a5b2e77dba @module-history
  Scenario: history agrees with the record
    Given a node that has been heard from more than one source, with no snapshot taken
    When the app walks its history
    Then each hearing returned matches the event that recorded it
