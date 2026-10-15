# Generated from legion requirement FR-RANNOCH-046, revision 2026-10-14T19:39:22.957969+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: A reference into another graph file learns its target's fate from that file

  @criterion-01a13963-1897-7b03-8b82-3d80b3cd6de0 @module-refs
    # fixture: T6 files (t6-t2-refs-replay, 2026-10-13): local file with references l7->s7, l8->s8, l10->s10 into a shared file
  Scenario: a forgotten target resolves as forgotten by its rule
    Given the local file mounts the shared file
    And shared node s7 was forgotten by rule superseded
    When the app resolves reference l7 to s7
    Then it answers forgotten by superseded

  @criterion-01a13963-1897-7b03-8b82-3d991cf1b88f @module-refs
    # fixture: T6 files (t6-t2-refs-replay, 2026-10-13): local file with references l7->s7, l8->s8, l10->s10 into a shared file
  Scenario: a replaced target resolves to its replacement
    Given the local file mounts the shared file
    And shared node s8 was replaced by s9 with a replace event
    When the app resolves reference l8 to s8
    Then it answers replaced by s9

  @criterion-01a13963-1897-7b03-8b82-3dae3494fa3e @module-refs
    # fixture: T6 files (t6-t2-refs-replay, 2026-10-13): local file with references l7->s7, l8->s8, l10->s10 into a shared file
  Scenario: a live target resolves as live
    Given the local file mounts the shared file
    And shared node s10 is live
    When the app resolves reference l10 to s10
    Then it answers live

  @criterion-01a13963-1897-7b03-8b82-3dbe32a4b868 @module-refs
    # fixture: T6 files (t6-t2-refs-replay, 2026-10-13): local file with references l7->s7, l8->s8, l10->s10 into a shared file
  Scenario: resolving does not change the referring file
    Given the local file mounts the shared file
    And the local file holds 300 references into the shared file
    When the app resolves them
    Then the local file's 300 references are unchanged

  @criterion-01a13bec-ffe3-7698-b595-abdde4035f23 @module-refs
    # fixture: T6 files (t6-t2-refs-replay, 2026-10-13): local file with references l7->s7, l8->s8, l10->s10 into a shared file
  Scenario: a new version is not a replacement
    Given the local file mounts the shared file
    And shared node s10 was written again with a changed property
    When the app resolves reference l10 to s10
    Then it answers live
