# Generated from legion requirement FR-RANNOCH-062, revision 2026-10-26T08:42:09.319737+00:00.
# Do not edit: regenerate with scripts/features.mjs.

Feature: The store keeps every event; a snapshot keeps every version row and every event id, and deletes nothing

  @criterion-01a17759-c26f-7d47-a940-a9e6856ee7f0 @module-purge
  Scenario: a purge reaches every snapshot the store made
    Given a graph holding a fact whose property carries a unique token
    And a snapshot taken at one cursor and, after later writes, another snapshot at a later cursor
    When the app purges the fact and the queue is closed
    Then a byte search of each of the two snapshots finds no copy of the token
