# Generated from FR-RANNOCH-124 (revision 2). Do not hand-edit.
Feature: rannoch-processor has two built-in steps, emit and store, that never reach outside rannoch

  @module-rannoch-processor @criterion-FR-RANNOCH-124-c1 @criterion-FR-RANNOCH-124-c2
  Scenario: A flow of built-in steps runs with no action of the app's
    Given an app whose flow has an emit step that branches on a field and an emit step that waits for another event, and no action of its own
    When the app's build checks the flow and an event of its ty is handed to it
    Then the build passes, the branch's next is sent for the case the field matches, and the wait sends its next when the other event arrives and its timeout when none does

  @module-rannoch-processor @criterion-FR-RANNOCH-124-c3 @criterion-FR-RANNOCH-124-c4 @criterion-FR-RANNOCH-124-c5
  Scenario: store keeps one row with the event's 2
    Given a table with a key, and a flow whose step stores a row shaped from the event's fields into it
    When events are handed to the flow, and one of them is run again
    Then every event that has its 2 has exactly its one row in the table, with the values its fields gave, and the event run again added none

  @module-rannoch-processor @criterion-FR-RANNOCH-124-c6
  Scenario: store refuses a table it does not have
    Given a flow whose step stores a row into a table the store does not have
    When an event is handed to the flow
    Then the event gets a -1 naming the table, and later events of the same inbox are written
