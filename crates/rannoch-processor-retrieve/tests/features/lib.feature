# Generated from FR-RANNOCH-122 (revision 6). Do not hand-edit.
Feature: rannoch-processor-retrieve queries the app's outside source from a flow step

  @module-rannoch-processor-retrieve @criterion-FR-RANNOCH-122-c1
  Scenario: A flow step queries an outside source the app reaches through the retrieve trait
    Given a test app that implements the retrieve trait with a stub source, declaring that it gives email as a string
    And an app built with a flow whose step queries that source
    Then the flow reports a job step
    When the queue hands rannoch-processor an event the stub answers with an email string
    Then the step's next carries the email on
    When the stub answers an event with email 7
    Then the step sends a -1 whose data is 'email as number', and no next
