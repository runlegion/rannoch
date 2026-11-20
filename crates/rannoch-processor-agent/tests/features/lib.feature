# Generated from FR-RANNOCH-120 (revision 6). Do not hand-edit.
Feature: rannoch-processor-agent calls the app's agent from a flow step, input checked first

  @module-rannoch-processor-agent @criterion-FR-RANNOCH-120-c1
  Scenario: A flow step calls an agent the app reaches through the agent trait
    Given a test app that implements the agent trait with a stub agent classify, declaring that it takes text as a string and gives severity
    And an app built with a flow whose step calls classify
    Then the flow reports a job step
    When the queue hands rannoch-processor an event whose text the stub answers with a severity
    Then the step's next carries the severity on
    When the queue hands rannoch-processor an event whose text is the number 5
    Then the step sends a -1 whose data is 'agent classify input gave text as number, it declares string', and the stub is not called
    When the stub answers an event without a severity
    Then the step sends a -1 whose data is 'agent classify gave no severity', and no next
