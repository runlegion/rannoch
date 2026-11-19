# Generated from FR-RANNOCH-119 (revision 8). Do not hand-edit.
Feature: rannoch-processor runs the app's compiled flows, and the app's build checks them

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c3 @criterion-FR-RANNOCH-119-c4 @criterion-FR-RANNOCH-119-c5
  Scenario: rannoch-processor runs the flow for an event the queue hands it
    Given an app built with a flow whose step is the chain trim.upper.greet, from a test processor crate, ending in a next with a delay of 500 whose shape fills who with or and builds msg with $concat
    And a flow for the receiving ty whose step has no next
    And a flow whose step puts its result under a field with as
    When the queue hands rannoch-processor an event of the chain's ty
    Then trim, upper and greet run on the one event, with no event written between them
    And each link reads the root's data, and is passed the options the links before it gave, the later winning
    And exactly one next is written, with the processor's 2: data msg 'hi SAM!' and who 'friend', source the event, status 0, run_at the time of the run plus 500
    When the queue hands rannoch-processor the next
    Then its flow ends with the processor's 2 and nothing more is sent
    When the queue hands rannoch-processor an event of the flow that uses as
    Then the step's result is under the field as names, and the input's fields are still there

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c6
  Scenario: A failing link fails the step, and the retry reruns the chain
    Given an app built with a flow whose step is a chain with a link limit that fails on text longer than 5
    When the queue hands rannoch-processor an event whose text is 13 long
    Then the links after limit do not run
    And the processor sends one -1 whose source is the event and whose data is 'limit: text is 13 long, max 5'
    And no next and no 2 are sent
    When the queue's retry of the event comes due
    Then the chain runs again from its first link

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c14
  Scenario: An action's real output is checked at run time
    Given an app built with a flow whose step runs a test action that declares it gives severity
    When the queue hands rannoch-processor an event and the action's output has a severity
    Then the step's next carries it on
    When the queue hands rannoch-processor an event and the action's output has no severity
    Then the step sends a -1 naming severity, and no next and no 2

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c11 @criterion-FR-RANNOCH-119-c13
  Scenario: A two-step flow sends its next only when every step succeeds
    Given an app built with a two-step flow whose step 0 sends a next and whose step 1 runs a test action that fails on the first attempt and succeeds on the next
    When the queue hands rannoch-processor an event of that flow
    Then step 1 is passed the options step 0 gave
    And the processor sends one -1 for the event, and no next
    When the queue's retry of the event comes due
    Then step 0's next is sent, and the processor sends its 2
    And across both attempts exactly one next was sent
