# Generated from FR-RANNOCH-119 (revision 8). Do not hand-edit.
Feature: rannoch-processor runs a flow's steps and sends its nexts on success

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

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c7
  Scenario: branch and each send their nexts
    Given an app built with a flow whose branch has a case when crash naming ty 31, a case when question naming ty 32, and an else
    And a flow whose branch has a case when level is the number 1, and an else
    And a flow whose each sends one next per item of a list
    When the queue hands rannoch-processor an event whose value is crash, one whose value is question, and one whose value has no case
    Then one next of ty 31, one of ty 32 and one else next are sent, each with its event as source
    When the queue hands rannoch-processor an event whose level is the number 1, and one whose level is the string "1"
    Then the first sends the case's next and the second sends the else next
    When the queue hands rannoch-processor an event whose list is 'a b c'
    Then three nexts are sent, one per item, each with that event as source

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c11 @criterion-FR-RANNOCH-119-c13
  Scenario: A two-step flow sends its next only when every step succeeds
    Given an app built with a two-step flow whose step 0 sends a next and whose step 1 runs a test action that fails on the first attempt and succeeds on the next
    When the queue hands rannoch-processor an event of that flow
    Then step 1 is passed the options step 0 gave
    And the processor sends one -1 for the event, and no next
    When the queue's retry of the event comes due
    Then step 0's next is sent, and the processor sends its 2
    And across both attempts exactly one next was sent

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c14
  Scenario: An action's real output is checked at run time
    Given an app built with a flow whose step runs a test action that declares it gives severity
    When the queue hands rannoch-processor an event and the action's output has a severity
    Then the step's next carries it on
    When the queue hands rannoch-processor an event and the action's output has no severity
    Then the step sends a -1 naming severity, and no next and no 2

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c1 @criterion-FR-RANNOCH-119-c2 @criterion-FR-RANNOCH-119-c12 @criterion-FR-RANNOCH-119-c16
  Scenario: The app's build checks its flow docs
    Given an app whose flow docs are a valid set over the actions of a test processor crate, one of whose actions is a job
    When the app is built
    Then the build succeeds
    And each flow that uses the job action reports a job step, and each flow that uses none does not
    And the built app runs its flows with the flow doc files removed from disk
    When the app is built with a flow doc whose step names an action nope that does not exist
    Then the build fails listing one error: no action named nope
    When the app is built with a flow doc whose options for greet have the unknown field salut
    Then the build fails listing one error: the options for greet do not parse
    When the app is built with a flow doc whose link upper takes text, which nothing before it gives
    Then the build fails listing one error saying so
    When the app is built with a flow doc whose link upper takes text as a string and is given a number
    Then the build fails listing one error saying so
    When the app is built with a flow doc whose next to notice gives no who, which notice takes
    Then the build fails listing one error saying so
    When the app is built with a flow doc whose next to notice gives who as a number, where notice takes a string
    Then the build fails listing one error saying so
    When the app is built with a flow doc whose next's $concat names a field greeting that nothing in scope gives
    Then the build fails listing one error: greeting names nothing in scope
    When the app is built with a flow doc whose next goes to ty 99, which no flow takes
    Then the build fails listing one error saying so
    When the app is built with a flow doc whose step names emit
    Then the build fails listing one error: no action named emit
    When the app is built with two flow docs holding three errors between them
    Then the build fails listing all three errors

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c10
  Scenario: The app's build gathers the signatures a separate processor crate exports
    Given a workspace with a processor crate, separate from the app, that exports its actions' signatures as data, among them greet
    And an app with its own processor crate, whose flow docs use actions of both, including a next whose template names the field greet gives
    When the app is built
    Then its build.rs gathers the signatures of both crates and the build succeeds
    When the app is built with each bad set of flow docs
    Then each build fails listing every error in it
    When the separate crate renames the field greet gives, and the app is built
    Then the build fails listing one error: the next's template names a field nothing gives

  @module-rannoch-processor @criterion-FR-RANNOCH-119-c8 @criterion-FR-RANNOCH-119-c9
  Scenario: A wait kept in _events matches, times out, and sends a late match to late, across a restart
    Given an app built with a flow whose step waits for a payment keyed by its order, with a timeout, a match next, a timeout next and a late next
    When orders o1 and o2 start waiting
    Then each wait is kept in _events as an event whose source is the waiting event and whose run_at is its timeout
    When the app's process is dropped and its _events file reopened
    And two payments for o1 arrive before o1's timeout
    Then o1's match next is sent once, with the wait as its source
    When the waits come due, and the due events are run again
    Then o1's wait sends no timeout, and o2's wait sends its timeout next once
    When a payment for o2 arrives after its timeout, and the due events are run again
    Then o2's late next is sent once, and no match next
