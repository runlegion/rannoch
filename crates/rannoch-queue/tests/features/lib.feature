# Generated from FR-RANNOCH-019 (document 01a1d941-e1c3-7f61-a267-cf7aeac8a904, revision 15). Do not hand-edit.
Feature: The queue runs the events that are due, in two buckets, and retries what fails

  @module-rannoch-queue @criterion-FR-RANNOCH-019-c1 @criterion-FR-RANNOCH-019-c2 @criterion-FR-RANNOCH-019-c3 @criterion-FR-RANNOCH-019-c4 @criterion-FR-RANNOCH-019-c8
  Scenario: The queue runs what is due
    Given a store with events from the app's forgr factory at status 0: two whose flow has no job step, one whose flow has a job step, one whose run_at is a short wait away, and one whose ty has no flow
    When the events crate signals the queue
    Then the queue sends a status 1 for each due event, with that event as its source, and none for the one not yet due
    And the two runs run one after the other while the job runs beside them
    And each processor sends its own result, a 2 with its log line
    And for the event with no flow the queue sends a -1 saying "no processor", and no retry
    When the signal comes again
    Then no event with a status 1 is taken again
    When the event not yet due comes due with nothing else appended
    Then the queue takes it and runs it

  @module-rannoch-queue @criterion-FR-RANNOCH-019-c5 @criterion-FR-RANNOCH-019-c9
  Scenario: A failing flow is retried, then binned
    Given a store with an event at status 0 whose flow fails every time
    And the queue's settings are 3 attempts and a short first wait
    When the events crate signals the queue
    Then the processor sends a -1 for the attempt, and the queue sends a retry with status 0, the attempt as its source, the failure's result as its data and a run_at after the first wait
    When the retry comes due and fails
    Then the processor sends a -1 for it, and the queue sends a second retry whose source is the first retry, not the original event
    When the second retry comes due and fails
    Then the processor sends a -1 for it, and the queue sends nothing more, because its chain has reached 3 attempts

  @module-rannoch-queue @criterion-FR-RANNOCH-019-c7
  Scenario: The hunt reruns a dropped run
    Given a store with an event from the app's forgr factory whose status 1 is older than the retry window, with no 2 or -1 after it, as a crash leaves it
    And the queue's settings are a short retry window and a short hunt interval
    And an event whose status 1 is older than the retry window and is followed by a -1 and a retry waiting on its run_at
    When the hunt runs
    Then the hung event is sent again as a retry, with status 0 and the hung event as its source
    And the failure waiting on its retry is left alone

  @module-rannoch-queue @criterion-FR-RANNOCH-019-c6
  Scenario: The app reads the queue's report
    Given a store with rows from the app's forgr factory at each of the four statuses
    When the app asks the queue for its report
    Then it gives the number of _events rows at each status
