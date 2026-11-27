# From issue #152, which traces to FR-RANNOCH-130.
Feature: rannoch-queue reads its settings from the [queue] section

  @module-rannoch-queue @issue-152-c1
  Scenario: The [queue] section's attempts replaces the attempts passed to Queue::start
    Given a settings file whose [queue] section sets attempts = 2, and Settings with 10 attempts passed to Queue::start
    When a run that always fails is taken
    Then it is binned after its second attempt, and no third attempt is sent

  @module-rannoch-queue @issue-152-c2
  Scenario: With no [queue] section the queue behaves as today
    Given a settings file with no [queue] section, and Settings with 3 attempts passed to Queue::start
    When a run that always fails is taken
    Then it is binned after its third attempt, as the Settings passed in say
