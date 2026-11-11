# rannoch queue

Notes, not yet reviewed: the operator's words on what comes after the inbox, moved here from core's spec (2026-11-10). Quoted lines are the operator's. Lines marked **[legion]** are mine and unconfirmed.

## Timed work

- The log trim: "rannoch event log has a trim, it's configured by the settings file, default 22.22 minutes, just because. It dumps everything with a 2 to flatfile as cloud event json and it's encrypted and compresed. Both of these are also setitngs."
- The default is 22 because "it's 2 over the retry limit."
- The trim removes the rows it dumps, in the same step that writes the file ("exactlly").
- "this is a function of the the queue": `schedule_event(event, delay)` and `process_due_events(processor)`, "and one of those is freeze_events()".
- Scheduling by time: "retries can can be sent with the offset, we can schedule the freeze based off the last freeze run. We can allow agents to schedule events and we build zero machinery becasue we add everything to the queue based on time."
- **[legion]** An event's run time is its own field on rannoch's struct, apart from `time`, which says when the occurrence happened and is never set to the future.
- The freeze dumps "as cloud event json"; since then rannoch dropped CloudEvents for its own struct ("ya we don't need it"). **[legion]** The archive is rannoch's own struct, serialized.
- "we just build a cron into the core of rannoch." It uses no cron notation: "The freeze flow has the rescehddule step."
- Retries: "retry x times per x time". Defaults: 10 attempts inside a 20-minute window (operator, 2026-11-09).
- "the retry queue will be memory only until we find a need for it to be durable."

## Open

1. **Where the time-ordered queue lives.** The inbox's heap is only the batch buffer, and the trim is "a function of the queue".
2. **Retries and durability.** A retry "sent with the offset" is an event in the log, so it is durable. That contradicts "the retry queue will be memory only".
3. **Binned events at the trim.** The trim dumps status 2. **[legion]** I read it as leaving -1 events live, because the bin is part of the queue.
4. **The encryption key.** Where it comes from: "We will figure this detail out as we spec an write the issue."
5. **The run-time extension's name.**
