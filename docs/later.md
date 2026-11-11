# Later

Questions raised while speccing core that are not core's. Not decided; for the crates they belong to.

- `run_at`: is it a field on core's event, or only the queue's? What it means, and what happens to an event whose `run_at` is in the future.
- Status -1, 1 and 2: which crate sets each, and when. A status update is a new row ("status updates means a new row"), with a new id, and its `source` is the root's id ("that means a NEW id and the source gets the ID of the root").
- A status function that returns a root and its tree: "we need to create a status function that can return a root and its tree." Which crate it lives in.
- Event or job: where that distinction lives, and how an event reaches the right queue.
- `source`: how an event produced by a processor gets the id of the event that caused it.
- When a flow that fans out is done.
- A step that runs long where only quick work is expected.
- Where the 0 returns: to the sender or to the queue ("we don't know yet").
