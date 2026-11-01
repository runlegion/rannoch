# Writing flows

> **This is a design. None of it is built.** `rannoch-worker`, `rannoch-harness`, `rannoch-processor` and every `rannoch-processor-*` crate are empty scaffolds. This page is for the operator and the residents to mark up; once agreed, it becomes the spec they are built from. The syntax below is a sketch of the shape, not a format to write against. To process events today, write a worker in your own code ([inbox.md](inbox.md)).

## The idea

A flow is a set of workers, each a file of JSON lines in a folder. The harness loads the folder, runs each worker against the inbox, and commits what each step did together with the worker's place, so a crash never runs a step's effect twice or skips one.

The parts, from the scaffolds' own descriptions:

- **A worker file** (`rannoch-worker`) is read and checked against the ontology before anything runs.
- **The harness** (`rannoch-harness`) holds the flow: it loads the workers in the folder, reads guards and `else`, follows `next` to a row or to another worker, renders templates, hands each row to its processor, and commits what the processor declared together with the worker's place.
- **A processor** (`rannoch-processor`) is one kind of step. rannoch ships the generic ones; an app can write its own against the same contract.

## How a worker works

Take the operator's example: a line arrives; one worker parses it and writes a report, then passes it on to worker x, which writes its own report; meanwhile workers x, y and z all run.

### It sees every event

Every worker has its own place in the inbox and is handed every committed event, in log order. Workers x, y and z read the same stream at their own pace, each with its own retries and its own state. One falling behind holds no other back. Within one worker, events go one at a time, in order. This part is built: it is the inbox.

### It recognises a kind with a guard

A row may carry a guard: a JSON Schema the event must match. When an event does not match the first row's guard, the worker closes it as done and moves on. Guards look at the envelope (`type`, `source`, `subject`) and, when needed, at `data`.

### It does work with steps

Each row names a step: the processor that runs it.

| Processor | What it does |
|---|---|
| `host` | Calls an action the app registered, under a declared key, checking its input and its answer, and remembering the answer |
| `store` | Writes a node with its properties and links in one unit; writing it again unchanged writes nothing |
| `lookup` | Reads a node by class and key, or nothing |
| `emit` | Sends a CloudEvent to the inbox or to a named sink, exactly once; its id is a hash of its content, its time the event's time |
| `branch` | Continues at the first case whose guard holds, else at the default, else ends |
| `bin` | Puts the input in the worker's bin, with a reason |
| `wait` | Parks a keyed instance until a matching event arrives or its time passes, on event time |
| `after` | Sends a CloudEvent to the inbox at the event's time plus a delay |
| `count` | Counts by a key in an hour, day or total bucket of event time, and runs a close row once when the bucket ends |
| `approve` | Waits for an approval signed with ed25519 by a named approver |
| `onnx` | Runs an ONNX model locally as a step |

**An app's own logic is a host action, not a crate.** `parse_line` would be a function in your app, registered with the harness under that name, with a declared input shape and output shape. The `host` processor checks the input, calls the function, checks the answer, and remembers it, so a retry after a crash gets the remembered answer instead of calling again. An app writes its own processor only for a new kind of step, not for new logic.

### It hands off with an emit

A worker passes work on by emitting a CloudEvent. The next worker's guard matches that event's type. Because an emitted event's id is a hash of its content, a step retried after a crash emits the same id, and the store holds it once.

`next` can also name another worker directly; whether that is needed beside emit is an open question below.

### The example, sketched

```
// workers/parse.jsonl
{"guard": {"properties": {"type": {"const": "log.line"}}}, "step": "host", "action": "parse_line", "next": "report"}
{"id": "report", "step": "host", "action": "write_report", "next": "pass"}
{"id": "pass", "step": "emit", "type": "line.reported", "else": "bin"}

// workers/x.jsonl
{"guard": {"properties": {"type": {"const": "line.reported"}}}, "step": "host", "action": "x_report"}

// workers/y.jsonl and workers/z.jsonl: other guards, other actions, running beside x
```

`parse_line`, `write_report` and `x_report` are the app's host actions. A line that fails to parse takes `else` to the worker's bin with the reason.

## Kessel's first flow

From kessel (bullpen, 2026-11-01), agreed in shape by the operator on 2026-10-27. One game build, from patch to sealed.

1. **Arrives.** `kessel-import`, a host action, runs once a Steam patch is installed. It walks the `.tor` archives and emits one CloudEvent per piece: a GOM object, a string table, an area file, a room file, or a media file. Type `dev.huttspawn.kessel.raw.<kind>`; source `kessel/<branch>/<steam build id>`; subject the FQN or derived path; data the raw bytes and a content hash. A piece the walker cannot split arrives whole as `raw.unsplit`, with a report event. One build is about 1-2 million events.
2. **Recognise.** Guards match by type alone (`raw.gom`, `raw.stringtable`, `raw.area`, `raw.room`, `raw.media`, `raw.unsplit`), with `source` scoping a guard to one build.
3. **Decode, in parallel.** Each raw kind goes to its reader, a host action: `raw.gom` to the field reader (typed in: the raw bytes and a reference to the build's prototype registry; typed out: decoded fields, or a decode failure with byte offsets). Each output is keyed to the raw event and the parser version, and emits `object.added`, `object.changed` or `object.removed` against the same FQN in the previous build. A failure emits `decode.failed` and goes to recovery; only if recovery fails does it go to a judge (Haiku, then Sonnet when Haiku is unsure).
4. **Hand off.** `object.*` fans out in parallel to domain workers (items, abilities, quests, conversations, maps), each emitting domain facts. A link (quest to reward item, NPC to conversation) is made once both ends exist and emits `link.found` with the source event ids. Beside them, an `onnx` step judges test-or-real on `object.added`.
5. **Join, then seal.** Once every raw event of the build has a decode or a judged failure, three checks run: 100% decode, nothing lost between archive and store, and coverage against the oracle. If all pass, the flow emits `build.sealed` and drops the build's `raw.*` events. A failed check holds the seal and emits the gap.

Open, the operator's call on cost: whether `raw.media` is kept after seal. Media is most of each build's 30 GB and more.

## Warden's first flow

**Pending.** To be filled from warden's answer.

## Open questions

1. **Worker file format.** Row fields, how a row names its next row, and how templates are written and rendered.
2. **Guards.** JSON Schema on the whole event, or on named parts? How a guard reads `data` that is bytes.
3. **`next` to another worker beside `emit`.** Is a direct hand-off needed, or is emit enough?
4. **Host actions.** How an app registers one, declares its input and output shapes, and how the harness remembers an answer.
5. **Bins.** Where a worker's bin lives, how a binned event is read and fixed, and how it is sent back through.
6. **Going live.** The earlier design had a program saved into the folder only staged, then made live by a signed request after a trial against history. Is that kept?
7. **Where flows live.** The workflow crates sit in rannoch's workspace today. The 0.3 scope RFC said workflows would be their own crate, with rannoch one backend. Do they stay?
8. **Kessel's asks:**
   - **A join:** wait until every event from source X has reached outcome Y.
   - **Replay:** rerun a step with a new parser version over the stored raw events, before seal.
   - **Purge scoped by source,** allowed only after seal. (Purge is #148; a retention rule per class, trim, has no issue yet.)
   - **Large inputs by reference:** host actions that take the prototype registry, hundreds of MB, by reference rather than inline.
9. **Scale.** Kessel's builds are millions of events and terabytes of import log across builds. What the store keeps after a build is sealed decides its size.
