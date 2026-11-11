# rannoch

Rannoch is a Rust library that keeps an app's specialized knowledge current, traceable to where it came from, correctable, and readable as it stood at any time. It is for software that takes in events and the agents that act on what those events say.

The name means walled garden: rannoch is the app's safe space. Each app has its own.

## Status

Version 0.1 is being designed. Nothing is built yet.

- A library only: no CLI, no UI, no network service.
- Not for use in wasm in 0.1.

## Docs

The files in `docs/` are working notes kept until the real documentation is written:

- `core.md`: the core crate (broker and inbox), as specified.
- `queue.md`: notes on the queue, not yet reviewed.
- `later.md`: questions for the crates after core.
