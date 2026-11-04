# The relay toy

The importer's and the recorder's predictions for relays (issue #151), on real game data. PREDICTIONS.md was committed before any run; RESULTS.md says what happened.

```sh
cd toys/relay
cargo build --release
./target/release/relay-toy <spice-7.9.a-v7.sqlite> <spice-7.8.1.c-v26.sqlite> [t1|t2|t3|recorder]
```

The two paths are the spice builds, opened read only; no data is committed. Set `TMPDIR` to the disk you want the graphs on: commits go through `fullfsync`, and a USB disk is slower than an internal one. Each line prints PASS or FAIL, the prediction and what was measured; the exit code is 1 when anything failed. A REPORT line is a measure #151 reports and does not judge.
