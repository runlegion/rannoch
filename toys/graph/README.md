# Toy: does the graph return a graph?

Writes known graphs, reads them back only through the public reads (`nodes`, `links`, `counts`,
`describe`), and compares them edge by edge with truth from outside the store. No store code changes.
Predictions were committed before any run (`PREDICTIONS.md`); findings are in `RESULTS.md`, raw output
in `results/`.

## Rerun

```
cargo build --release
D=/path/on/the/test/disk
./target/release/rannoch-toy-graph small  "$D"             # hand-written expected edges
./target/release/rannoch-toy-graph git    "$D" /path/repo  # truth from git rev-list and git log
./target/release/rannoch-toy-graph random "$D" 40          # seeded histories against a plain model
```
