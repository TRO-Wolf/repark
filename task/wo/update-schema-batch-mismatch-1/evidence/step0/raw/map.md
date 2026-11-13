# task/wo/update-schema-batch-mismatch-1/evidence/step0/raw/ — step 0's event log

One JSON record per line, from `probe/print_batches.py` on main `dd77603a`'s release module (`_native.abi3.so`, named on the first line) with the print hook applied. `attempt` and `outcome` lines come from the probe; `plan`, `batch`, `done` and `register` lines come from the hook (one `plan` kept per door and statement); `setup-failed` marks a table the door could not create.

- `main-dd77603a.jsonl` — 372 attempts on the Spark and native doors.

Up: [../map.md](../map.md).
