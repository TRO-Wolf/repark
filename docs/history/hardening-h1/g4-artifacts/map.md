# map — docs/history/hardening-h1/g4-artifacts/

## Purpose

Identity-gate evidence for **G-4** (the `crates/repark-spark/src/tests.rs` → `src/tests/`
declared-rename). Travels with [../g4-tests-split-ledger.md](../g4-tests-split-ledger.md).
Archived mid-campaign with the H-1 phase ledgers on **2026-08-11** (G-9). Not an input to any
live gate — re-verify by regenerating the lists against today's tree and comparing leaf
multisets to the committed snapshots here.

## Contents

- `before-list.txt` / `after-list.txt` — full `cargo test -p repark-spark --lib -- --list` paths
  before and after the split.
- `before-leaves.txt` / `after-leaves.txt` — leaf (test-name) multisets.
- `before-names.txt` / `after-names.txt` / `after-names-fmt.txt` — intermediate name extracts.
- `leaf-diff.txt` — empty when the leaf multiset is identical (the gate).
- `name-map.md` — the 202 full-path renames (authoritative rename map).
- `make-ci.log` / `make-test.log` — gate logs from the unit's green run.

## Pointers

- Up: [../map.md](../map.md)
- Unit ledger: [../g4-tests-split-ledger.md](../g4-tests-split-ledger.md)
- Live test layout: [../../../../crates/repark-spark/src/tests/map.md](../../../../crates/repark-spark/src/tests/map.md)

## Debug

| Symptom | First check |
|---|---|
| Leaf multiset no longer matches | Re-run `--list` on today's tree; a post-G-4 test addition is expected drift — the committed lists are dated evidence, not a live lock |
| Name-map row missing for a path | See the ledger's "Drift application at rebase" section (orchestrator transplants) |
