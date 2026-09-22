# Charter ledger — IPI-23-MT-AS-OF-1 · `VERSION AS OF` on Iceberg metadata tables, Spark door

**Date:** 2026-09-22 · **Branch:** `fix/ipi-23-mt-as-of` · **Base:** `cdb5e234` (`origin/main`) · **Model:** muse-spark-1.3-contributor (R1) + swe-2-high (R2/R3/R4) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** row `MT-1` in `docs/spark-sql-iceberg-parity.md` re-ruled DECLARED → FIXED (served) this unit.

**Retires:** in flight.

**Scope:** `VERSION`/`TIMESTAMP AS OF` composed with a dotted Iceberg metadata
table on the Spark door. `crates/repark-iceberg/src/catalog/snapshot_metadata_table.rs`
(the snapshot-scoped `SnapshotMetadataTableProvider` over the fork's public
`iceberg::inspect` constructors; `metadata_asof_mode` routes the four log-like
tables to the current table, the seven file-like tables to the resolved snapshot —
unknown numeric ids answer empty with the normal schema — and the five `all_*`
tables to Spark's `Cannot select snapshot in table: <TYPE>` refusal) plus its
`catalog/mod.rs` hook; `crates/repark-spark/src/time_travel.rs` and
`metadata_tables.rs` (the AS OF routing into the provider); the Rust battery
`crates/repark-spark/src/tests/metadata_tables_asof.rs` with `tests/common.rs` and
`tests/mod.rs`; the MT-1 registry row; the facade pins
`python/repark/tests/test_ice_mt_as_of_1.py`; the reformatted
`python/repark/tests/test_metadata_tables.py`; five `map.md` files
(`crates/repark-iceberg/src/catalog/`, `crates/repark-spark/src/`,
`crates/repark-spark/src/tests/`, `python/repark/tests/`, `scripts/` — the last
for the `check_rust_file_size.py` EXCEPTIONS line the branch carries); and this
ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md` and the ANSI door
are untouched; no code comment added anywhere.

**R2 (2026-09-22):** the parenthesized `(t.snapshots) VERSION AS OF` form pinned
as a SQL parse error in the spark-door test, with lockstep maps (commits
`c0b0e583`, `a381924a`).

**R3 (2026-09-22, WO mt-r3-ci):** CI-green mechanics only — `ruff format` on
`test_metadata_tables.py`, the `pins:` key re-pointed `xo55-mt` →
`ipi-23-mt-as-of-1`, this ledger and its map row filed. No test logic or asserted
value changed.

**R4 (2026-09-22, WO mt-r4-pins, critic r1 V-001/V-002):** test-only pin
tightening — every refusal pin now asserts the mapped `engine_err` class plus
the full message, not `contains()` on the DataFusion text: the unknown-ref
(`files`/`snapshots VERSION AS OF 'nope'`) and too-old-timestamp
(`files TIMESTAMP AS OF '2000-01-01 00:00:00'`) refusals pin
`IllegalArgument` with Spark's recorded sentences verbatim; `snapshots
VERSION AS OF 999` pins `IllegalArgument("Cannot find snapshot with ID 999")`
(RePark stays loud; Spark's INTERNAL_ERROR is not copied); the five `all_*`
refusals keep the Spark text and additionally pin their mapped `Analysis`
variant (the gap to Spark's `UnsupportedOperationException` is declared,
not copied); the paren form pins the mapped `Parse` class. The empty-scan
schema pin compares Arrow fields (name, data type, nullability) instead of
rendered text. New facade pin C-006 asserts `IllegalArgumentException` and
the full message for the three recorded refusals. No production change.

## Measurements (decide-then-build evidence)

**M-1 — the oracle is recorded, not re-derived.** The five facade pins replay the
run-25/26 inventory harness cells recorded against live PySpark 4.1.2 and carried
in `/tmp/oc-worker/scoreboard/2026-09-22/matrix.json` (cells `R-MT-SNAPSHOTS-TT`,
`R-MT-FILES-TT`, `R-MT-ENTRIES-TT`, `R-MT-PARTITIONS-TT`, `R-REF-BRANCH-FILES`);
the packet's Spark answers are three data files at the second snapshot and
snapshots-TT equal to snapshots non-TT.

**M-2 — the seed shape behind the counts.** Every pin builds the same fixture: a
format-v2 merge-on-read table `PARTITIONED BY (cat)` with two appends
(`(1,'a','x'),(2,'b','y'),(6,'f','x')` then `(3,'c','x')`) and one delete
(`id = 1`), so the second snapshot carries three live data files across
partitions `x` (3 records) and `y` (1 record).

## PROPOSITION LEDGER — IPI-23-MT-AS-OF-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT * FROM mt.ns.t.snapshots VERSION AS OF <second-id>` answers the same rows and the same schema field names as the un-pinned `SELECT * FROM …​.snapshots`. | `test_snapshots_tt_equals_current` green. | **PROVEN** | Recorded cell `R-MT-SNAPSHOTS-TT`: the pinned rows (sorted compare) and the pinned schema field names both equal the un-pinned table's. pins: ipi-23-mt-as-of-1/C-001 |
| C-002 | `SELECT count(*) FROM mt.ns.t.files VERSION AS OF <second-id>` answers `[[3]]`. | `test_files_tt_scopes_to_snapshot` green. | **PROVEN** | Recorded cell `R-MT-FILES-TT`: three data files live at the second snapshot. pins: ipi-23-mt-as-of-1/C-002 |
| C-003 | `SELECT count(*) FROM mt.ns.t.entries VERSION AS OF <second-id>` answers `[[3]]`. | `test_entries_tt_scopes_to_snapshot` green. | **PROVEN** | Recorded cell `R-MT-ENTRIES-TT`: one entry per live file at the second snapshot. pins: ipi-23-mt-as-of-1/C-003 |
| C-004 | `SELECT partition.cat, record_count FROM mt.ns.t.partitions VERSION AS OF <second-id>` answers `[["x",3],["y",1]]`. | `test_partitions_tt_scopes_to_snapshot` green. | **PROVEN** | Recorded cell `R-MT-PARTITIONS-TT`: per-partition record counts at the second snapshot. pins: ipi-23-mt-as-of-1/C-004 |
| C-005 | After `ALTER TABLE … CREATE BRANCH b0`, `SELECT count(*) FROM mt.ns.t.files VERSION AS OF 'b0'` answers the same count as the un-pinned `SELECT count(*) FROM …​.files`. | `test_branch_files_reads_branch_head` green. | **PROVEN** | Recorded cell `R-REF-BRANCH-FILES`: the branch ref reads the branch head. pins: ipi-23-mt-as-of-1/C-005 |
| C-006 | `SELECT count(*) FROM mt.ns.t.files VERSION AS OF 'nope'`, `… .snapshots VERSION AS OF 'nope'` and `… .files TIMESTAMP AS OF '2000-01-01 00:00:00'` each refuse with `IllegalArgumentException` whose message equals Spark's recorded sentence (`Cannot find matching snapshot ID or reference name for version nope` ×2; `Cannot find a snapshot older than 2000-01-01T00:00:00+00:00`). | `test_mt_as_of_refusals_match_spark` green. | **PROVEN** | Spark 4.1.2 probe 55b recordings; class and full message asserted by equality on the facade. pins: ipi-23-mt-as-of-1/C-006 |

## Gates

| Command | Result |
|---|---|
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| `./scripts/check_crate_dag.sh` | exit 0 (22 internal edges clean) |
| `./scripts/check_manifest.sh` | exit 0 (18 components agree) |
| `./scripts/check_lib_rs.sh` | exit 0 (10 crate roots clean) |
| `./scripts/check_rust_file_size.sh` | exit 0 (804 files clean) |
| `./scripts/check_parity_live_dual_wire.sh` | exit 0 (dual-wire OK) |
| `python3 scripts/sync_map_md.py --check` | exit 0 (323 maps clean) |
| `python3 scripts/ledger_lifecycle.py check` | exit 0 (463 ledgers, 1088 links resolve) |
| `python3 scripts/check_ledger_grammar.py` | exit 0 (245 live ledgers clean) |
| `python3 scripts/check_docs_compaction.py` | exit 0 |
| `python3 scripts/check_docs_links.py` | exit 0 (6297 links clean) |
| `uvx ruff@0.15.22 format --check .` | exit 0 (1130 files formatted) |
| `uvx ruff@0.15.22 check .` | exit 0 |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-mt origin/main HEAD` | exit 0 (`hits=0`) |
| `.venv/bin/python -m pytest -q python/repark/tests/test_ice_mt_as_of_1.py python/repark/tests/test_metadata_tables.py` | `24 passed in 3.10s` |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-23-mt-as-of-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked to its recorded PySpark 4.1.2 cell — row/schema equality on snapshots-TT (C-001), exact counts on files/entries (C-002/C-003), exact partition rows (C-004), branch-head count equality (C-005), refusal class plus full message on the probe-55b refusals (C-006); every assert collects on the Arrow path.
      artifacts: [python/repark/tests/test_ice_mt_as_of_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The shared seed exercises a two-append merge-on-read history with a delete, so the pinned snapshot sits mid-history; the facade pins cover snapshot-id and named-branch AS OF spellings; the Rust battery adds rule-1/rule-2 per-type scoping, timestamp resolution, and unknown-id/ref behavior — R4 pins the mapped `IllegalArgument` class plus the full Spark-recorded message on each refusal, `IllegalArgument("Cannot find snapshot with ID 999")` verbatim on the rule-1 unknown id, and compares the empty scan's Arrow fields (name, data type, nullability) rather than rendered text.
      artifacts: [python/repark/tests/test_ice_mt_as_of_1.py, crates/repark-spark/src/tests/metadata_tables_asof.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The five `all_*` tables refuse with Spark's `Cannot select snapshot in table: <TYPE>` text under their mapped `Analysis` class (the gap to Spark's `UnsupportedOperationException` is declared, not copied) and the parenthesized `(t.snapshots) VERSION AS OF` form pins the mapped `Parse` class; both refusal classes are asserted in the Rust battery.
      artifacts: [crates/repark-spark/src/tests/metadata_tables_asof.rs, crates/repark-spark/src/tests/metadata_tables.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Snapshot resolution runs per query through Session state; the provider streams one PartitionStream per scan with no shared mutable state or global registration.
      artifacts: [crates/repark-iceberg/src/catalog/snapshot_metadata_table.rs, crates/repark-spark/src/time_travel.rs]
    - id: AT-5
      status: N/A
      justification: Read-only metadata scans over the session's own catalog; no auth, secret, or injection surface added.
    - id: AT-6
      status: ATTACKED
      evidence: Registry row MT-1 moved from a DECLARED refusal to the recorded served behavior; the replaced refusal was itself a declared divergence, so no prior green behavior is silently altered — the new answers are pinned to recorded Spark 4.1.2 cells.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_mt_as_of_1.py]
    - id: AT-7
      status: N/A
      justification: The scan streams and projects each batch; no added materialization, unbounded growth, or hot-loop pattern beyond the fork read.
    - id: AT-8
      status: ATTACKED
      evidence: The provider builds on the fork's public snapshot-scoped `iceberg::inspect` constructors at the pinned rev and mirrors `IcebergMetadataTableProvider::try_new` schema sources, so scoped and un-pinned reads agree column for column; no dependency or pin move.
      artifacts: [crates/repark-iceberg/src/catalog/snapshot_metadata_table.rs, crates/repark-iceberg/src/catalog/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals carry Spark's own `Cannot select snapshot in table: <TYPE>` text and the paren form surfaces the parser's `Expected: end of statement`, so a mis-scope is diagnosable from the error, not silent.
      artifacts: [crates/repark-iceberg/src/catalog/snapshot_metadata_table.rs, crates/repark-spark/src/tests/metadata_tables.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Five recorded cells replayed verbatim on the facade harness; every clause carries a `pins:` citation in the test file plus the catalog and tests map.md rows; C-001 asserts schema field names alongside values.
      artifacts: [python/repark/tests/test_ice_mt_as_of_1.py, python/repark/tests/map.md, crates/repark-iceberg/src/catalog/map.md]
```

Every clause above is PROVEN against a recorded PySpark 4.1.2 cell replayed by
the named facade test; no clause is OPEN. Touched files per the Scope paragraph;
the ANSI door, the fork, `Cargo.toml`/`Cargo.lock` and `STATUS.md` are untouched.
`make verify`, the full facade suite and the parity harness were not run per the
work order's gate list; the gates table above is the proof.
