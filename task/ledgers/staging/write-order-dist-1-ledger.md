# Unit ledger — WRITE-ORDER-DIST-1 · `ALTER TABLE … WRITE ORDERED BY` / `WRITE DISTRIBUTED BY` and the write properties they set

**Date:** 2026-09-06 · **Branch:** `feat/write-order-dist-1` · **Base:** `origin/main` `1883968b` (v1.1.0) ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `V3-COV-5` **FIXED**; `RDF-SORT-1`, `WRITE-DISTRIBUTION-1` gain a dated sentence; new row `WRITE-ORDER-DIST-1` **FIXED**; new row `WRITE-RANGE-1` BACKLOG (global cross-file ranging).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Spark users declare a table's write layout with five `ALTER TABLE … WRITE …`
forms; RePark refuses all five. Separately the write path hash-distributes partitioned CTAS
writes unconditionally (WRITE-DISTRIBUTION-1) — it never reads `write.distribution-mode` —
and never sorts within a file. This unit builds the DDL and honours the metadata it sets.

**Two brief premises corrected against the tree (2026-09-06).** (1) The hash rule is applied
on partitioned **CTAS only** (`write_data_files_from_plan`, called from `ctas.rs` and
`repark-sql create_table.rs`) — not on OVERWRITE/MERGE, which stage through the stream
funnel without any distribution rule. Gating therefore lands in `hash_distribution` (CTAS)
and per-writer sorting lands in the two funnel entries so every staged path inherits it.
(2) `task/ledgers/staging/write-distribution-2-ledger.md` does not exist on this tree; this
ledger follows `write-distribution-1-ledger.md` as its form.

**Not in this unit:** `rewrite_data_files` sort/z-order (fork R135 ceiling, stays refused);
plain `INSERT INTO` row order (the fork's `TaskWriter` owns it); global cross-file ranging
on unpartitioned tables (`WRITE-RANGE-1`); `DESCRIBE TABLE EXTENDED` rendering of the
sort order (measured, not built); single/double-quoted sort-column identifiers (loud refusal;
backticks are accepted — the oracle takes them, §8 addendum); the fork
pin; `STATUS.md`; `briefs/next-sequence.md`.

**Writable paths:** `crates/repark-spark/src/{alter.rs,alter_write_order.rs,lib.rs,router.rs,map.md}`,
`crates/repark-spark/src/tests/{alter.rs,alter_write_order.rs,mod.rs,map.md}`,
`crates/repark-iceberg/src/write/{sort_order.rs,distribution.rs,append.rs,merge/mod.rs,mod.rs,map.md,merge/map.md}`,
`scripts/{check_rust_file_size.py,map.md}` (ratchet-down rows only),
`python/repark/tests/{test_write_order_dist_1.py,_v3_statement_coverage_golden.py,_v3_statement_coverage_repark.py,map.md}`,
`python/repark-parity/{tests/test_cap_1_source_file_line_cap.py,tests/map.md,bench/writepath/probe_cell.py,bench/writepath/map.md}`,
`docs/{map.md,spark-sql-iceberg-parity.md}`, `docs/perf/{iceberg-write-baseline.md,map.md}`,
this ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every
dependency, `.github/`, every other ledger.

## Plan

- [x] Reproduce on the base tree: all five forms against v2 + v3 partitioned tables; record refusal texts.
- [x] Spark oracle (live PySpark 4.1.2, one JVM, killed at exit): metadata.json effects per form, sequences, errors, DESCRIBE, metadata_log, subsequent CTAS/OVERWRITE/MERGE layout + per-file monotonicity.
- [x] Facade pins red on the base release native (`test_write_order_dist_1.py`): 10 failed, 1 skipped.
- [x] Rust pins red against the unwired code, then green: DDL round-trips, resets, refusals, gating.
- [x] The DDL: pre-parse `alter_write_order.rs` (alter.rs is at its exact ceiling — deletion only there) + `write/sort_order.rs` one-transaction primitive over the fork's `replace_sort_order`.
- [x] The write path: `write.distribution-mode` gating in `hash_distribution`; per-writer sort in the two funnel entries; CTAS node inherits through the funnel.
- [x] Measure after: `ctas_partitioned8` + `insert_overwrite` cells, three passes of five, control paired; Spark row-set equality.
- [x] Registry rows, baseline §9, maps, attestation, gates.

## PROPOSITION LEDGER — WRITE-ORDER-DIST-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `WRITE ORDERED BY (a, b DESC NULLS LAST)` appends a sort order, makes it default, and sets `write.distribution-mode = range`, on v2 and v3. | Rust pins round-tripping `metadata.json` per version; the facade DDL pin; the live pin. Red before the fix. | **PROVEN** | `write_ordered_by_sets_order_and_range` (Rust) + `test_write_ordered_by_sets_sort_order_and_range` (facade, v2+v3) + the C-009 live leg; red on base (refusal), green on branch. |
| C-002 | `WRITE LOCALLY ORDERED BY (…)` appends a sort order and makes it default, and leaves `write.distribution-mode` untouched. | Rust pin asserting the property is absent before and after; facade pin. | **PROVEN** | `write_locally_ordered_by_leaves_distribution_untouched` (both levels); red on base, green on branch. |
| C-003 | `WRITE DISTRIBUTED BY PARTITION` sets `write.distribution-mode = hash` and resets the default sort order to the unsorted order 0 without appending an order. | Rust pin over a table with a declared order; facade pin. | **PROVEN** | `write_distributed_by_partition_sets_hash_and_resets_order` (both levels; 2 orders, default 0); red on base, green on branch. |
| C-004 | `WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (…)` sets both the sort order and `hash`; the observed sixth form without `LOCALLY` behaves the same. | Rust pins for both spellings; facade pin for the brief's spelling. | **PROVEN** | `write_distributed_by_partition_locally_ordered_sets_both` (both spellings, Rust) + `test_write_distributed_by_partition_locally_ordered_sets_both` (facade); red on base, green on branch. |
| C-005 | `WRITE UNORDERED` resets the default sort order to unsorted order 0 without appending an order, and sets `write.distribution-mode = none`. | Rust pin over a table with a declared order; facade pin. | **PROVEN** | `write_unordered_resets_order_and_sets_none` (both levels; 2 orders, default 0, `none`); red on base, green on branch. |
| C-006 | A bad sort column, an empty order list, `WRITE DISTRIBUTED BY (col)`, and trailing tokens all refuse loud; nothing is committed. | Rust pins per shape; the metadata version does not advance. | **PROVEN** | `write_order_bad_column_refuses_and_commits_nothing` + `write_order_malformed_shapes_refuse` (Rust) + `test_write_order_bad_column_refuses_without_committing` (facade; metadata count unchanged). The bad-column pin is red-then-green across the boundary: it fails on base for the wrong reason (blanket refusal, no `nope` needle) and passes here for the right one. |
| C-007 | `write.distribution-mode = none` skips the hash rule (CTAS file count returns to writers × values); unset and `hash` keep one file per value. | Rust gating pins; facade CTAS pins at `shuffle.partitions = 8`. Red before the fix. | **PROVEN** | `none_distribution_mode_skips_the_hash_rule` (32 files) + `hash_and_range_distribution_modes_hash_by_partition_value` (8 files) + `unknown_distribution_mode_is_a_planning_error` (Rust); `test_write_distribution_none_ctas_skips_the_hash_rule` (facade, 32 vs 8). Provocations R1/R2 §6. Red on base (unconditional hash), green on branch. |
| C-008 | When a default sort order is declared, every data file a staged write commits is monotone in that order, and Spark reads the same row set. | Facade pins over CTAS + INSERT OVERWRITE + MERGE reading parquet back with pyarrow; a Spark row-set leg. Red before the fix. | **PROVEN** | `test_write_ordered_overwrite_writes_sorted_files` + `test_write_ordered_merge_writes_sorted_files` (8/8 monotone, full row count) + `test_write_ordered_overwrite_row_set_matches_spark` (live: same row set per value on both engines) + the Rust CTAS-node pin `written_files_are_sorted_by_the_declared_order`. Red on base (the DDL refuses), green on branch. |
| C-009 | After the same five statements on both engines, RePark's `metadata.json` sort orders, default id, and distribution property equal Spark's. | One live pin, fresh tables per form per engine, compared field for field. | **PROVEN** | `test_write_order_metadata_matches_spark_after_same_statements`: 11 passed live (both live legs green), Spark 4.1.2, one JVM beside at most one other, reaped at exit. |
| C-010 | `range` writes the observed Spark layout on partitioned tables: the hash shape, and a CTAS replace keeps it while resetting the default order to 0 (corrected 2026-09-06 — the brief's "8 sorted files" assumed the replace preserves the default order; the oracle refutes it on both engines); the unbuilt global cross-file ranging is registry `WRITE-RANGE-1` BACKLOG, a stated residual. | Facade replace-shape pin; `WRITE-RANGE-1` BACKLOG. | **PROVEN** | `test_write_ordered_ctas_replace_keeps_hash_layout_and_resets_order` (8 files, 2 orders, default 0, `range` — Spark's measured shape); first draft asserted sorted files and failed on part=0, corrected against the oracle (§8 addendum). Per-file sort under `range` is pinned by C-008 (its DDL sets `range`). |
| C-011 | The cost is measured on the baseline `ctas_partitioned8` and `insert_overwrite` cells (three passes of five, control paired) and reported honestly in `docs/perf/iceberg-write-baseline.md`. | §9 with every pass median, load, and RSS; no claim beyond the measured cells. | **PROVEN** | §9: no overhead unordered (control ratios overlap), +71 ms best-median (+5.4%) ordered on the 1e6 overwrite; every pass median, load, and RSS listed. |
| C-012 | No dependency moves and RePark spawns nothing: `git diff origin/main -- Cargo.toml Cargo.lock` is empty and the sort runs on DataFusion's executor. | The diff; `make rust-panic-ban` exit 0. | **PROVEN** | The diff is empty (§10); `make rust-panic-ban` exit 0; the sort is `SortExec` over an in-memory source on the caller's task, no spawn, no new dependency. |

VERDICT: 12 clauses, 12 PROVEN, 0 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: write-order-dist-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is stated against a measurement or a red-then-green pin. The C-010 pin first asserted sorted files after replace and failed on the branch; the oracle refuted the premise on both engines and the pin asserts the measured agreement instead. The cost is reported as control-paired ratios because the box was loaded and the cells swing with it; the unordered pair measures as zero and the ordered cost as +71 ms best-median.
      artifacts: [docs/perf/iceberg-write-baseline.md, task/ledgers/staging/write-order-dist-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: All five WRITE forms plus the sixth spelling Spark accepts, v2 and v3, ASC/DESC with every NULLS default and override, case-insensitive matching, identical-order id reuse, the UNORDERED reset, bad-column/empty-list/malformed-shape/trailing-token refusals, quoted-vs-backtick columns, none/hash/range/unset distribution modes, partitioned and unpartitioned funnels, CTAS/overwrite/MERGE staged writes; live Spark on the same five statements and the same DDL plus overwrite.
      artifacts: [crates/repark-spark/src/alter_write_order.rs, crates/repark-iceberg/src/write/sort_order.rs, crates/repark-iceberg/src/write/distribution.rs, python/repark/tests/test_write_order_dist_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap or expect in production; an unknown sort column is a loud DataInvalid and commits nothing; malformed shapes and unknown distribution modes are typed Plan errors; a non-identity sort transform refuses loud. make rust-panic-ban exit 0.
      artifacts: [crates/repark-spark/src/alter_write_order.rs, crates/repark-iceberg/src/write/distribution.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Each writer sorts only its own stream through DataFusion's SortExec on the caller's task; the abort flag still skips finish and close on failure and the CTAS node still sweeps the attempt. No spawn, no unsafe, no new dependency.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs, crates/repark-iceberg/src/write/append.rs, crates/repark-iceberg/src/write/merge/mod.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The abort paths are unchanged and their pins stay green: a bad column commits no metadata version (count pinned), an unknown distribution mode fails at plan time before any file is staged, and make verify runs the full workspace suite green.
      artifacts: [crates/repark-spark/src/tests/alter_write_order.rs, python/repark/tests/test_write_order_dist_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Live Spark 4.1.2 leaves equal sort orders, default ids and distribution properties after the same five statements, commits the same row set per partition value after the same DDL plus overwrite, and takes the same replace shape (orders kept, default 0, range kept, unsorted files). 12 passed under REPARK_PARITY_LIVE=1, one JVM beside at most one other, reaped at exit.
      artifacts: [python/repark/tests/test_write_order_dist_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The base native reds all ten always-run facade pins; the unwired gate reds the none pin alone; the unwired sort predicate reds the three sort pins. Section 6.
      artifacts: [task/ledgers/staging/write-order-dist-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Base and branch back to back on the same bed, three passes of five timed statements per cell, every pass median, the load at every pass, RSS at every pass, the file counts, and paragraphs on what the unordered pair does NOT show and what the pass-3 outliers are.
      artifacts: [docs/perf/iceberg-write-baseline.md]
    - id: AT-9
      status: N/A
      justification: No AWS surface is touched; the DDL and the write path run over a memory catalog on the local filesystem, and no catalog seam, credential path or region behaviour changes.
    - id: AT-10
      status: ATTACKED
      evidence: Cargo.toml and Cargo.lock byte-identical to origin/main; STATUS.md, briefs/next-sequence.md, .github untouched; the fork pin does not move.
      artifacts: [docs/spark-sql-iceberg-parity.md, Cargo.toml]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

| # | provocation | pins that redden |
|---|---|---|
| R1 | the base release native (`origin/main` `1883968b`, own worktree + venv) under the ten always-run facade pins | all ten: the five DDL transitions, the bad-column/malformed refusals (wrong refusal, needle missing), the `none` gating count, the sorted overwrite/MERGE legs, the replace shape — `10 failed, 1 skipped in 11.26s`; the same file on the branch reads `10 passed, 1 skipped` |
| R2 | `distribution_is_none` forced to `Ok(false)` (the gate unwired, hash unconditional) | `none_distribution_mode_skips_the_hash_rule` alone: 8 files where 32 are expected; the other thirteen distribution pins stay green |
| M1 | `default_sort_is_declared` forced to `false` on top of R2 (no writer ever sorts) | the three sort pins — `declared_sort_order_sorts_batches_across_batch_boundaries`, `written_files_are_sorted_by_the_declared_order`, `unpartitioned_sorted_write_keeps_sorted_files` — plus the R2 pin still red: 4 failed, 10 passed |
| — | the C-010 first draft asserted sorted files after DDL + CTAS replace | red on the branch itself (`AssertionError` on `part=0`, 15,000 rows unsorted) — the pin bit, the premise was wrong, and the oracle settled it (§8 addendum); the corrected shape pins green |
| — | the old `alter_unsupported_forms_refuse_loud` WRITE blocks | reddened `make verify` (`816 passed; 1 failed`) until retired — the previous contract's pins caught exactly the behavior this unit changes |

None is committed: R2/M1 were uncommitted edits, verified red, then restored with `git checkout`.

## 7. Design, and the alternatives

**Why a pre-parse intercept, not the sqlparser AST.** sqlparser carries none of the five
forms — `WRITE` after the table name is not in its ALTER grammar — and the partition-spec
forms this unit copies already pre-parse for the same reason. The tokenizer is sqlparser's
own (Databricks dialect), so identifiers and quoting lex the way the rest of the door lexes;
only the clause walk is hand-written. The alternative, extending the sqlparser dependency, is
a fork of a dependency this repo refuses to fork.

**Why a sibling module, not an `alter.rs` arm.** `alter.rs` sits at its exact file-size
ceiling: any arm added there is a ceiling breach by construction. `alter_write_order.rs` holds
the parse + execute, `alter.rs` only loses the refusal it replaces (1830→1821, ratcheted
down with the mirror).

**Why one transaction over the fork's `replace_sort_order`.** The pinned fork exposes
`Transaction::replace_sort_order` with `asc`/`desc` builders, and its `add_sort_order`
dedups — an empty field list resolves to the unsorted order 0 without appending, an
identical order reuses its id — which is exactly Spark's reset/reuse behavior from the
oracle sequence. No `F-*` ask was needed. The property set rides the same transaction, so a
bad column commits neither half (pinned by the unchanged metadata count).

**Why the sort lives in the two funnel entries.** Every staged write — CTAS node partitions,
INSERT OVERWRITE, MERGE, predicate DML — drains through `fanout_sorted_*` (partitioned) or
`drive_unpartitioned` (unpartitioned), so one placement sorts all of them and `partition_write.rs`
is untouched. Each writer collects its own stream and sorts it through DataFusion's `SortExec`
over an in-memory source on the caller's task: no spawn (the `clippy.toml` ban), no new
dependency (C-012), and no behavior change when no order is declared (the drivers delegate
straight through). The alternative, a `SortExec` node in the CTAS plan, would have sorted only
CTAS and left the funnel paths unsorted.

**Why `range` is hash plus per-writer sort.** Spark's range needs a shuffle-sort — a global
exchange, a unit of its own. The brief offered the loud-refusal route or the cheap rule; the
oracle shows range + sorted on a partitioned table as one sorted file per value, which is what
hash + per-writer sort produces. The unbuilt global ranging is filed as `WRITE-RANGE-1`
BACKLOG, stated in the registry rather than silent.

**Why the replace pin asserts the reset, not sorted files.** The brief's C-010 pin assumed a
CTAS replace preserves the declared order. The fork's `begin_replace` stages an unsorted
default (deduped to order 0) while merging properties, and the oracle shows Spark doing the
same: orders kept, default 0, `range` kept, 8 unsorted files. Both engines agree, so the pin
asserts the agreement — layout plus metadata — instead of the sorted files neither engine
writes. The per-file sort under `range` is still pinned, by the C-008 overwrite/MERGE legs
whose DDL sets `range`.

## 8. Measurement — the Spark oracle (2026-09-06)

Live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, `local[2]`, ANSI on, UTC,
`shuffle.partitions = 8`, one JVM beside at most one other, killed at exit. Probes:
`/tmp/writeorder_oracle.py` (DDL matrix + subsequent INSERT OVERWRITE) and
`/tmp/writeorder_oracle2.py` (sequences + errors + CTAS/MERGE/append). Tables partitioned
by `part` (8 values, 20,000 shuffled rows probe 1; `range(0, 8000)` probe 2).

**Base-tree refusals (release native at `1883968b`, v2 and v3 identical).**
`WRITE ORDERED BY` / `WRITE DISTRIBUTED BY [PARTITION LOCALLY ORDERED BY]` →
`UnsupportedOperationException: This feature is not implemented: ALTER TABLE WRITE ORDERED
BY / WRITE DISTRIBUTED BY is not supported yet — sort-order evolution is out of I7 READY
(partition-spec evolution is ADD/DROP/REPLACE PARTITION FIELD)`.
`WRITE LOCALLY ORDERED BY` / `WRITE UNORDERED` → `ParseException: SQL error:
ParserError("Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET
TBLPROPERTIES after ALTER TABLE, found: WRITE at Line: 1, Column: 20")`.
Untouched metadata carries `sort-orders=[{order-id 0, fields []}]`, default 0, no
`write.distribution-mode`.

**DDL matrix — every form on v2 and v3 (identical across versions).** Fresh table per
form; `id long`, `name string`, `part int`, source ids 1/2/3.

| form | sort-orders after | default | `write.distribution-mode` |
|---|---|---|---|
| `WRITE ORDERED BY (id, name DESC NULLS LAST)` | + order 1 `[id identity asc nulls-first, name identity desc nulls-last]` | 1 | `range` |
| `WRITE LOCALLY ORDERED BY (id)` | + order 1 `[id asc nulls-first]` | 1 | unchanged (`None`) |
| `WRITE DISTRIBUTED BY PARTITION` | unchanged `[order 0]` | 0 | `hash` |
| `WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (id)` | + order 1 `[id asc nulls-first]` | 1 | `hash` |
| `WRITE UNORDERED` | unchanged `[order 0]` | 0 | `none` |

**Sequence on one table (v1 CTAS → v8, `range(0, 8000)`).** v2 `ORDERED BY (id)`:
+order 1 `[id asc nf]`, default 1, `range`. v3 `DISTRIBUTED BY PARTITION`: orders
unchanged, **default 0**, `hash`. v4 `ORDERED BY (name DESC)`: +order 2 `[name desc
nl]` — a bare `DESC` defaults to `NULLS LAST`, a bare `ASC` to `NULLS FIRST` — default 2,
`range`. v5 `UNORDERED`: orders unchanged, **default 0**, `none`. v6 `LOCALLY ORDERED BY
(id DESC NULLS FIRST)`: +order 3 `[id desc nf]`, default 3, dist stays `none`. v7
`DISTRIBUTED BY PARTITION`: orders unchanged, **default 0**, `hash`. v8 `DISTRIBUTED BY
PARTITION ORDERED BY (id)` — the sixth form, bare `ORDERED BY` without `LOCALLY`, which
Spark accepts: orders unchanged, **default 1** (the identical existing order is reused,
not appended), `hash`. Resets reuse order 0; equal orders dedup. Each ALTER is one
metadata version; `metadata_log_entries` gains one row per ALTER (`timestamp, file,
latest_snapshot_id, latest_schema_id, latest_sequence_number`, sequence unmoved at 1).
`DESCRIBE TABLE EXTENDED` lists `sort-order=id ASC NULLS FIRST, name DESC NULLS LAST`
and `write.distribution-mode=range` inside Table Properties.

**Refusals.** `WRITE ORDERED BY (nope)` → `ValidationException: Cannot find field 'nope'
in struct: struct<1: id: optional long, 2: part: optional int, 3: name: optional
string>`. `WRITE ORDERED BY ()` → `AnalysisException: no viable alternative at input
')'`. `WRITE DISTRIBUTED BY (id)` → `AnalysisException: mismatched input '(' expecting
'PARTITION'`.

**Subsequent writes.** INSERT OVERWRITE (`SELECT * FROM src ORDER BY id DESC`, 20,000
rows) after each form: 8 files × 2,500 rows in all five cases; per-file `id` monotone
ASC in 8/8 files for the three order-carrying forms, 0/8 for `hash`-only and `none`.
CTAS with `TBLPROPERTIES ('write.distribution-mode' = …)` over a 2-partition source:
`none` → 16 files (2 tasks × 8 values, no exchange), `hash` → 8 files. MERGE (`UPDATE
SET name` on all even ids) after `ORDERED BY (id)`: 8 files, 8/8 monotone. Append after
`ORDERED BY (id)`: 8 → 16 files.

**What this unit copies.** The five metadata transitions above (plus the sixth spelling),
the ASC/DESC null defaults, the loud bad-column refusal, `none` skipping the exchange,
and per-file sortedness after an order-carrying form. What it does not copy:
cross-file global ranging on unpartitioned tables (registry `WRITE-RANGE-1`), and the
`DESCRIBE` rendering (measured here, not built).

## 8b. Measurement — the replace and spelling addendum (2026-09-06, second session)

Same oracle (banner `4.1.2 UTC` on both probes), same discipline, both JVMs reaped at exit.
Probes: `/tmp/writeorder_replace_oracle.py`, `/tmp/writeorder_bare_oracle.py` (throwaway, not
committed).

**CREATE OR REPLACE after `WRITE ORDERED BY (id)`.** Spark: the order list is kept
(`[order 0, order 1]`), the default resets to **0**, `write.distribution-mode = range` is
kept, and the replace writes 8 files × 1,000 rows, **0/8 monotone**. RePark measures
identical: the fork's `begin_replace` stages an unsorted default (deduped to order 0) while
`set_properties` merges the statement properties over the previous ones, the staged write
sees no declared order, and the publish carries orders `[0, 1]`, default 0, `range` with 8
unsorted hash-layout files. This refuted the brief's C-010 pin shape ("DDL then CTAS writes
8 sorted files") on both engines; the pin asserts the measured agreement instead (C-010).
A replace restarts version numbering at v0, so a name-sorted metadata read goes stale across
it — the pin resolves the current file through `metadata_log_entries`, whose last row the
fork synthesizes as the current file.

**Bare and backticked order lists.** Spark accepts `WRITE ORDERED BY id` (no parens) and
`WRITE ORDERED BY (\`id\`)` — both `OK`, default 1, two orders. The parser's bare-list arm
and backtick acceptance are Spark-equal, not a superset; both are pinned at the Rust level.

## 9. Questions for the owner

None reached the owner. The two candidate questions answered themselves against the oracle:
the bare order list and backticks are accepted by Spark (§8b), and the replace reset is
Spark's own shape (§8b) — both pinned, neither a divergence. No `F-*` ask: the pinned fork
exposes `Transaction::replace_sort_order`, so no fork work was needed.

## 10. Gates

Every command in the brief, real exits, on the lane at the unit's last commit. The brief's
pytest line names `test_write_distribution_2.py`, which does not exist on this tree (like
its ledger, a brief premise corrected in the header) — the three files that exist ran.

| gate | exit | result |
|---|---|---|
| `make verify` | 0 | `ci` (fmt, workspace clippy, `rust-panic-ban`, the structure gates, py-lint, py-format, lock, toml, spell) plus the Rust workspace suite, every crate and integration binary green |
| `.venv/bin/python -m pytest python/repark/tests/test_write_order_dist_1.py python/repark/tests/test_write_distribution_1.py python/repark/tests/test_perf_ice_writepath_1.py -q` | 0 | 20 passed, 5 skipped (the live legs, without `REPARK_PARITY_LIVE`) on the release native |
| `REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest python/repark/tests/test_write_order_dist_1.py -q -rs` | 0 | 12 passed in 31.96 s, Spark 4.1.2, one JVM beside at most one other, reaped at exit (one earlier attempt died in a transient gateway-startup race beside another lane's JVM and passed on retry) |
| `make py-test-facade` | 0 | 5,264 passed, 251 skipped in 741 s (the target's maturin step leaves a DEBUG native; the release native was restored afterwards and the pins re-read green) |
| `make py-test-dbt` | 0 | 59 passed, 1 skipped in 246 s |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 | 624 passed in 58 s |
| `make check-map-sync` | 0 | 223 maps clean |
| `make check-ledger-grammar` | 0 | 66 live ledgers clean (327 clauses, 910 pinned clause ids) |
| `make check-ledgers` | 0 | 262 ledgers in bins, 766 links resolve, frozen rule clean |
| `make check-docs-compaction` | 0 | clean |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 | clean |
| `uvx typos@1.47.2 .` | 0 | clean |
| `git diff origin/main -- Cargo.toml Cargo.lock` | — | empty |

Disk: 803 GB free at hand-back. The base worktree `/tmp/wo-base-1883968b` (base release
native + venv, built for the red-proof and the §9 before-column) was removed with `git
worktree remove --force` after the last measurement; the lane's `scratch/` (the 1e6 bed and
the cell logs) and `.ivy2/` (the brief's ivy redirect) are untracked and never staged. No
JVM or pytest this unit started is left running.

## 11. Merge with WRITE-DISTRIBUTION-2 (`origin/main`, 2026-09-06)

`git merge origin/main` lands WRITE-DISTRIBUTION-2 (#406) and CSV-INFER-PERF-1 (#405) under
this branch's eleven commits. Nine files conflicted; each resolution keeps both units'
behaviour, and the §10 header's corrected premise now holds: `test_write_distribution_2.py`
and the WD-2 ledger exist on this tree and ran below.

- `write/distribution.rs` (the only code conflict): union — the distribution-mode gate plus
  staged-write sort stays, WD-2's `PartitionRouter` dispatcher stays, and the dispatcher now
  honours `write.distribution-mode`: `none` deals whole batches round-robin (an `Either` arm
  in `route_partitioned_stream`), `hash`/unset/`range` route; each stream worker sorts its
  routed parts through the existing `fanout_sorted_stream` when a default sort order is
  declared, as the CTAS path does. One new Rust pin
  (`none_distribution_mode_deals_stream_batches_round_robin`) covers the merged behaviour;
  every pin of both units moved verbatim (modulo the module-file dedent and rustfmt's reflow).
- The eight doc/table conflicts: union — both sides' rows kept; our baseline section renumbers
  §9 → §10 with a pre-merge caveat; the three sentences the other unit falsified (the Spark
  paragraph's "hash rule is CTAS-only", the determinism paragraph's "whatever the writers
  received", the map's "serial path untouched") are corrected; the cap table takes 1792 for
  `merge/mod.rs` (the lower side, exact) and ratchets `append.rs` 1883 → 1882 (both sides
  removed one disjoint line).
- The union file is 1259 lines against the 1000 ceiling, so the merge commit moves the tests
  to `write/distribution/tests.rs` and a follow-up commit extracts the dispatcher to
  `write/distribution/router.rs`, each with its map row; no EXCEPTIONS row.

| gate | exit | result |
|---|---|---|
| `make verify` | 0 | `ci` plus the Rust workspace suite, every crate green, on the merged tree |
| `.venv/bin/python -m pytest python/repark/tests/test_write_order_dist_1.py python/repark/tests/test_write_distribution_1.py python/repark/tests/test_write_distribution_2.py python/repark/tests/test_perf_ice_writepath_1.py -q` | 0 | 23 passed, 7 skipped (the live legs) on the merged release native |
| `.venv/bin/python -m pytest python/repark/tests/test_mw7_scale_smoke.py -q -k copy_on_write` | 0 | 1 passed (WD-2's path-set C-003 control) |
| `REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest python/repark/tests/test_write_order_dist_1.py -q -rs` | 0 | 12 passed in 26.69 s, Spark 4.1.2, one JVM, reaped at exit |
| `REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest python/repark/tests/test_write_distribution_2.py -q` | 0 | 5 passed in 23.70 s, Spark 4.1.2, one JVM, reaped at exit |
| `make py-test-facade` | 0 | 5,334 passed, 256 skipped in 719.60 s (the DEBUG native was replaced with the release native afterwards and the pins re-read green) |
| `make py-test-dbt` | 0 | 59 passed, 1 skipped in 39.49 s |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 | 624 passed in 30.15 s |
| `make check-map-sync` | 0 | 224 maps clean |
| `make check-ledger-grammar` | 0 | 68 live ledgers clean (342 clauses, 924 pinned clause ids, 3 exception rows) |
| `make check-ledgers` | 0 | 264 ledgers in bins, 768 links resolve, frozen rule clean |
| `make check-docs-compaction` | 0 | clean |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 | clean |
| `uvx typos@1.47.2 .` | 0 | clean |
| `git diff origin/main -- Cargo.toml Cargo.lock` | — | empty |
| `git merge-base --is-ancestor origin/main HEAD` | 0 | the merge holds |

Disk at merge hand-back: re-checked before the facade run; the lane keeps `target/` (debug
plus release natives), `scratch/` and `.ivy2/`, all untracked or ignored and never staged. No
JVM or pytest this merge started is left running.
