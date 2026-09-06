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
sort order (measured, not built); quoted sort-column identifiers (loud refusal); the fork
pin; `STATUS.md`; `briefs/next-sequence.md`.

**Writable paths:** `crates/repark-spark/src/{alter.rs,alter_write_order.rs,lib.rs,router.rs,map.md}`,
`crates/repark-spark/src/tests/{alter_write_order.rs,mod.rs,map.md}`,
`crates/repark-iceberg/src/write/{sort_order.rs,distribution.rs,partition_write.rs,append.rs,merge/mod.rs,mod.rs,map.md}`,
`scripts/check_rust_file_size.py` (ratchet-down rows only),
`python/repark/tests/{test_write_order_dist_1.py,map.md}`,
`docs/perf/{iceberg-write-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7 + §2,
this ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every
dependency, `.github/`, every other ledger.

## Plan

- [x] Reproduce on the base tree: all five forms against v2 + v3 partitioned tables; record refusal texts.
- [x] Spark oracle (live PySpark 4.1.2, one JVM, killed at exit): metadata.json effects per form, sequences, errors, DESCRIBE, metadata_log, subsequent CTAS/OVERWRITE/MERGE layout + per-file monotonicity.
- [ ] Facade pins red on the base release native (`test_write_order_dist_1.py`).
- [ ] Rust pins red against the unwired code, then green: DDL round-trips, resets, refusals, gating.
- [ ] The DDL: pre-parse `alter_write_order.rs` (alter.rs is at its exact ceiling — deletion only there) + `write/sort_order.rs` one-transaction primitive over the fork's `replace_sort_order`.
- [ ] The write path: `write.distribution-mode` gating in `hash_distribution`; per-writer sort in the two funnel entries; CTAS node inherits through the funnel.
- [ ] Measure after: `ctas_partitioned8` + `insert_overwrite` cells, three passes of five, control paired; Spark row-set equality.
- [ ] Registry rows, baseline §9, maps, attestation, gates.

## PROPOSITION LEDGER — WRITE-ORDER-DIST-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `WRITE ORDERED BY (a, b DESC NULLS LAST)` appends a sort order, makes it default, and sets `write.distribution-mode = range`, on v2 and v3. | Rust pins round-tripping `metadata.json` per version; the facade DDL pin; the live pin. Red before the fix. | **OPEN** | Oracle §8: order 1 `[id asc nulls-first, name desc nulls-last]`, default 1, dist `range`, identical on v2/v3. |
| C-002 | `WRITE LOCALLY ORDERED BY (…)` appends a sort order and makes it default, and leaves `write.distribution-mode` untouched. | Rust pin asserting the property is absent before and after; facade pin. | **OPEN** | Oracle §8: default 1, dist `None` on a fresh table and `none` preserved through the sequence (v6). |
| C-003 | `WRITE DISTRIBUTED BY PARTITION` sets `write.distribution-mode = hash` and resets the default sort order to the unsorted order 0 without appending an order. | Rust pin over a table with a declared order; facade pin. | **OPEN** | Oracle §8: v3/v7 `default=0`, order list unchanged, dist `hash`. The fork's `add_sort_order` dedups to order 0. |
| C-004 | `WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (…)` sets both the sort order and `hash`; the observed sixth form without `LOCALLY` behaves the same. | Rust pins for both spellings; facade pin for the brief's spelling. | **OPEN** | Oracle §8: v8-equivalent `default=new`, dist `hash`; Spark accepts bare `ORDERED BY` there too. |
| C-005 | `WRITE UNORDERED` resets the default sort order to unsorted order 0 without appending an order, and sets `write.distribution-mode = none`. | Rust pin over a table with a declared order; facade pin. | **OPEN** | Oracle §8: v5 `default=0`, order list unchanged, dist `none`. |
| C-006 | A bad sort column, an empty order list, `WRITE DISTRIBUTED BY (col)`, and trailing tokens all refuse loud; nothing is committed. | Rust pins per shape; the metadata version does not advance. | **OPEN** | Oracle: `ValidationException: Cannot find field 'nope' in struct: …`; `no viable alternative at input ')'`; `mismatched input '(' expecting 'PARTITION'`. |
| C-007 | `write.distribution-mode = none` skips the hash rule (CTAS file count returns to writers × values); unset and `hash` keep one file per value. | Rust gating pins; facade CTAS pins at `shuffle.partitions = 8`. Red before the fix. | **OPEN** | Oracle: CTAS with `none` → 16 files, `hash` → 8 files over a 2-partition source. |
| C-008 | When a default sort order is declared, every data file a staged write commits is monotone in that order, and Spark reads the same row set. | Facade pins over CTAS + INSERT OVERWRITE + MERGE reading parquet back with pyarrow; a Spark row-set leg. Red before the fix. | **OPEN** | Oracle: 8/8 monotone after ORDERED BY on overwrite and MERGE; 0/8 without an order. |
| C-009 | After the same five statements on both engines, RePark's `metadata.json` sort orders, default id, and distribution property equal Spark's. | One live pin, fresh tables per form per engine, compared field for field. | **OPEN** | Oracle §8 table; the dedup corner (reused order id) is avoided by using fresh tables. |
| C-010 | `range` writes the observed Spark layout on partitioned tables (hash distribution + per-file sort); the unbuilt global cross-file ranging is a stated residual, not a silent gap. | Facade pin: DDL `WRITE ORDERED BY` then CTAS writes 8 sorted files; registry `WRITE-RANGE-1` BACKLOG. | **OPEN** | Oracle: range + sorted on a partitioned table wrote one sorted file per value. |
| C-011 | The cost is measured on the baseline `ctas_partitioned8` and `insert_overwrite` cells (three passes of five, control paired) and reported honestly in `docs/perf/iceberg-write-baseline.md`. | §9 with every pass median, load, and RSS; no claim beyond the measured cells. | **OPEN** | Method of WRITE-DISTRIBUTION-1 §8. |
| C-012 | No dependency moves and RePark spawns nothing: `git diff origin/main -- Cargo.toml Cargo.lock` is empty and the sort runs on DataFusion's executor. | The diff; `make rust-panic-ban` exit 0. | **OPEN** | The sort helper drives `SortExec` over a `MemoryExec`; the tasks are DataFusion's. |

VERDICT: 12 clauses, 0 PROVEN, 12 OPEN, 0 REJECTED.

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

To be recorded as the pins land. None is committed.

## 7. Design, and the alternatives

To be written as the code lands.

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
