# Charter ledger — DF-PLAN-INTROSPECT-1 · `DataFrame.inputFiles` + `DataFrame.semanticHash` in Rust

**Date:** 2026-09-14 · **Branch:** `feat/df-plan-introspect-1` · **Base:** `origin/main`
`efcb14efa30ad360503ad1c9b9da2527a923041a` · **Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** EX-DF-11 unchanged (`sameSemantics` stays handle identity; the hash implication is measured in §5).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** DF-SURFACE-A-1's first cut parsed EXPLAIN text for both names and the critic
showed wrong answers (L-006: a `, ` path split into three URIs; L-007: an overlapping-name
join raised instead of listing files; L-009: the hash covered physical batch/shuffle
tokens). Owner standing instruction (2026-09-14): plan introspection lands in Rust.
Home: `crates/repark-core/src/plan_introspect.rs`.

**Not in this unit:** glob-escape semantics for bracket-bearing directory segments (both
engines glob read paths; §6); the bracket-dir writer anomaly (§6, write path — another
lane's territory); Iceberg manifest reads for data files (§5, measured `[]`); memtable
data-content hashing (§5 boundary); `STATUS.md`, `briefs/next-sequence.md`.

## PROPOSITION LEDGER — DF-PLAN-INTROSPECT-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `inputFiles()` answers PySpark 4.1.2 from Rust: the four oracle cells plus a `", "`/bracket filename as one URI, an overlapping-name join listing both sides, csv/json reads, a glob, a cache-kept lineage, and the SQL door. | `test_df_plan_introspect_1.py` inputFiles pins, red on the base. | **PROVEN** | 10 pins green on the release module. Red on base `efcb14ef`: all 19 pins `PySparkAttributeError [ATTRIBUTE_NOT_SUPPORTED]` (both names absent). Oracle filename `part-` is Spark's writer naming; repark writes `<rand>_0.parquet`, so the parquet pin asserts shape (one `file://` URI, exists) not the name. pins: df-plan-introspect-1/C-001 |
| C-002 | `semanticHash()` answers PySpark 4.1.2 from Rust: the five oracle cells plus literals, column order, limits, CAST widths, two file paths, and batch-size independence, folded to a Java-int range. | `test_df_plan_introspect_1.py` semanticHash pins, red on the base. | **PROVEN** | 9 pins green on the release module, same red base as C-001. Fold: low 32 bits of the process-seeded SipHash digest read as signed `i32`, widened to `i64`. pins: df-plan-introspect-1/C-002 |
| C-003 | Example coverage, maps, and ledger grammar close the loop with no baseline raised. | New dataframe example, refreshed inventory, count pin, map rows, grammar checker. | **PROVEN** | `docs/examples/dataframe/plan_introspect.py` covers both names and runs green; `inventory.txt` gains exactly the two rows; `len(rows)` 961 → 963; `check_example_coverage.py` and its parity suite green; `core.py` holds its exact 4035 baseline (two bindings absorbed into one collapsed raise); `check_ledger_grammar.py` green except the orchestrator-owned attestation block. pins: df-plan-introspect-1/C-003 |
| C-004 | No regression: `make verify`, the DataFrame test files, and the API inventory stay green. | The gates. | **PROVEN** | `make verify` green except the sanctioned attestation finding (see C-003); the touched suites (`test_df_surface_a_1`, `test_examples_dataframe_*`, `test_ex_0_example_coverage`, `test_dfcore_1_exports`) green; full facade and parity suites green (counts in the handback). The facade run first failed exactly the three `test_dfcore_1_exports` freeze pins (new `plan_introspect` submodule, two new dir names); the freeze now absorbs them per that file's own precedent. pins: df-plan-introspect-1/C-004 |

## Per-name decisions

| Name | Verdict | Reason |
|---|---|---|
| `DataFrame.inputFiles` | implemented | File-group walk over the built physical plan in `repark-core`; Python binds the name and picks the cache lineage, nothing else. |
| `DataFrame.semanticHash` | implemented | Analyzed-logical-plan canonical hash in `repark-core`; Python binds the name only. |

## 5. Measurements and rulings applied

**sameSemantics implication.** `f.sameSemantics(f)` is `True` and `f.semanticHash()` is
stable, so self-implication holds; independently built twins answer `sameSemantics ==
False` with equal hashes, so the implication holds vacuously on every pinned shape.
EX-DF-11 is unchanged.

**Iceberg exposure.** The fork's `IcebergTableScan` exposes table, snapshot, projection,
predicates, and limit — never a materialized data-file list — so Iceberg frames answer
`[]`. Measured live on this branch (memory catalog, `CREATE TABLE probe_ice_t (a INT)
USING iceberg`, two rows): `collect()` answers both rows, `inputFiles()` answers `[]`,
`semanticHash()` answers `-1080756021`. A data-file list needs manifest I/O the exec
does not do; candidate registry row, reported not filed.

**Memtable boundary.** Same-schema different-data memtables share a hash (schema shape
only — row content needs a scan the hash must never trigger). No pin covers it.

**Snapshot boundary.** Two scans of one table at different snapshots share a hash (the
fallback provider fingerprint is the table type). No pin covers it.

## 6. Out-of-scope observations (not fixed — write path and glob semantics belong elsewhere)

**Bracket-bearing directory segments are un-globbable.** `spark.read.parquet` over a
`br[0]` dir refuses at plan time (`No files found`, or `Pattern syntax error` for an
unbalanced `[`); the COPY write refuses the same way. Both engines glob read paths, so
the Spark side of a bracket-dir read is UNMEASURED (no JVM in this lane). The pin
therefore carries the brackets in the FILE name (`data [0], x.parquet` under an
`if, a` dir): the URI integrity the L-006 class is about, through public doors only.

**Bracket-dir writer anomaly.** Writing into a `brackets [0]` dir drops a stray
`<rand>_0.parquet` in the PARENT directory and names the in-target file
`part-00000.parquet` (the plain path names `<rand>_0.parquet` in-target only).
Write-path territory; reported, not touched.

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.
