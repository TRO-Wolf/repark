# Unit ledger — ICE-TSNS-COERCION-1 · slice 0 measures the twin oracle and the Spark lattice

**Scope (2026-10-12):** measurement only, on main `572130dda35f739795578134348f99fd7b530cfd`.
No product file changes. The twin is the same statement with `__repark_cast_timestamp_ns__`
or `__repark_cast_timestamptz_ns__` written by hand. Spark 4.1.2 is the second oracle:
`timestamp_ns` stands where `TIMESTAMP_NTZ` stands, and `timestamptz_ns` where `TIMESTAMP` stands.

**Date:** 2026-10-12 · **Branch:** `test/ice-tsns-coercion-1-slice-0` · **Base:** `572130dd` ·
**Model:** Grok 4.7 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the owner's ruling of 2026-10-10 on parity row R-017, card
[ice-tsns-coercion-1-card-2026-10-10.md](../../roadmap/mid-term/ice-tsns-coercion-1-card-2026-10-10.md).
**This ledger closes when the unit's pull request merges.** Slice 0 does not merge that change.

## PROPOSITION LEDGER — ICE-TSNS-COERCION-1 — 2026-10-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The twin of every card spelling is recorded from main 572130dd on every door, five zones. | The twin fixture and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_coercion_1_twins.json`: 107,520 twin, 107,520 statement and 1,920 plain stored cells, 1,120 reads, 1,680 range cells, 5 zones; recorder `_record_ice_tsns_coercion_1_twins.py`; summary `task/wo/ice-tsns-coercion-1/evidence/twins-summary.md`. |
| C-002 | Spark 4.1.2's lattice for the temporal set is recorded in both ANSI modes. | The lattice JSON and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_coercion_1_spark_lattice.json`: 1,540 cells, Spark 4.1.2 read from the live session, both ANSI modes, 5 zones, 0 errors; recorder `_record_ice_tsns_coercion_1_spark_lattice.py`. |
| C-003 | Every twin's kind and zone placement matches Spark's analogue. | The comparison table. | **OPEN** | Comparison table in `twins-summary.md`: kind 560/560 match; placement matches 336/560; 224 `tzns` non-UTC cells differ by the zone offset because RePark's `tzns` seed is wall+00:00 (zone-fixed) while Spark's `src_ltz` is wall-in-session-zone, and the twin passes `tzns` through unchanged. Open: align seeds (re-record) or rule pass-through placement out of scope. |
| C-004 | The gate-open rate, the cost baseline and the four censuses are recorded. | Section 9 of the slice brief. | **PROVEN** | `task/wo/ice-tsns-coercion-1/evidence/`: `gate.md` (rate uncountable from public `EXPLAIN`; hook named), `cost-baseline.json` + `cost-baseline.md`, `pins.md`, `casts.json` + `written-casts.md`, `try-repark.json` + `try-spark.json` + `try-cast.md`, `lambda.json` + `lambda.md`. |
| C-005 | Before the first type_coercion, every card site that holds a nanosecond value is wrapped in the existing cast kernels. | Slice 1. | **OPEN** | Not this slice. |
| C-006 | `nvl` and `ifnull` over the temporal set become `coalesce`, then those kernels apply. | Slice 3. | **OPEN** | Not this slice. SQL `nvl` on main still has no timestamp signature. |
| C-007 | A written `CAST`, `::timestamp`, `to_timestamp` or `TIMESTAMP` literal is unchanged by the card. | Slice 2. | **OPEN** | Not this slice. |

## 1. What step A records (C-001, C-002)

Sources are `timestamp_ns` and `timestamptz_ns`, each holding `2026-01-02 03:04:05.123456789`
and `1969-12-31 23:59:59.999999999`. Targets are `timestamp_ns`, `timestamptz_ns`, and a
microsecond `TIMESTAMP` control. Spellings are `coalesce`, `array(...)[0]`, `CASE`, `if`,
`nvl`, `ifnull`, and `UNION ALL`. Siblings are an untyped NULL, `CAST(NULL AS TIMESTAMP)`,
`CAST(NULL AS TIMESTAMP_NTZ)`, a microsecond column and literal of each kind, and `DATE`.
Doors are the 64 R-017 doors, including MERGE, UPDATE, table-creating doors and the cached
frame. Zones are UTC, America/New_York, Asia/Kolkata, Australia/Lord_Howe and `-03:30`.

Stored cells are read from the Parquet files as int64 ticks. Reads, including field metadata,
are `toArrow()`. The `UNION ALL` write doors use `max(v) GROUP BY id` because a
`ScalarSubquery` does not plan. `INSERT VALUES` of a column sibling is skipped: `VALUES`
has no column scope. `nvl` and `ifnull` twins are `coalesce` with the kernels; the statement
role keeps SQL `nvl` and `ifnull`.

The Spark lattice is five zones, both ANSI modes, the same spellings, and the three range
siblings. The summary `twins-summary.md` lands in step A, written by
`_summarize_ice_tsns_coercion_1.py`.

## 2. Kind and placement (C-003)

Step A recorded 2026-10-12 by Muse Spark (muse-spark-1.3-contributor). Kind
matches Spark on all 560 card-spelling twin reads. Placement matches on 336:
every naive read and every `ns`-source zoned read (the kernel places the naive
wall in the session zone exactly as Spark places `TIMESTAMP_NTZ`). The 224
`tzns` non-UTC reads differ by the zone offset: the seeds differ by
construction and the twin passes `tzns` through, so those cells carry no
coercion signal either way. Two comparison fixes landed with the summary: the
unit match reads `[ns`/`[us` prefixes so zoned types compare, and epoch
microseconds floor like `unix_micros`.

The 35 untyped-NULL stored-value mismatches on nanosecond targets are 15
`union` cells through MERGE doors where statement and twin error identically
(oracle consistent) and 20 `array` cells on the DataFrame-functions door where
the twin text fails inside `functions.expr` while the same text plans through
`spark.sql` and `selectExpr`. Slice 1 wraps programmatically with the kernel's
declared return type, so neither class moves the design. The sketch's 96 hold:
192 cells over `coalesce`/`array`/`CASE`/`if`, both sources, all three targets,
`insert_select`/`insert_values`/`cached_frame_append`/`ctas`, UTC and New York,
zero differ.

## 3. Product clauses left open (C-005, C-006, C-007)

Nothing in `crates/*/src/` or `python/repark/src/` changes in this slice. The gate, the
lattice rewrite, and the temporal `nvl` rewrite are later slices.

## 4. Step B (C-004)

Recorded 2026-10-12 by Muse Spark. Item 1: the rate is uncountable — no
`EXPLAIN` text, plain or `VERBOSE`, prints leaf field types — so `gate.md`
names the missing hook (analyzed leaf schemas, or one analyzer counter on
`Timestamp(Nanosecond, _)`) and counts 19 pandas `datetime64[ns]` source
lines. Item 2: the baseline ran with the cargo lock free and no build up
(load decaying 5.7 to 5.6); set medians agree to <2% except `merge_mor`.
Items 3 to 6 re-ran byte-identical except `lambda.json`, whose physical plan
carries the machine's `RoundRobinBatch` fanout (26 here, 8 before); the
logical plan and the markdown match.

## Q. Questions

No ruling is requested by this ledger until the summary names a kind mismatch or an
untyped-NULL stored-value mismatch on a nanosecond target.

Update (2026-10-12, step A): the tripwire fired — the summary names zero kind
mismatches and 35 untyped-NULL stored-value mismatches — and measurement
disposes it without a ruling; see §2. No ruling requested. C-003 stays OPEN
with its seed question above.
