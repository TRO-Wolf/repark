# Overnight report — run 17b of 2026-09-15 → 16 (the facade surfaces, IO and type widths of the 1.5 Spark-parity campaign)

**Session:** one Opus orchestrator (overnight-17b), 2026-09-15 19:56 → 2026-09-16 07:15 EDT · **Orchestrator:** Claude (claude-opus-5) ·
**Lanes:** `rb-` · **Builders:** Devin SWE-2 (both units, the owner's "use devin for at least a few of the workers") ·
**Reviewers:** Grok 4.6 (critic-logic, the two S2-21 perf reads, the verification critic) · **Beside:** run 17a (functions),
run 17c (the SQL door) · **Grants:** G-1..G-5 as briefed, stop opening lanes 06:30, finished 07:15. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md). STATUS.md, tags and the release
pipeline untouched; no file owned by run 17a or 17c edited.

## 1. Census slice — before / after

`census.py` over run 15's PySpark 4.1.2 surface dump, on a release native.

| Surface | PySpark | Present before | Present after | Missing after |
|---|---|---|---|---|
| DataFrame | 115 | 108 | **114** | `metadataColumn` |
| Column · GroupedData · DataFrameReader · DataFrameWriter · DataFrameWriterV2 · DataFrameNaFunctions · Catalog · SparkSession · Row · types | — | complete | complete | — |

**Before** (main `0355ef5e`, 20:10): DataFrame 108/115 — missing `asTable` `exists` `freqItems` `lateralJoin` `metadataColumn`
`scalar` `transpose`. **After**: `freqItems` and `transpose` merged (#640); `scalar`, `exists`, `lateralJoin` and `asTable` merged (#647).
Re-measured on merged main at 07:12: **DataFrame 114 / 115**. Six of the seven names closed; `metadataColumn` (DF-METADATA-COL-1) is the whole remainder of the
DataFrame census, and its oracle cells (`metadata_*` in `dfrust3_probe_2026-09-15.json`) are already recorded.

## 2. Per-PR table

| PR | Unit | Actor tier and rounds | Reviews (Grok 4.6) | State |
|---|---|---|---|---|
| #640 | **DF-RUST-3** — `DataFrame.freqItems`, `DataFrameStatFunctions.freqItems`, `DataFrame.transpose` as Rust kernels: a DataFusion UDAF implementing Spark's `FreqItemCounter` and an eager `ResolveTranspose` kernel in repark-core, `#[pyfunction]` bindings, thin facade | **Devin SWE-2**, 4 rounds (58 / 47 / 94 / 23 agent steps) | logic **NEEDS_REMEDIATION** 1 P1 + 2 P2 $0.90; Rust perf **NEEDS_REMEDIATION** 2 P2 (turn-1 stall $0.02, resumed $0.87); Python perf **PASS** $0.42; verification **PASS** $0.37 | **merged** `fb754bf0`, tree-equal 05:31 |
| #647 | **DF-SUBQUERY-1** — `DataFrame.scalar` / `.exists` / `.lateralJoin` / `.asTable` over `Column.outer`: scoped resolution, the `__repark_single_row` guard UDAF, the EXISTS-in-projection rewrite and the lateral-projection hoist in repark-core; `TableArg` and the table-argument UDTF path on the facade | **Devin SWE-2**, 3 rounds (115 / 25 / 90 agent steps) | logic **NEEDS_REMEDIATION** 0 P1 + 4 P2 $0.68; Rust perf **PASS** $1.09; Python perf **PASS** $0.57 | **merged** `39f49fb3`, tree-equal 07:12 |

## 3. Devin SWE-2 vs Muse — the comparison the owner asked for

Run 17b ran **every** unit on Devin, so the Muse column here is run 16b's same-campaign baseline rather than a measurement of
tonight's box.

| | Devin SWE-2 (run 17b) | Muse Spark 1.3 contributor (run 16b, same campaign) |
|---|---|---|
| rounds | 7 (4 on DF-RUST-3, 3 on DF-SUBQUERY-1) | 8 across two units |
| work per round | 58 / 47 / 94 / 23 and 115 / 25 agent steps — median 52 | 144–574 steps, median ~250 |
| rounds lost | **0** to the provider; 0 rate-limit hits (last night lost two) | 0 |
| wall time, round 1 | DF-RUST-3 4 h 51 m; DF-SUBQUERY-1 4 h 51 m (both ran 19:09 → 01:15 in parallel) | comparable |
| wall time, later rounds | 25–75 min each | comparable |
| cost | $0 (free tier) | $0 (subscription) |
| gates in the hand-back | green and accurate every round; each claimed count reproduced when the orchestrator re-ran it | same |
| failure mode seen | **round 1 of DF-SUBQUERY-1 finished the entire unit and did not commit** — "no commit was requested" | a round ignoring the comment ban once |

**The finding that matters for future briefs:** Devin read "Do the card below, run the gates, commit, write handback.json" as prose
and left 35 finished files uncommitted. The same brief shape has worked on Muse all campaign. A Devin brief must put the commit in
its own **imperative numbered step**. Cost: one extra round (25 steps, ~20 min) — cheap, but it would have been expensive at the
stop time. Otherwise Devin was excellent: it read Spark's bytecode with `javap` unprompted to settle `FreqItemCounter`, it HALTed
with a correct, well-premised question rather than guessing on map-key equality, and its long first round (4–5 h of reading before
the first file) produced code that needed no structural rework.

**Grok 4.6 as reviewer: 8 rounds, $4.92.** One turn-1 stall (the known pattern), fixed by resuming the same session with a proceed
mandate; the resumed round produced a full report. The reviews were worth the money — see §4.

## 4. The review rounds actually found things

Every UNMEASURED review claim was measured on live PySpark 4.1.2 before it was ruled on; **five short JVM probes tonight**
(`l101_probe`, `mapkey_probe`, `nullmap_probe`, `width_spark`, plus repark's answers to the same width cells).

- **L-101 (P1, DF-RUST-3) — the critic was right and its diagnosis was wrong.** It filed `+0.0` / `-0.0` counted as two keys and
  reasoned from `Double.doubleToLongBits`. Measurement showed Spark collapses signed zeros **and** treats every NaN as equal to
  nothing — the exact opposite of `doubleToLongBits` on both counts. The real rule is Scala `BoxesRunTime` numeric equality on a
  `mutable.Map[Any, Long]`: primitive IEEE `==`. Had the fix been written from the critic's reasoning it would have been wrong in
  both directions. *This is the whole case for the measure-before-ruling discipline.*
- **R-13** (the actor's own HALT, then confirmed independently): Spark never dedupes freqItems **map** keys (identity), while array
  and struct keys dedupe by content.
- **V-101** (verification critic, P2): the never-equal map arm also matched a NULL map. Measured: Spark dedupes NULL maps. Narrowed.
- The final measured contract, now on the `DF-FREQITEMS-1` registry row: **freqItems key equality is Spark's boxed-value equality —
  floats by IEEE `==` (signed zeros collapse, NaN equals nothing), non-null maps by identity, NULL and everything else by content.**
- **L-201 (P2, DF-SUBQUERY-1):** the lateral hoist drops the right side's `SubqueryAlias` qualifier, so `LATERAL (…) t` with a
  qualified reference — the ordinary SQL spelling — fails. It also falsified the unit's own claim that the LATERAL SQL-door gap was
  closed, which is why the unit was sent back rather than shipped with a BACKLOG row.

## 5. Rulings taken under G-2 (R-17b-1..24; full text in the unit ledgers)

- **R-17b-4/5/6 — LOGICAL-WIDTH-1 is one PR, and much smaller than it looked.** Measured before carding: the ENGINE is already
  exact — `to_arrow()` carries `int16` / `int8` / `float32` / `binary` through `createDataFrame`, arithmetic, aggregates, casts,
  `union`, the SQL door and a parquet round trip, all matching Spark. The entire divergence is one match in
  `crates/repark-spark/src/type_table.rs`: `ArrowNameSurface::LogicalKey` widens Int8/Int16 → `int`, Float32 → `double` and Binary →
  `string`. Its only entry point is `logical_type_key`, whose only caller is `arrow_type_key` in repark-python. The Python decode in
  `DataFrame.schema` **already** has `short` / `byte` / `float` / `binary` arms, and nested types already recurse through the
  correct `Describe` surface. Scope cuts ruled: `toPandas` dtypes out (Spark's answer unrecordable — no pandas in the oracle env);
  nullability out; `F.lit(b"...")` only if reachable without run 17a's files. See §8 — the card is written and ready.
- **R-17b-11/12/15/20** — the four measured rulings of §4.
- **R-17b-13** — the S2-21 perf findings on DF-RUST-3 (per-row `ScalarValue` boxing; the O(capacity) min-scan and O(P·C²)
  worst-case merge) are **ledger residue, not fixed**: Spark's own `FreqItemCounter` boxes per row into a `mutable.Map` and performs
  the same linear `minBy`, so repark is in Spark's class, and at the default support (C=100) the merge term is negligible.
- **R-17b-16** — `BACKLOG_BASELINE` needed a second ratchet (112→111 by the actor, main independently 112→111, both removals
  applying, measured 110). `make verify` caught it. A ratchet down, recorded in `scripts/map.md`.
- **R-17b-21/22 — two `make verify` reds were investigated, not retried blind.** One raced the actor in the same clone; one was two
  gate scripts racing plus a single `repark-iceberg` test failing under load (`cargo test -p repark-iceberg --lib` alone: 434
  passed). Run cleanly: rc=0. Both were artefacts of a box carrying three orchestrators, three Grok critics and two Devin rounds.
- **R-17b-23** — DF-SUBQUERY-1's four P2s go back to the actor rather than shipping as BACKLOG rows, because L-201 falsifies the
  unit's own SQL-door claim.

## 6. Rust-first roll-call (Python-only logic left in Python, with reason)

| Unit | Left in Python | Why it may stay there |
|---|---|---|
| DF-RUST-3 (#640) | `freqItems` argument shapes (`NOT_LIST_OR_TUPLE`, `NOT_ITERABLE`, `NOT_FLOAT`, the support range), the name→column resolution and its sorted suggestion list, `transpose`'s index-argument coercion, and error-metadata attachment (`_spark_error_class` / `_spark_message_parameters` / `_spark_sql_state`) | API plumbing and exception attributes the engine does not own. Every count, merge, eviction, key comparison, type fold, sort and Arrow build is Rust. One known duplication is recorded: `statistics._java_double` re-implements the Rust Java-double renderer, because moving it would touch a file run 17a owns tonight (Y-3). |
| DF-SUBQUERY-1 (#644) | the four bindings' argument checks (`NOT_DATAFRAME`, `NOT_STR`, `NOT_COLUMN_OR_STR`), `TableArg`'s three builders and their ordering guards, the `TABLE(name)` SQL call-arg spelling, and the Python UDTF `eval` loop for a table argument | argument checks and API plumbing; the UDTF `eval` loop is Python **because the UDTF body is Python** — the reviewer measured the crossing as per-batch Arrow, not per-row PyO3. All scoped resolution, outer-reference rewriting, decorrelation, the EXISTS rewrite, the lateral hoist and the single-row guard are Rust in repark-core. |

## 7. What did not land, and exactly where it stands

Six of the nine ordered units did not open. The clock, not the box, was the limit: both Devin round-1s ran 19:09 → 01:15 in
parallel (4 h 51 m each of reading-then-building), which is most of the night, and the two units then took four and three
review-driven rounds to close properly.

| Unit | State | Where it stands |
|---|---|---|
| **LOGICAL-WIDTH-1** (owner ruling Q-16b-3) | **carded and oracled, not opened** | The blast radius is MEASURED (§5 R-17b-4) and the card is written: `~/repark-lanes/briefs/run17b/brief-logicalwidth-1.md`, with the live-Spark oracle `width_spark_2026-09-15.json` (22 cells), repark main's answers to the same cells, and the probe that wrote both. It is a **one-PR unit touching four match arms in `crates/repark-spark/src/type_table.rs`** plus `fillna`'s widening cast, not the cross-cutting change it looked like. This is the highest-value thing on the next run's desk. |
| **IO-ORC-1** | not opened | Run 16b's brief and fixtures are still valid (`/tmp/oc-worker/qb-oracle/brief-ioorc-1.md`, `orc-fixtures/`). orc-rust 0.8.0 already builds in the workspace tree. |
| **DF-METADATA-COL-1** | not opened | The last DataFrame census name. Oracle cells `metadata_*` already recorded in `dfrust3_probe_2026-09-15.json`. |
| **WITHCOLUMNS-NESTED-1**, **TODF-REF-1** | not opened | DECIMAL-CACHE-1 side cards; no oracle recorded yet. |
| **IO-TEXT-PART-POOL-1** (Q-16b-4) | not opened | Run 16b's reproduction stands (a tight pool refuses loudly at 256 keys). |

**PR #647 (DF-SUBQUERY-1) merged as `39f49fb3`, tree-equal, at 07:12** — three minutes inside the window, on the last CI job.

## 8. Owner questions (with recommendations)

- **Q-17b-1 — brief the commit as a numbered step for Devin.** One Devin round finished a whole unit and did not commit (§3).
  *Recommendation:* add "the commit is step N, in imperative form" to the runbook's brief-assembly section. One line, prevents a
  lost round. No decision needed beyond agreeing.
- **Q-17b-2 — raise the file-descriptor soft limit on this box.** Two `make verify` reds and one worker round-trip tonight were fd
  exhaustion at the 1024 soft limit under three-orchestrator load (§5 R-17b-21/22); the workers now work around it by hand.
  *Recommendation:* set `LimitNOFILE=65536` on the `systemd-run --user` launches in `_lib/r7-launch.sh` and the gate scripts, so no
  round has to discover it again.
- **Q-17b-3 — SUBQ-CELLS-1, a 20-minute follow-up.** DF-SUBQUERY-1's round 3 honestly reported five cells it could not settle from
  the fixture and pinned shape rather than values. The orchestrator then measured all five on live PySpark 4.1.2 and re-ran them
  against the unit's native: **repark matches Spark exactly on every one** (`subq_wanted_probe_2026-09-16.json`).
  *Recommendation:* a small follow-up folds the five measured cells into `facade_df_subquery_oracle.json` and repoints those pins at
  them. Nothing is wrong today; the pins simply assert less than the evidence now supports.
- **Q-17b-4 — one stale clone needs an owner hand.** `/tmp/rb-build` (DF-RUST-3's build clone, ~40 G) survived its merge because
  removing it needs an approval this session could not give. Nothing depends on its objects (checked against every
  `alternates` file). *Recommendation:* `rm -rf /tmp/rb-build`. `/` is at 325 G free, so it is housekeeping, not pressure.
- **Q-17b-5 — was running every unit on Devin the right call?** The owner asked for "at least two"; both units ran on it, so
  tonight has no same-night Muse control. *Recommendation:* next run, split the slate — the unit with the clearest card on Devin,
  the exploratory one on Muse — so the comparison in §3 stops leaning on a cross-night baseline.

## 9. Pointers

- Unit ledgers: `task/ledgers/staging/df-rust-3-ledger.md`, `task/ledgers/staging/df-subquery-1-ledger.md`
- Orchestrator rulings R-17b-1..24: `~/repark-lanes/briefs/run17b/rulings-17b.md`
- The next run's desk: `~/repark-lanes/briefs/run17b/` — the LOGICAL-WIDTH-1 card, `oracle/` (six live-Spark probe files recorded
  tonight) and `reviews/` (all eight reviewer reports)
- Up: [map.md](map.md)
