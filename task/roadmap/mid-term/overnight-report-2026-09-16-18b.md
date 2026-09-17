# Day report — run 18b of 2026-09-16 (the facade surfaces, IO, type widths and the memory guard of the 1.5 Spark-parity campaign)

**Session:** one Opus orchestrator (day-18b), 2026-09-16 09:02 → 21:15 EDT · **Orchestrator:** Claude (claude-opus-5) · **Lanes:** `sb-` ·
**Actor tier:** Muse Spark 1.3 contributor for every unit (owner, 2026-09-16 morning), GLM 5.3 Flash for one build-free sweep ·
**Reviewers:** Grok 4.6 (critic-logic, the two S2-21 perf reads, the verification critic) · **Beside:** run 18a (functions), run 18c (the
SQL door) · **Grants:** G-1..G-5 as briefed; stop opening lanes 20:30, finished 21:15. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md). STATUS.md, tags and the release pipeline
untouched; no file owned by run 18a or 18c edited.

## 1. Census slice — before / after

`census.py` (run 15b) over run 15's PySpark 4.1.2 surface dump, on a release native.

| Surface | PySpark | Present before (main `33c87cbf`, 09:15) | Present after (main at 21:00) | Missing after |
|---|---|---|---|---|
| DataFrame | 115 | 114 | 114 | `metadataColumn` (DF-METADATA-COL-1, draft PR — §7) |
| Column · GroupedData · DataFrameReader · DataFrameWriter · DataFrameWriterV2 · DataFrameNaFunctions · Catalog · SparkSession · Row · types | — | complete | complete | — |

The slice's census was already one name from complete, so today's merged value is in **shape**, not names: every narrow width now
answers Spark on both doors (#655), a Never-OOM defect that could surface a Rust panic is gone (#653), and six subquery pins now assert
measured Spark answers instead of shapes (#652). `DataFrameReader.orc` was a declared refusal; its read is implemented on a draft PR (§7).

## 2. Per-PR table

| PR | Unit | Actor and rounds | Reviews (Grok 4.6) | State |
|---|---|---|---|---|
| #652 | **SUBQ-CELLS-1** (Q-17b-3) — six live-measured subquery cells folded into `facade_df_subquery_oracle.json`; three pins repointed, two SQL-door pins added | GLM 5.3 Flash, 1 round (48 steps, $0.12) | logic **PASS** $0.31 | **merged** `e7d01022`, tree-equal 10:44 |
| #653 | **NEVER-OOM-PANIC-1** (Q-17c-7) — `NljBuildSideReset`: the NLJ build child runs through `reset_plan_states` so DataFusion 54.1's spill fallback really spills instead of panicking; the LEFT family with >1 right partition refuses typed | Muse, 2 rounds (227 / 153 tool calls) | logic **NEEDS_REMEDIATION** 1 P1 + 1 P2 + 1 P3 $1.16; Rust perf **PASS** (P3s) $0.69; verification **PASS** $0.76 | **merged** `02abfd0e`, tree-equal 13:23 |
| #655 | **LOGICAL-WIDTH-1** (Q-16b-3) — `LogicalKey` arms answer `short`/`byte`/`float`/`binary`; fillna's width decision is a Rust kernel (`na_fill.rs`); zero-arg grouped aggregates keep the narrow widths | Muse, 3 rounds (221 / 188 / 133) | logic **NEEDS_REMEDIATION** 1 P1 + 2 P2 $0.54; Rust perf **PASS** $0.52; Python perf **PASS** $0.40; verification **PASS** $0.73 | **merged** `778fa9b1`, tree-equal 15:44 |
| #659 | **IO-ORC-1** (Q-15B-1) — read-only ORC scan on orc-rust 0.8.0: types, codecs, paths, options, partitions, user schema, projection; write stays DECLARED | Muse, 3 rounds (471 / 182 / 182) | logic **NEEDS_REMEDIATION** 3 P2-or-lower $0.62; Rust perf **NEEDS_REMEDIATION** 3 P2 $0.66; Python perf **PASS** $0.59 | **green, ready, not merged** — CI all pass; orchestrator gates on the rebased release head: parity 757 passed, `make verify` rc 0, facade 9324 passed with 2 failures that are a pre-existing clock-boundary flake (R-18b-20, §8); behind #660 in the merge queue at the stop time |
| #662 (draft) | **DF-METADATA-COL-1** — `DataFrame.metadataColumn` and the hidden `_metadata` struct on parquet / csv / json / text scans | Muse, 4 rounds (443 / 455 / stopped / stopped at 21:00) | logic **NEEDS_REMEDIATION** 1 P1 + 3 P2 + 1 P3 $0.81; Rust perf **NEEDS_REMEDIATION** 2 P2 $0.94; Python perf **PASS** $0.65 | **draft, parked** — P1 L-501 and three P2s in progress (§7) |

**Grok 4.6 total: 15 rounds, $9.73.** Not opened (clock): WITHCOLUMNS-NESTED-1, TODF-REF-1, IO-TEXT-PART-POOL-1 — cards and live-Spark
oracles are written (§7).

## 3. Muse discipline

| Unit | Rounds | Resumed rounds | Gate claims overturned by the audit | Comment-ban rejections | Other audit rejections |
|---|---|---|---|---|---|
| NEVER-OOM-PANIC-1 | 2 | 1 | 0 — every reported count reproduced (facade 9038/9039, parity 757, verify rc 0) | 0 | — |
| LOGICAL-WIDTH-1 | 3 | 2 | 0 | 0 | A-1: the fillna width fix landed as a Python type branch — sent back under Q-17a-2 |
| DF-METADATA-COL-1 | 4 (one stopped by the orchestrator) | 3 | n/a — rounds 1 and 2 never reached gates | **1** (A-2: 18 added `///` lines) | A-3 a 1031-line Rust file; A-4 **two rounds ended at the step budget with zero commits** |
| IO-ORC-1 | 3 | 2 | 0 | **1** (A-5: five `#` lines) | A-6 a wrong trailer (`Authored-By: muse-worker (run 18b, round 1)`); A-7 multi-path schema merge in Python |

No provider outage and no lost round. `grep -c untrusted stderr.log` was 0 on every launch. The failure that cost the most time was new:
on the two biggest units Muse spent a whole 400-step round implementing without committing, despite a numbered commit step, and the
second round repeated it after an explicit "commit what is green" instruction; only a brief whose first numbered step *is* the commit
worked. See Q-18b-2.

## 4. What the reviews found (every disputed claim measured on live PySpark 4.1.2 or re-run by the orchestrator first)

- **NEVER-OOM L-201 (P1) — fixing the panic exposed a wrong answer.** Once the NLJ fallback actually spilled, LEFT and LEFT ANTI joins at
  an 8 MiB pool with 4 partitions returned **4,001,953 rows instead of 1,001,953** (re-measured by the orchestrator before ruling).
  DataFusion 54.1's own source documents that fallback as incorrect for one-sided emission with several right partitions; it disables
  only FULL. Ruling R-18b-1: the LEFT family refuses typed; INNER / RIGHT spill and are pinned by value (count + sum + sha256) against
  the 1 GiB run. A panic was replaced by correctness, not by a silent duplication.
- **LOGICAL-WIDTH L-301 (P1)** — `groupBy().sum()` with no arguments silently dropped every smallint / tinyint / float column once they
  stopped reporting as `int` / `double`: a Python keep-set predicate knew only the old tokens. Measured Spark keeps them (and the result
  types); fixed with a 16-consumer sweep.
- **LOGICAL-WIDTH L-302 (P2) → ARITH-FLOAT-INT-1** — the old widened label had been hiding a real engine divergence: Spark computes
  `float_col + 1` in double (`2.100000023841858`), repark in Float32 (`2.0999999046325684`), on both doors. Pinned exactly; the
  coercion rule is the SQL/type-coercion lane's (Q-18b-1).
- **DF-METADATA L-501 (P1)** — `df.distinct().select(..., "_metadata.row_index")` answered 3 rows where Spark answers 2: the hidden
  struct was injected under the Distinct and became part of its key. Measured on Spark (`distinct_then_metadata`), sent back (R-18b-16).
- **IO-ORC R-01 (P2)** — each ORC partition was fully decoded into a `Vec` before streaming, outside the session memory pool; the scan
  now streams (R-18b-14). R-02/R-03 (five footer opens per file, no stripe split; 20-column sum 3.99 s vs parquet 0.35 s) are the
  dated BACKLOG row `IO-ORC-PERF-1`.
- **A reviewer and a worker both wrong about one glob** — the logic critic's L-402 was a pin reading the fixture directory's `map.md`
  through `m*`; the worker's first fix silently skipped non-`.orc` files, which Spark does not do. The ruling kept Spark's behaviour and
  changed the pin (R-18b-12).

## 5. Rulings taken (G-2; full text in `~/repark-lanes/briefs/run18b/rulings-18b.md` and the unit ledgers)

Owner rulings applied and recorded in the ledgers: Q-16b-3 (LOGICAL-WIDTH-1), Q-17b-3 (SUBQ-CELLS-1), Q-17c-7 (NEVER-OOM-PANIC-1),
Q-15B-1 (IO-ORC-1), Q-17a-2 (every unit's roll-call and audits A-1 / A-7), Q-17a-3 (`target/debug` dropped after every pushing gate),
Q-17a-4 (whole facade + parity suites on every gate), Q-17b-1 (commit as its own numbered step in every brief), Q-17b-2 (`LimitNOFILE`
on every `systemd-run`).

- **S-1** — a correlated `LIMIT 1` without ORDER BY is nondeterministic in Spark: match-set assertion kept, schema bound to the cell.
- **R-18b-1..3** — NEVER-OOM: the LEFT-family refusal, value pins per join type, P3 residue (empty build-side child metrics under
  EXPLAIN ANALYZE; O(depth²) resets; spill replays the build side).
- **R-18b-4** — fillna's width-preserving cast is a Rust kernel, not a Python type branch (Q-17a-2).
- **R-18b-5..8** — LOGICAL-WIDTH: the narrow keep-set, ARITH-FLOAT-INT-1 exact BACKLOG, `to(StringType)` over binary is `'hi'` (cell),
  perf P3 residue.
- **R-18b-9 / R-18b-10** — DF-METADATA: derived-expression nullability out of scope (function kernels are run 18a's); the base parquet read
  dropping hive partition columns is BACKLOG `IO-PARQUET-PARTITION-DISCOVERY-1`, not this unit.
- **R-18b-11..15** — IO-ORC: refuse unappliable user-schema types; pin `m?` and keep Spark's extension-agnostic listing; fix or disclose
  mixed writer zones; stream batches; defer footer reuse and stripe splitting to `IO-ORC-PERF-1`.
- **R-18b-16..19** — DF-METADATA: Distinct dedupes on visible columns (or refuses), stacked select / SQL-string filter / aggregates over
  `_metadata` answer Spark, `SQL-METADATA-COL-1` filed, the per-file reread cost deferred to `DF-METADATA-COL-PERF-1`.
- **R-18b-20** — #659's facade gate failed only on the Q-18b-7 clock-boundary flake (CI's facade job passed the same pin); not a blocker.
- **Orchestrator stop of a Muse round** — DF-METADATA round 3 was stopped at 19:50 (its own systemd unit) and resumed as round 4 so the
  measured P1 could be worked before the stop time.

## 6. Rust-first roll-call (under the Q-17a-2 sentence)

| Unit | Left in Python | Why it may stay there |
|---|---|---|
| SUBQ-CELLS-1 | — | test-and-fixture only |
| NEVER-OOM-PANIC-1 | — | the rule, the wrapper, the policy and the refusal are Rust; Python holds only the test harness |
| LOGICAL-WIDTH-1 | `core._is_numeric_type_key` — the zero-arg `GroupedData` keep-set | **a type decision still in Python.** Kept narrow because the shortcut's column expansion is Python plumbing today; moving the keep-set with the shortcut into Rust is a card (Q-18b-4). `key_to_cls` is a display-overlay target selector; fillna keeps argument checks, subset and the Column wrapper |
| IO-ORC-1 | argument shapes (`NOT_STR_OR_LIST`, the positional-mergeSchema `IllegalArgumentException`), the option-map plumbing, the user schema as name/type pairs | API plumbing; the listing, merge, type mapping, conversions and error conditions are Rust (the multi-path merge moved in round 2) |
| DF-METADATA-COL-1 | `NOT_STR`, the `MetadataColumn` Column subclass for getField naming, `errors.py` attribute-aware `getCondition`, and a `fromDDL` pre-step that tolerates `NOT NULL` in a struct type key | the first three are plumbing; **the `fromDDL` pre-step parses a type string in Python** — it should move into the Rust type table before the unit merges (Q-18b-4) |

## 7. What did not land, and exactly where it stands

| Unit | State | Where it stands |
|---|---|---|
| **IO-ORC-1** | **#659 green and ready, not merged** | Branch `feat/io-orc-1`. Every review finding CLOSED by the verification critic. The only open item is merging; `IO-ORC-PERF-1` (footer reuse, stripe split) is the follow-up card. |
| **DF-METADATA-COL-1** | draft PR #662 | Branch `feat/df-metadata-col-1` at the last commit the actor made; the round-4 working tree (P1 L-501 and the three P2s in progress) is saved as `~/repark-lanes/briefs/run18b/df-metadata-col-1-round4-wip.patch`. Next run: apply the patch in the build clone, finish R-18b-16..19 from `/tmp/oc-worker/sb-meta/followup-4.md` (copied to the desk), move the `fromDDL` NOT NULL pre-step into Rust, then a verification critic. The Spark cells for every finding are recorded (`meta_r2_spark_2026-09-16.json`). |
| **WITHCOLUMNS-NESTED-1** | carded and oracled, not opened | Card `~/repark-lanes/briefs/run18b/card-withcolumns-nested-1.md`; 14 live-Spark cells. **Measured today: a NULL struct's field reads as `0` / `0.0` / `''` instead of NULL** (`df.withColumns({"x": df.s.x})`, and `SELECT s.x FROM t`) — a silent wrong answer, the highest-value item on the desk. Also every dotted reference `F.col("s.x")` is unresolved and SQL-door output names are `datafusion.public.t.s[x]`. |
| **TODF-REF-1** | carded and oracled, not opened | Card `card-todf-ref-1.md`; 13 cells. Narrowed by measurement: only a **case-only** rename (`toDF("ID", "PRICE")` over `id, price`) breaks the next `filter` / `withColumn`; real renames, swaps, joins and views already match. |
| **IO-TEXT-PART-POOL-1** (Q-16b-4) | carded, not opened | Card `card-io-text-part-pool-1.md` with the three pool/partition targets to complete. |

## 8. Owner questions (with recommendations)

- **Q-18b-1 — who fixes float×integral arithmetic?** LOGICAL-WIDTH-1 unmasked `ARITH-FLOAT-INT-1`: `float_col + 1` computes in Float32
  (2.0999999046325684) where Spark promotes to double (2.100000023841858), on both doors. It is binary-arithmetic type coercion, the SQL
  lane's rule (BL-20's analyzer landed today, #656). *Recommendation:* a 1.5 card on the 18c lane, extending the BL-20 rule to float with
  an integral operand; the exact pins are already on main and turn red when it lands.
- **Q-18b-2 — Muse on large units runs out of steps without committing.** Two DF-METADATA rounds and one IO-ORC round spent 440–470
  steps and ended with no commit (or a malformed one) despite a numbered commit step; the commit only happened when it was step 1 of the
  brief. *Recommendation:* for any unit expected to exceed ~300 steps, (a) split the brief into rounds whose FIRST step is the commit of
  the previous round's tree, and (b) raise `--max-steps` to 600 for those rounds so a round can reach its gates.
- **Q-18b-3 — the LEFT-family NLJ now refuses under a tight pool where it previously panicked.** That is correct (DataFusion documents
  the fallback as wrong for those join types), but it means a LEFT non-equi join that does not fit the pool cannot complete.
  *Recommendation:* file the upstream DataFusion issue with the measured 4× duplication (the reproduction is in the ledger), keep the
  refusal, and revisit when DataFusion fixes the fallback.
- **Q-18b-4 — two type decisions still in Python.** `_is_numeric_type_key` (the zero-arg grouped-aggregate keep-set) and, on the
  DF-METADATA branch, a `fromDDL` pre-step that parses `NOT NULL` in a struct type key. *Recommendation:* move the keep-set with the
  GroupedData shortcut expansion into Rust as a small FACADE card; make the second a merge condition for DF-METADATA-COL-1.
- **Q-18b-5 — base parquet reads drop hive partition columns.** Found while pinning `_metadata`: `spark.read.parquet(dir)` over `s=a/`
  directories answers `[i]` where Spark answers `[i, s]`. *Recommendation:* a 1.5 card `IO-PARQUET-PARTITION-DISCOVERY-1` (the ORC scan
  already implements Spark's inference and can be the reference).
- **Q-18b-7 — a date-boundary flake in the facade suite.** `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren` compares
  the engine's `current_date` (UTC session) with the host's local `date.today()`; between 20:00 and 24:00 EDT they differ by a day and the
  pin fails on any branch (it failed the orchestrator's #659 gate at 20:45; CI in UTC passed). *Recommendation:* the pin reads today's
  date in the session time zone; a one-line test fix for whichever lane owns the SQL grammar pins (run 18c today).
- **Q-18b-6 — correct-but-slow perf findings were deferred, not fixed.** `IO-ORC-PERF-1` (ORC 11× parquet on a wide sum) and
  `DF-METADATA-COL-PERF-1` (a referenced `_metadata` rereads every file as its own scan). *Recommendation:* confirm deferral to 1.5
  perf cards; neither changes a value.

## 9. Pointers
- Rulings R-18b-1..15: `~/repark-lanes/briefs/run18b/rulings-18b.md`
- Briefs, oracle probes and every reviewer report: `/tmp/oc-worker/run18b/` (copied to `~/repark-lanes/briefs/run18b/` at session end)
- Up: [map.md](map.md)
