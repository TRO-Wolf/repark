# Day report: run 18a, the functions

**Date:** 2026-09-16 09:02 → 21:15 EDT
**Orchestrator:** Claude (claude-opus-5), one of three run-18 orchestrators. 18b owns DataFrame, Column, Session, Catalog and types; 18c owns the SQL door and the parser/planner.
**Charter:** the 1.5 row of the release roadmap: every `pyspark.sql.functions` name answers PySpark 4.1.2 on both doors, or is a dated declared refusal. The actor tier was **Muse Spark 1.3 contributor** on every unit (owner, 2026-09-16 morning). Reviewers ran on Grok 4.6.

## 1. Census slice (functions), before and after

I measured this by import against a release native built from each tree. The name list is the 508 real names in `/tmp/oc-worker/run15/pyspark-surface.json`: the 519 entries minus the 11 typing re-exports. A name counts as absent when `hasattr(repark.spark.functions, name)` is false. A stub that is present but refuses does not count as absent, so the stubs this run closed get their own line.

| Point | Absent names |
|---|---|
| Run start (main `33c87cbf`) | **20** |
| On main at the run's end (after #657) | **20** (#657 closed three present-but-refusing stubs: `json_tuple`, `from_csv`, `schema_of_csv`) |
| On #625 `feat/fnp-agg-1` at `4a3379fe` | 8 (closes 12: `any_value` `count_min_sketch` `grouping_id` `histogram_numeric` `listagg_distinct` `max_by` `min_by` `percentile` `product` `string_agg_distinct` `sum_distinct` `sumDistinct`) |
| On #628 `feat/fnp-math-1` at `10f18d40` | 17 (closes 3: `bround` `conv` `mask`, and the stubs `hash`, `format_number`, `split`) |
| **When both branches merge** | **5**: `aes_encrypt` `aes_decrypt` `try_aes_decrypt` (#628 AES step, approved RustCrypto crates) and `collate` `collation` (#628 step 6, or a declared refusal extending G15) |

Files: `/tmp/oc-worker/run18a/census-before.json`, `census-625.json`, `census-628.json`.

## 2. Per-PR table

| PR | Unit | Scope | Muse rounds | Reviews (Grok 4.6) | State |
|---|---|---|---|---|---|
| **#657** | FNP-GEN-1 steps 3–4 | `json_tuple`, `from_csv`, `schema_of_csv` as Rust kernels (`json/tuple.rs`, `csv/*`, the `GeneratorRewrite` arm, the `CsvFold` rule). The 13 strict-xfails from step 1 are retired, and so are #654's two `schema_of_csv` tripwires | 4 (1 launch, 3 resumes: two step caps and the remediation) | critic-logic **3 P1** + 2 P2; Rust perf **1 P1** (the same panic) + 5 P2; Python perf no P1/P2. **I measured every claim live before ruling, and all were confirmed.** Verification critic: all CLOSED (PERF-006 partial, P3). Its one new P1 had already been fixed in `2006a88d` | **merged `a8b9c2dc`** 17:44, tree-equal; ledger 7/7 PROVEN, moved to completed |
| **#625** | FNP-AGG-1 | The two run-17a door-split P1s (PYPERF-001 `any_value`, PYPERF-002 `product`), then `percentile`, `listagg_distinct`/`string_agg_distinct`, `histogram_numeric`, `grouping_id`, `count_min_sketch` (byte-exact), `sum_distinct`/`sumDistinct` | 7 (1 launch, 6 resumes) | critic-logic **1 P1** (`grouping_id` subset) + 2 P2; Rust perf **2 P1** (O(n³) histogram state, a `///` comment) + 3 P2; Python perf no P1, 2 P2. **All measured live, all dispositioned** (R-18a-19…27) | **Draft, pushed at `4a3379fe`, not merged.** Squashed once onto main `a8b9c2dc`. The round reports `make verify` 0 and the card pins green. **Not yet done on this head:** my whole-facade and whole-parity gate, and the verification critic. CI was started by the push (§9) |
| **#628** | FNP-MATH-1 step 2 + D-8 + D-9 + `mask` | `bround`, `conv`, `hash`, `format_number`, `mask`, the `split` facade on `spark_split`, and BL-6 (`bin`/`rint` refuse BOOLEAN in Rust) | 2 (1 launch, 1 resume after a step cap) | not reviewed yet | **Draft, pushed at `10f18d40`** (rebased onto `778fa9b1`). The round reports 141 passed / 94 failed / 8 xfailed on `test_fnp_math_1.py` against a base of 16 passed; the 94 are the later steps' names |
| — | LIT-DECIMAL-1 step 2 | — | — | — | not reached (§9) |

**Grok 4.6 reviewers: 7 rounds, $5.40.** #657 cost $3.03 (logic $0.83, Rust perf $0.64, Python perf $0.63, verification $0.94). #625 cost $2.16 (logic $0.64, Rust perf $0.82, Python perf $0.70).

## 3. Oracles recorded (live PySpark 4.1.2, this box, one short-lived JVM each)

| Fixture (copied into the branch) | Cells | What it settled |
|---|---|---|
| `fnp_agg_1_p1_spark_oracle.json` | 30 | `any_value`'s `ignoreNulls` must be a boolean **literal** on both doors (`_LEGACY_ERROR_TEMP_1210` for `col('g') > 0` and for `CAST(NULL AS BOOLEAN)`). Also `product` beside a `lit` companion, over an expression, and windowed. Ungrouped `any_value` over two partitions depends on scan order, so its value is pinned as membership |
| `fnp_gen_1_s34_spark_oracle.json` | 80 | The exact error classes that step 1's fixture had hidden behind `Py4JJavaError`: `MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION`, `PARSE_MODE_UNSUPPORTED`, `INVALID_SCHEMA.NON_STRING_LITERAL`, `INVALID_OPTIONS.*`, `UDTF_ALIAS_NUMBER_MISMATCH`. `json_tuple` rendering: Jackson numbers, case-sensitive keys, nested text verbatim. `schema_of_csv('')` → Spark's own `INTERNAL_ERROR` |
| `fnp_gen_1_s34_critic_spark_oracle.json` | 34 | **Confirmed all five critic-logic claims.** An extra token marks the corrupt record. A default TIMESTAMP parses in the **session time zone** (UTC vs New York epochs differ by 18000 s; offsets are kept; NTZ keeps wall clock). DECIMAL rounds HALF_UP and accepts `1e2`. A non-string corrupt column → `INVALID_CORRUPT_RECORD_TYPE` at analysis. A zero-row batch answers empty. Also settled the inference conjectures: `.1` fractions, `Z`, and `yyyy-MM-dd HH:mm` → TIMESTAMP; 24 digits → `DECIMAL(24,0)`; `1.5f` → DOUBLE. Spark tolerates trailing content in `json_tuple` |
| `fnp_agg_1_critic_spark_oracle.json` | 15 | `grouping_id` args must equal the grouping columns **exactly and in order**: even `cube(g,k)` + `grouping_id(k,g)` refuses. `histogram_numeric` keeps sorted bins and merges the closest adjacent pair (scan 30,10,20 → `[{10,1},{25,2}]`). **DISTINCT listagg element order is not a contract:** `a-b-c-d` in one partition, `a-c-d-b` when repartitioned |

**Oracle results that overturned a claim:**
- **A worker's premise was backwards.** Muse asked for a ruling because "Spark marks `hex()` output nullable". The unit's own fixture records `hex(count_min_sketch(...))` as `nullable: False`. Spark's `Hex` is nullable exactly when its argument is, and the kernel now follows that (R-18a-25).
- **A worker's contract was an accident of one fixture row.** `string_distinct.rs` implemented "reverse-scan last occurrence" to reproduce the fixture's `c-b-a`. Critic-logic called it a multi-batch break. The live cells showed Spark has **no** stable order at all, so the pins now compare elements as a multiset (R-18a-21).
- **A critic's P2 conjecture was right.** `histogram_numeric`'s tie handling diverged from Spark on unsorted input (R-18a-20).

## 4. Muse discipline

| | FNP-GEN-1 (#657) | FNP-AGG-1 (#625) | FNP-MATH-1 (#628) | Total |
|---|---|---|---|---|
| Rounds (launch + resumes) | 1 + 3 | 1 + 6 | 1 + 1 | **13** |
| Steps (from `runs.tsv`) | 457 / 331 / 210 + one ~200 | 437 / 181 / 440 / 436 / 150 / 128 / 329 | 510 / 185 | ~4 000 |
| Rounds ended by the step cap | 1 | 2 | 1 | 4 |
| Rounds ended with work **uncommitted** | 0 | **3** (percentile, grouping_id, sketch facade) | 1 (format_number) | **4** |
| HALTs | 0 | 4 (one asked to confirm a sequence the brief already stated) | 1 (at its stop) | 5 |
| Gate or evidence claims overturned by the audit | 0 (its facade 9079 claim was consistent with my 9083) | **3**: "8 reds, 5 in unrelated areas" (my gate found 42 reds, all this unit's or rebase fallout), the backwards `hex` nullability premise, and "halted at the 20:40 hard stop" at **18:48** (it read the UTC clock) | 0 | 3 |
| Comment-ban rejections | 0 | **2**: two `///` lines in the squash (Rust perf read), then a new four-line `///` block in the remediation (I removed it) | 0 | 2 |
| Forbidden flag | 0 | **1**: `git commit --no-verify` (self-reported, amended with hooks live) | 0 | 1 |
| Scope overrun against a ruling | 0 | 1: `sum_distinct` built despite R-18a-18 (accepted as R-18a-28, still unreviewed) | 0 | 1 |

`grep -c untrusted stderr.log` was 0 on every launch. No round lost to a provider outage. No banned trailer anywhere.

**What the table says.** Muse carried three families of real Rust (CSV parsing, a Hive histogram, a byte-exact Count-Min sketch) and absorbed about twenty rulings correctly. The failure modes are procedural, not technical: it did not commit before long steps, it reached for a HALT when a question was already answered, and it misreported the time. The commit rule was briefed as numbered steps (Q-17b-1) and was still missed four times. Every miss happened when a *single* step ran past the step cap.

## 5. Rulings taken (numbered, recorded in the unit ledgers)

- **Owner rulings applied:** Q-17a-1 (#625's P1s opened the run), Q-17a-2 (every argument check this run lives in Rust: `any_value`'s literal check, `mode`'s flag, CSV options/schema validation), Q-17a-3 (`target/debug` dropped after the #657 push), Q-17a-4 (every gate ran the whole parity and facade suites), Q-17b-1 (commit as its own numbered step), Q-17b-2 (`LimitNOFILE=65536` on every `systemd-run`), Q-17c-6 (the launcher proceeded after 10 minutes of cargo activity twice), Q-17c-3 (#654's blanket refusal retired two of this slice's tripwires).
- **R-18a-1** Counts after a rebase are measured, never computed (EX-0, `BACKLOG_BASELINE`, line ceilings).
- **R-18a-2 / R-18a-4 / R-18a-6** CSV/JSON argument decisions live in the kernel or the analyzer rule. `json_tuple` parses once per row. No new parser dependency.
- **R-18a-3** `schema_of_csv('')` raises Spark 4.1.2's own `INTERNAL_ERROR` / `XX000`. It is a Spark defect, and we match its class.
- **R-18a-5** `LATERAL VIEW` cells stay blocked on run 18c.
- **R-18a-7 / R-18a-8 / R-18a-12** Sequencing, and `count_min_sketch` implemented byte-exact rather than declared.
- **R-18a-9 / R-18a-10** The signature helper strips `*args` annotations. The tie order of rows equal on every sort key is not a contract.
- **R-18a-13 / R-18a-14** `INVALID_CORRUPT_RECORD_TYPE` at analysis. The measured inference ladder.
- **R-18a-16 / R-18a-17** I denied a global type-table flip inside a functions PR (18b's surface; #655 landed it hours later). The SQL alias Debug-rendering of CAST arguments is engine naming for 18c, so it is strict-xfail.
- **R-18a-18 → R-18a-28** I moved `sum_distinct` out of #625, the round built it anyway, and I accepted it rather than deleting working pinned code. It needs the verification critic.
- **R-18a-19…R-18a-27** The #625 critic dispositions (§3): `grouping_id` exact order, the Hive histogram, the listagg multiset, the comment removals, percentile/sketch state, `mode` flag as a literal, `hex` nullability, the gate reds, and the three-partition histogram cell as a strict-xfail partitioning artifact.

## 6. Owner questions (with recommendations)

- **Q-18a-1: #625 is pushed but not gated on its final head. How should the next functions run finish it?** The round reports `make verify` 0 and the card pins green. My last full gate ran on the pre-remediation head and was red (42 pins, EX-0, example coverage), and every one of those reds has a dispositioned commit since. *Recommendation:* the next run starts with a Grok verification critic on `4a3379fe` (all R-18a-19…27 items plus the `sum_distinct` code, which no reviewer has read). It runs the gate chain (`/tmp/oc-worker/run18a/gate.sh`: whole facade, whole parity, verify, clippy, sizes) in `/tmp/qa-build2` after a rebase onto main `b8e79fca`+, then queues. CI on the pushed head gives an overnight signal.
- **Q-18a-2: the step cap is where Muse loses commits.** All four uncommitted exits happened when one step crossed the 400-step cap. *Recommendation:* for Muse, brief any kernel step as two numbered commits ("kernel + Rust tests: commit; facade + pins: commit"). The launcher's cap could also rise to 600 for resumed sessions, which reached the cap only mid-step.
- **Q-18a-3: time checks in briefs must name the zone.** A Muse round stopped two hours early because its `date` prints UTC. *Recommendation:* every brief's hard stop is written as `TZ=America/New_York date` plus the UTC equivalent, and the runbook template says so.
- **Q-18a-4: is `sum_distinct` (R-18a-28) acceptable in #625?** It closes the last two AGG census names on a new native `distinct_aggregate_column` pyfunction. The ceiling on `column/mod.rs` forced it into `expr_build.rs`. *Recommendation:* keep it, conditional on the verification critic. Splitting it out now costs a PR and a CI cycle for no gain.
- **Q-18a-5: the collation pair.** `collate` / `collation` are absent. An honest implementation needs collation-aware comparison, which the engine does not have. ICU4X is approved only for slice 13c (Q-16c-2). *Recommendation:* make both a dated DECLARED refusal with Spark's `COLLATION_INVALID_NAME` shape, extending registry row G15, inside #628's step 6. Implement them when slice 13c lands.

## 7. Rust-first roll-call (Q-17a-2): Python-only logic left in Python

Everything **added** this run is Rust behind a thin wrapper. From #657: `json/tuple.rs`, `csv.rs`, `csv/from_csv.rs`, `csv/schema_of_csv.rs`, `csv/fold.rs`, `generator/json_tuple.rs`. From #625: `any_value.rs` (now bound on both doors), `percentile.rs`, `string_distinct.rs`, `histogram_numeric.rs`, `grouping.rs`, `count_min_sketch.rs`, and the `hex` nullability. From #628: `bround.rs`, `conv`, the Spark Murmur3 `hash`, `format_number`, `mask`, and the `bin`/`rint` BOOLEAN refusal moved out of the Python pre-cast.

| Where | What | Why it is still Python | Next |
|---|---|---|---|
| `functions_agg_1.py::count_min_sketch` | an omitted `seed` is a Python `random.randint` baked in as a literal (pyperf-003, P3) | Spark's Python API draws the seed on the driver too. It is API plumbing, not a value decision | none |
| `functions_agg_1.py::percentile` | omits a literal frequency of 1 from the kernel call but prints `, 1` (pyperf-006, P3) | both doors default the frequency the same way. It is display plumbing | fold into the kernel's display at the next touch |
| `functions_expr.py::json_tuple` / `CANNOT_BE_EMPTY` | zero fields raises on the Python door | Spark's own Python API raises this before the JVM. The SQL door's `WRONG_NUM_ARGS` is in Rust. Both are pinned | none |
| `functions_byname.py` resolver | tries scalar dispatch, falls back on `ValueError` | API plumbing (carried from 17a) | a later by-name pass |
| `functions.py::expr` | display name from the raw fragment text | run 17c C-029 (carried) | 18c parser work |

The run-17a defects in this table (`any_value`, `product`, `rint`, `split`) are **gone**: on #625 and #628 respectively, not yet on main.

## 8. Mechanics lessons

- **A watcher that diffs against its own starting snapshot misses anything already finished when it starts.** A round had exited before my watch began, and I lost ten minutes. The replacement (`w2.sh`) keeps a handled-set file and reports anything not in it.
- **Never share a clone between a running gate and a worker.** The gate's `make py-test-facade` rebuilds a *debug* native into the same `.venv` the worker tests against. I stopped the gate unit (by unit name, not by killing cargo) before launching the remediation.
- **A tripwire another run adds is part of your merge.** #654 armed two `schema_of_csv` tripwires on main while #657 was in review. The verification critic's clone predated my fix and flagged them as a new P1. Pins that name another unit's name belong in the rebase checklist.
- **Squash once onto a restructured main.** #625's sixteen commits met #654/#655/#657. One `merge --squash`, resolved once, left a diff-of-diffs containing only the two measured ceilings.
- **Measure the claim, including your own worker's.** Four of the run's most consequential rulings came from live cells taken after a claim and before the ruling. Two of them contradicted the claimant.
- **Reuse a released build clone for the next family instead of cloning again.** #628 started in `/tmp/sa-gen3` minutes after #657 went to CI, and saved a full release build.

## 9. Carry-over

| Item | Where | Brief / state |
|---|---|---|
| **#625 FNP-AGG-1**: verification critic, whole gate, rebase onto main, merge | `feat/fnp-agg-1` at `4a3379fe`, build clone `/tmp/qa-build2` (release native built) | ledger `task/ledgers/staging/fnp-agg-1-ledger.md` holds every disposition. Critic reports: `/tmp/oc-worker/sa-ag-{logic,rustperf,pyperf}/report.md`. Gate chain: `/tmp/oc-worker/run18a/gate.sh` |
| **#628 FNP-MATH-1**: steps 8–10 (census, registry EX-FN-5/7/18 flips, gates), then `sentences`, `collate`/`collation` (Q-18a-5), AES on the approved crates, `locate(pos)`, `array_join(null_replacement)`; then reviews | `feat/fnp-math-1` at `10f18d40`, clone `/tmp/sa-gen3` (8 G) | ledger's OPEN list at the hard stop. Brief: `/tmp/oc-worker/sa-gen3/math-brief-1.md` |
| **LIT-DECIMAL-1 step 2**: the Rust literal path for `F.lit(Decimal)`, `like`/`ilike` `escapeChar` | `feat/lit-decimal-1` (Devin's step 1) | not started. **Hand-off to 18c:** the SQL door answering `decimal(39,0)` for a 39-digit literal is the parser's literal typing, next to BL-20 (#656) |
| GEN-1 P3 residuals | registry + ledger | PERF-006 default-DATE parser allocation; `schema_of_csv` option broadcast if `CsvFold` misses |
| Hand-offs to 18c | registry rows | `any_value` duplicate select-item names; the SQL alias Debug-rendering of CAST arguments (`count_min_sketch` cells); SQL `listagg ... WITHIN GROUP` grammar if the round's registration did not reach it |
