# Overnight report: run 17a, the functions

**Date:** 2026-09-15 19:56 → 2026-09-16 07:15 EDT
**Orchestrator:** Claude (claude-opus-5), one of three run-17 orchestrators. 17b owns DataFrame, Column, Session, Catalog and types; 17c owns the SQL door and the parser/planner.
**Charter:** the 1.5 row of the release roadmap — every `pyspark.sql.functions` name answers PySpark 4.1.2 on both doors, or is a dated declared refusal. Actor tier a **mix of Devin SWE-2 and Muse Spark 1.3 contributor** (owner, 2026-09-15 evening); reviewers on Grok 4.6.

## 1. Census slice (functions), before and after

Measured by import against the built native, not from `__all__` (`__all__` understates the surface by ~130 names because the facade re-exports past it). PySpark 4.1.2 `functions`: **508 real names** after dropping the 11 typing re-exports from the 519 in `/tmp/oc-worker/run15/pyspark-surface.json`.

| Point | Missing names |
|---|---|
| Run start (main `0355ef5e`, before #618 merged) | **37** |
| After #618 FNP-WIN-1 merged (`a09f11cf`) | **34** |
| After #627 FNP-11B merged (`5dc62a2e`) | **22** |

**Twelve names closed tonight:** `current_time`, `make_time`, `time_diff`, `time_trunc`, `to_binary`, `to_char`, `to_number`, `to_varchar`, `to_timestamp_ltz`, `to_timestamp_ntz`, `typeof`, and `make_timestamp`'s full PySpark signature — plus `window`, `window_time` and `session_window` from the carried-over #618.

**The 22 that remain, with their owning unit:**

| Unit | Names left |
|---|---|
| FNP-AGG-1 #625 (12) | `any_value` `count_min_sketch` `grouping_id` `histogram_numeric` `listagg_distinct` `max_by` `min_by` `percentile` `product` `string_agg_distinct` `sumDistinct` `sum_distinct` |
| FNP-MATH-1 #628 (8) | `aes_decrypt` `aes_encrypt` `bround` `collate` `collation` `conv` `mask` `try_aes_decrypt` |
| FNP-GEN-1 #629 (2) | `inline` `inline_outer` |

Baseline files: `/tmp/oc-worker/run17a/census-before.json`, `census-after-627.json`.

## 2. Per-PR table

| PR | Unit | Scope | Actor tier & rounds | Reviews (verdict) | State |
|---|---|---|---|---|---|
| **#618** | FNP-WIN-1 | `window`, `window_time`, `session_window` | **Muse** ×9 (run 16a); run 17a rebased and re-gated | run-16a critic ×2 + Rust/Python perf | **merged `a09f11cf`** 21:26, tree-equal |
| **#627** | FNP-11B | datetime formats, the TIME family, the `to_char` family, `typeof`, BL-13, the `make_timestamp` widening | **Muse** ×5 (steps 4–7 + one remediation), resumed in one session | critic-logic 2 P1 / Rust perf 3 P1 / Python perf 3 P2; **verification critic: all CLOSED, no new findings** | **merged `5dc62a2e`** 02:46, tree-equal |
| **#629** | FNP-GEN-1 | `posexplode`, `posexplode_outer`, `inline`, `inline_outer`; `from_xml` / `schema_of_xml` declared | **Devin SWE-2** ×3 (one round lost to a turn-stall) | critic-logic 4 P1 / Python perf 1 P1 / Rust perf 2 P2; **verification critic: all CLOSED**, 2 new P2 (one raised to P1 by me and fixed) | gated, in the merge chain |
| **#625** | FNP-AGG-1 | `any_value`, `max_by`, `min_by`, `product` + `kurtosis` / `skewness` / `mode` destubs | **Muse** ×1 (a single long round) | critic-logic **no P1**, Rust perf **no P1**, Python perf 2 P1 | **handed over green-with-findings** — see §6 |
| — | LIT-DECIMAL-1 | `F.lit(Decimal)` typing, `like`/`ilike` `escapeChar` | **Devin SWE-2** ×1 (step 1) | — (pins only) | step 1 committed, step 2 carried over |

## 3. Devin SWE-2 vs Muse Spark 1.3 contributor — the comparison the owner asked for

| | Devin SWE-2 | Muse Spark 1.3 contributor |
|---|---|---|
| Rounds launched | 4 | 7 |
| Rounds that produced a commit | 3 | 7 |
| **Rounds lost** | **1** (turn-stall, no commit, no hand-back) | **0** |
| Units carried | LIT-DECIMAL-1 step 1, FNP-GEN-1 steps 2 + remediation | FNP-11B steps 4–7 + remediation, FNP-AGG-1 step 2 |
| Metered cost | **free tier** | **no metered cost** (contributor model) |

**How each behaved, which matters more than the counts.**

*Devin* reads exhaustively before it writes. Its FNP-GEN-1 round spent **53 minutes reading with zero file edits** before the first byte of code — checking `unnest_columns_with_options`, `Case`, `arrays_zip`, nullability propagation, the analyzer-rule wiring — and then produced a correct analyzer rewrite in one pass. That is not waste: the design it derived unaided (an analyzer rule over a `Projection`, avoiding the forbidden `dataframe/**` seam) is the one I adopted as the sanctioned design. But it has a real failure mode: **round 1 ended a turn to narrate progress**, with no commit and no `handback.json`, burning 28 minutes and ~1 M tokens. The fix was a resume with an explicit proceed mandate; after that it ran clean for two rounds. **Give Devin the well-bounded card and always brief the proceed mandate.** Its hardest round (the five-P1 remediation, including replacing a `coalesce(expr, zero)` data-corruption hack with a validity-union UDF) came back with every finding closed and all gates green.

*Muse* starts writing sooner and sustains long sessions. One resumed session carried FNP-11B through **four steps and a six-P1 remediation** without a stall, which is the behaviour the resume-never-restart rule is meant to protect. It is also the tier that absorbed the most orchestrator rulings per round and applied them correctly, including one where it had leaned the other way. Its weakness is over-confident reporting: it called a CAP-1 gate failure "pre-existing rebase fallout" when **its own earlier step had caused it**, and it claimed a stale ledger row was "red on main too" when main was clean. Both took one `git` command to disprove. **Check a Muse round's claims about anything outside its own diff.**

*Neither tier produced a comment in a source file* — zero added comment lines across every commit of all four units.

## 4. Oracles recorded (live PySpark 4.1.2, this box)

| Fixture | Cells | What it settled |
|---|---|---|
| `lit_decimal_1_spark_oracle.json` (LIT-*, LIKE-*) | 46 | Spark's decimal-literal typing (`Decimal("1.00")` → `decimal(3,2)`, `1E+10` → `decimal(11,0)`, over-38 → `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`, non-finite → `NumberFormatException`), decimal arithmetic result types, and `like`/`ilike`'s `escapeChar` including `INVALID_ESCAPE_CHAR` for a length ≠ 1 escape |
| `fnp11b_typeof_spark_oracle.json` (TYPEOF-*) | 40 | `typeof` across the type table: `NULL` → `void`, `array()` → `array<void>`, `INTERVAL 1 DAY` → `interval day`, `1E10` → `double`; wrong arity → `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` |
| `unresolved_column_spark_oracle.json` (UC-*) | 10 | `UNRESOLVED_COLUMN.WITH_SUGGESTION` — and that Spark's `proposal` list is **ranked by edit distance, not schema order**, with qualified misses proposing qualified names |
| `fnp11b_hour_time_spark_oracle.json` (HT-*) | 11 | `hour` / `minute` / `second` **refuse** a TIME input on both doors; `lit(datetime.time(...))` is `time(6)` |
| `fnp_agg_1_mode_tie_spark_oracle.json` (MODE-*) | 7 | `mode(v, true)` answers the **smallest** on a frequency tie, on both doors; the `DESC` in its display name is cosmetic |

**Oracle results that overturned a claim — in both directions:**

- A **pin already in the repository asserted behaviour Spark does not have.** `test_hour_minute_second_on_time` asserted `hour`/`minute`/`second` over `lit(datetime.time(12,34,56))` returning 12/34/56, with a docstring naming it a port of an Apache test. PySpark 4.1.2 raises `UNSUPPORTED_TIME_TYPE` for all three on both doors. A pin inherited from upstream is not evidence about the Spark we target.
- A **critic's P2 conjecture was refuted**: `mode(v, true)` on a tie returns the smallest, which is what the code already did. The critic was right to flag the missing cell and right to label the claim a conjecture.
- A **worker's HALT lean was overruled by a measurement it had not taken**: the `UNRESOLVED_COLUMN` suggestion ranking (§5, R-17a-22).
- A **second divergence fell out of a probe run for something else**: repark types `lit(datetime.time(...))` as `string` where Spark says `time(6)` — the type table's, not a function kernel's.

## 5. Rulings taken (the ones that changed an outcome)

Full list with reasoning in the run notes; these are the ones worth the owner's attention.

1. **R-17a-22 — deferred an engine-wide error taxonomy, against the worker's lean, on evidence.** Five `make_timestamp` cells want `UNRESOLVED_COLUMN.WITH_SUGGESTION`; RePark raises a raw DataFusion `Schema` error, pinned as-is by **seven** pins across five files, three of them run 17c's. The worker leaned toward building a narrow central rule. I measured first: Spark **ranks its suggestion list by edit distance**, so a rule that listed schema columns in order would get three of seven cells wrong. "Narrow" was not available. Filed as carry-over **ERR-UNRESOLVED-COL-1** with its oracle.
2. **R-17a-16 — accepted a divergence rather than relax a correct pin.** One Arrow `MonthDayNano` carries both ANSI `YEAR TO MONTH` and `CalendarInterval`, so one renderer cannot spell both. Relaxing EX-FN-19 would have traded a correct pin for an incorrect one; the fix is a separate interval type in the type table.
3. **Strict xfail, not a red, for every ruled residual.** Thirty-one pins across #627 and #629 became `xfail(strict=True)` naming their ruling, registry row and owning slice. Strict is the point: when the owning slice lands its fix the pin **XPASSes and fails**, so the residual is retired rather than forgotten. A merged PR cannot carry failing tests.
4. **R-17a-4 (rules Q-16a-1)** — `posexplode` / `posexplode_outer` rename the frozen parameter `column` → `col` to match PySpark, as a freeze update, precedent Q-15a-3.
5. **Raised a P2 to P1 and fixed it myself:** the FNP-GEN-1 verification critic's V-002, an `ordinality` kernel that cloned the input's offset buffer and **panicked on a sliced `ListArray`**. A panic is not a P2 in a repository with a panic-ban gate, and slices are ordinary after a limit or a take even though no oracle cell produces one.
6. **Raised L-004 (P2 as filed) to P1:** `TimeCastGuard` missing from `analyzer_rules()` made `F.expr` plan a CAST-to-TIME that `session.sql` refused. A dual-door split is a shape-rule violation whatever its severity as a defect.

## 6. Owner questions (with recommendations)

- **Q-17a-1 — #625 FNP-AGG-1 is handed over green-with-findings, and its two P1s are the campaign's recurring defect.** Seven aggregate names answer with oracle-exact pins, `make verify` is 0, the whole parity suite is green, and **critic-logic and the Rust perf read each found no P1** — the two-partition merge test on every UDAF held under attack, which is the class that cost FNP-11B a remediation round the same night. The Python perf read found two P1s, both door splits: `F.any_value` binds DataFusion's `first_value` and folds `ignoreNulls` **in Python** while the SQL door runs `SparkAnyValue` (two kernels behind one name, so an ungrouped select and a `groupBy().agg` take different paths), and `F.product`'s `sql_expr` leaks the internal `__repark_product` name. Neither is a measured value miss on the recorded frame; both are the shape-rule breach the campaign keeps finding. *Recommendation:* land the remediation as the first item of the next functions run — the fix is dispatch arms onto `any_value_udaf()` passing the `ignoreNulls` Column through instead of folding it, and a corrected `join_sql_expr`. The brief is written; the branch is pushed and rebased.
- **Q-17a-2 — four of the nine P1s this run were "a decision taken in Python that the SQL door never makes."** `F.any_value` and `F.product` (#625), the sticky-aggregate check and `_GeneratorColumn`'s missing `_generator` flag (#629), plus FNP-11B's `TimeCastGuard`. The Rust-first rule already forbids this, but it is stated as an *implementation* rule ("kernels land in Rust") and the workers read it that way — a Python `if` that raises is not obviously a "kernel". *Recommendation:* add one sentence to the standing instruction: **"any branch that decides behaviour — a raise, a coercion, a flag fold, a name choice — belongs where both doors reach it; the facade may only bind names, argument order and defaults."** I briefed it as the thread of the #629 remediation and it closed cleanly; making it standing would have prevented the four.
- **Q-17a-3 — the per-clone size stop condition needs a rule, not vigilance.** `/tmp/pa-build` hit **80 G** (past the 60 G stop) purely because `make verify`, `cargo test` and `make py-test-facade` leave **76 G of `target/debug` against 2.9 G of release** — and the gate runs all three. Dropping `target/debug` after the gate pushes took the clone to 4.5 G and gave back 60 G, twice, with no cost but a debug rebuild. *Recommendation:* make it a line in the runbook — *after a unit's gate has pushed, drop `target/debug`; never `target/release`, never while a build is running in that clone*.
- **Q-17a-4 — my gate was a subset of CI, and CI caught it.** #627's first merge attempt died on CI's Python job while my local gate was green: I ran three hand-picked files from `python/repark-parity/tests` where CI runs the whole suite. The runbook already says to run the whole parity suite; the gate scripts inherited from run 16a do not. *Recommendation:* fix the inherited `chain*.sh` template rather than each copy — I fixed mine, but 17b and 17c inherited the same shape.

## 7. Rust-first roll-call — Python-only logic left in Python, with the reason

Everything **added** tonight is Rust behind a thin wrapper: `java_datetime.rs`, `timestamp_ltz_ntz.rs`, `time_family.rs`, `interval_avg.rs`, `try_invert/strict.rs` (#627); `generator.rs` with `GeneratorRewrite`, `__repark_gen_ordinality` and `__repark_gen_field` (#629); `any_value.rs`, `max_min_by.rs`, `moments.rs`, `mode.rs`, `product.rs` (#625).

Python-only logic still in the functions slice:

| Where | What | Why it is still Python | Next |
|---|---|---|---|
| `functions_agg_1.py::any_value` | folds `ignoreNulls` in Python and binds `first_value` | **not justified — a defect** (Q-17a-1, PYPERF-001) | first item of the next run |
| `functions_agg_1.py::product` | `sql_expr` leaks the internal name | **not justified — a defect** (PYPERF-002) | same |
| `functions_math.py::rint` | casts to DOUBLE in Python, so BOOLEAN fail-opens | carried from run 16a; the `degrees` shape | FNP-MATH-1 D-9 (#628) |
| `functions_expr.py::split` | `UnsupportedOperationException` stub | the `spark_split` kernel is on main; binding it is FNP-MATH-1 D-8 | #628 |
| `functions.py::expr` | display name from the raw fragment text | run 17c's C-029, a strict-xfail pin | after 17c's parser work |
| `functions_byname.py` resolver | tries scalar dispatch, falls back on `ValueError` | API plumbing, not a kernel | a later by-name pass |
| `functions_generators.py::_GeneratorColumn` | alias marker packing | facade machinery; every *behaviour* decision moved to the Rust rule in the remediation | none |

The honest summary: **the Rust-first rule held for everything new, and the two places it did not are the two P1s I am handing over.**

## 8. Mechanics lessons

- **A faithful replay of a stale fact is still a stale fact.** The diff-of-diffs audit proves a rebase did not change the branch's delta — it cannot see that main has since *moved the file a row points at*. A union-resolved `map.md` passed the replay audit and still left a ledger link to a path main had emptied. The gate that catches that class is `check_map_md` / the ledger link check on the merged tree.
- **A union conflict resolver is a convenience, never a proof.** It duplicated a whole ledger block whose two sides differed by one path segment, and `make check-map-sync` passed anyway. I made the lane-switch script compute the diff-of-diffs itself and refuse to launch a worker on a non-empty one.
- **Check the run's `exit` file, not the commit log, before touching a lane clone.** I edited a clone after seeing a commit appear, while its worker was still running. Nothing was lost; it could have cost the round.
- **A gate that is a subset of CI is a rehearsal.** Three hand-picked parity files instead of the suite let a census pin through to CI.
- **Retiring a pin is part of implementing the thing it pinned** — including a pin that was *already wrong*. Two of tonight's cross-unit reds were pins asserting pre-4.1.2 behaviour.
- **Squash onto main once when the base restructured.** #627 and #629 each met a main that had moved analyzer-rule registration; replaying eleven and six commits would have re-resolved the same hunk at every step. One `git merge --squash`, resolved once, with every `Authored-By` kept.
- **Size ceilings: compact before you except.** A round added a `check_lib_py` EXCEPTIONS row for a **gate script** that crossed the default by two lines; reflowing one docstring paragraph put it back at exactly 1000.
- **The 60 G clone stop condition is a debug-profile problem**, not a repo-size problem — see Q-17a-3.
- **Verify a worker's claims about anything outside its own diff.** Two claims this run ("pre-existing", "red on main too") were both false and both took one command to check.

## 9. Carry-over

| Item | Where | Brief |
|---|---|---|
| **#625 FNP-AGG-1: the two door-split P1s**, then steps 3–5 (`count_min_sketch`, `grouping_id`, `histogram_numeric`, `listagg_distinct`, `string_agg_distinct`, `percentile`, `sum_distinct`/`sumDistinct`) | `feat/fnp-agg-1`, pushed and rebased | `/tmp/oc-worker/ag-pyperf/report.md` + the ledger's critic section |
| **ERR-UNRESOLVED-COL-1** (new unit) — `UNRESOLVED_COLUMN.WITH_SUGGESTION` engine-wide, both doors, all plan nodes, with Spark's edit-distance suggestion ranking | shared error mapper | seeded by `/tmp/oc-worker/run17a/unresolved_column_spark_oracle.json` (10 cells); rewrites seven pins across five files, three of them run 17c's |
| **LIT-DECIMAL-1 step 2** — the Rust literal path and `like`/`ilike` `escapeChar` | `feat/lit-decimal-1`, step 1 committed | 46-cell oracle in the branch; step-1 ledger names the seam. **Found by step 1:** the SQL door silently answers `decimal(39,0)` for a 39-digit literal where Spark refuses |
| **FNP-MATH-1 #628 steps 2+** — `bround` `conv` `hash` `format_number`, the `split` facade half, BL-6, then AES on the approved RustCrypto crates | `feat/fnp-math-1` | `/tmp/oc-worker/ra-math2/brief-2.tmpl.md`, already carrying the R-17a-6 scope strike |
| **FNP-GEN-1 steps 3–4** — `json_tuple`, `from_csv`, `schema_of_csv` | #629 | 13 strict xfails go XPASS when the kernels land |
| **GEN-ALIAS-1** (registry BACKLOG) — the SQL door drops a user `AS` whose text looks like the call | `generator.rs::peel_generator` | needs a non-textual marker on restored aliases; the seam is the name-restoration path |
| Hand-offs to run 17c | BL-14 (ten cells), the INTERVAL DAY dialect (two `try_avg` cells), the duplicate-select-item-name planner limit (two `any_value` cells) | all registry rows name the seam |
| Hand-off to run 17b | `lit(datetime.time(...))` types `string` where Spark says `time(6)`; the ANSI `YEAR TO MONTH` vs `CalendarInterval` type split | pinned so each goes red when fixed |
