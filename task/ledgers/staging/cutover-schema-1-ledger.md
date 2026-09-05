# Unit ledger — CUTOVER-SCHEMA-1 · nullability derived the way Spark derives it

**Retires:** this ledger moves to `../completed/` when the orchestrator merges this lane.

**Unit:** CUTOVER-SCHEMA-1 · **Date:** 2026-09-04 · **Model:** muse-spark-1.3 ·
**Branch:** `fix/cutover-schema-1` · **Base:** `main` `bfef4a62`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`CUTOVER-CTAS-REQ-1`, `CUTOVER-DEDUP-SCHEMA-1`; sibling `V3-COV-8` (nullability half);
converged `DYNFLATTEN-READNULL-1`.
Round 3 (2026-09-05): FIXED `DEC-5`; filed `CAST-NULL-1`, `CAST-BOOL-DEC-1`,
`CUTOVER-NULLDEPTH-1`, `READ-TSNTZ-DTYPE-1`; corrected `BL-1`, `V3-COV-8`,
`DYNFLATTEN-READNULL-1` prose.
**Ruling:** owner 2026-09-04 "get this matched up with Spark".

**Rubric:** STANDARD. `risk_tier: standard`.

**Writable paths:** `crates/repark-core/src/` (reader), `crates/repark-functions/src/`
(analyzer), `crates/repark-spark/src/ctas.rs`, `crates/repark-sql/src/create_table.rs`,
`crates/repark-python/src/` (export boundary), `crates/repark-sql/tests/`,
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, lockstep `map.md` files,
this ledger. Closed: `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, AWS.

## 1. Scope, as checkable propositions

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `read.parquet` of a required-field file reports every field nullable, recursive over struct/list/map down to `MAX_NESTED_TYPE_DEPTH = 32` (non-null from level 34; residual `CUTOVER-NULLDEPTH-1`); csv/json likewise; Spark-equal on the pinned shapes. | `test_cutover_schema_1.py` read pins + live legs | **PROVEN** |
| C-002 | CTAS stores every derived column optional on both doors, including `SELECT coalesce(x, 0)`; the SE-1 tighten-derived refusal still fires on its case. | `test_cutover_schema_1.py` CTAS pins + SE-1 control; existing tighten pins green | **PROVEN** |
| C-003 | Analyzer nullability after `coalesce` + `cast` is Spark's on the pinned shapes: a decimal-target cast is nullable iff it can overflow or lose digits (`decimal_cast_can_overflow`); non-decimal-target divergences are filed as `CAST-NULL-1`, not fixed here; the dedup Arrow schema equals Spark field for field. | `test_cutover_schema_1.py` cast + dedup pins + live legs | **PROVEN** |
| C-004 | `to_arrow`/`collect` present Utf8 for Utf8View (Binary for BinaryView) at the export boundary only — a per-batch Arrow `cast` (a copy); `coerce_batch_views` casts any analyzed-vs-physical mismatch under safe options (new coercion surface: lossless widening casts, uncastable mismatches refuse loud); written row sets and `next-row-id` unchanged. | `test_cutover_schema_1.py` + flipped goldens (rows/ids byte-identical) + the two `arrow_export.rs` coercion pins | **PROVEN** |
| C-005 | Registry rows flipped with date + unit id; verdict tables moved; V3-COV-8 width half stays BACKLOG; ledger + maps lockstep. | flipped rows + `test_sql_harden_*` green + maps | **PROVEN** |
| C-006 | Red-first battery red on base, green after; revert-reader and revert-analyzer mutations red the named subsets; gates green. | mutation table §6 + gate table §7 | **PROVEN** |

`LOGIC_SCORE` = **6/6 `PROVEN**`. Live-leg evidence: green in round 1 (12 passed) on code
byte-identical to this HEAD for every file the trio exercises (verified by
`git diff 7e6fc8c..HEAD` — round 2 touched only pins, registry, matrix, maps, ledger);
the round-2 re-run was not observed because sibling lanes held every JVM for 75+ min
(see §7). Round 3 re-ran the trio on the tip: 185 passed, 0 failed (see §7).

## 2. Oracle table

| Engine | Pin |
|---|---|
| live PySpark 4.1.2 + Iceberg 1.11.0 | banner `version 4.1.2, tz UTC, ansi true`; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, jar `/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, catalog `m1` |
| repark 1.0.1 (`bfef4a62` + this unit) | memory catalog `ice`, facade session |

Measured 2026-09-04. Single-file bronze parquet (the `_BRONZE_SCHEMA` shape). Live-cell
rules: shared session where the harness owns one, module-private catalogs, single JVM.
Round-3 re-measurement 2026-09-05 on the same oracle (banner `version 4.1.2, tz UTC,
ansi true`; every cast cell under both ANSI modes); verbatim output in
`/tmp/cutsch3_measure_spark.out` (oracle) and `/tmp/cutsch3_measure_repark.out`
(repark, two identical runs).

## 3. The measured rules (verbatim oracle output in `/tmp/cutsch1_measure*.out`; round 3
in `/tmp/cutsch3_measure_spark.out` and `/tmp/cutsch3_measure_repark.out`)

| # | Rule | Oracle evidence |
|---|---|---|
| R-1 | `read.parquet` marks every column nullable, including nested struct children, list elements, map values. | `read_flat` all `true`; `read_nested_json` inner `true`; list `containsNull true`, map `valueContainsNull true` |
| R-2 | csv/json reads mark every column nullable. | `read_csv` / `read_json` all `true` |
| R-3 | CTAS stores every column optional, even provably non-null outputs (`coalesce`, literals). | `ctas_coalesce_required` all `false`; `ctas_literal_required` `int`/`string` `false` |
| R-4 | A `CAST` to a decimal target is nullable iff the cast can overflow or lose digits (round-3 rewrite — the round-2 "uniformly nullable" was false); identical ANSI on/off. Non-decimal targets: four measured divergences, filed as `CAST-NULL-1`. | `int_to_dec10_0` F, `dec10_2_to_dec10_0` F, `bigint_to_dec38_18` F, `bigint_to_dec19_0` T, `int_to_dec10_4` T, `double_to_dec10_2` T, `tinyint_to_dec10_0` F, `smallint_to_dec4_0` T, `string_to_dec10_2` T, `dec10_4_to_dec12_4` F — both engines, both ANSI modes, values agree throughout |
| R-5 | `coalesce` is non-null when any argument is non-null. | `coalesce(a=false, b=true, c=true)`; DataFusion already implements this rule |
| R-6 | Arrow export says `string`, never `string_view`. | `read_flat_arrow` `string` |

R-5 needs no code change (verified in DataFusion 54.1 `coalesce.rs`:
`nullable = args.all(nullable)`). R-3 forces a create-path relax beyond the reader fix.
R-4 scopes the analyzer rule to decimal targets only; the rule's nullability test is
`decimal_cast_can_overflow` in `crates/repark-functions/src/decimal_cast.rs` (small ints
under their digit bound and same-or-wider decimal targets stay non-null, everything
overflow-exposed is nullable). Non-decimal-target cast nullability (STRING→INT/DATE/
TIMESTAMP nullable on Spark, DATE→TIMESTAMP non-null) is the pre-existing `CAST-NULL-1`,
out of this unit's scope.

## 4. Baseline (pre-fix, `bfef4a62`)

| Suite | Result |
|---|---|
| facade `python/repark/tests -q` | 4601 passed, 191 skipped, EXIT 0 (333 s) |
| parity `python/repark-parity/tests -q` | 572 passed, EXIT 0 |
| `make develop` + import path | `repark.__file__` under `/tmp/oc-cutsch` |

The brief's `--timeout 600` flag is not installed in this lane venv; the repo-canonical
`pytest -q` form (identical to `make py-test-facade` / `parity-live.yml`) was used instead.

## 5. Blast-radius classification

Full facade suite after the fix (`python/repark/tests -q`, `/tmp/cutsch1_facade.log`):
3 failed, 4607 passed, 194 skipped (553 s). Baseline was 4601 passed, 191 skipped; the
delta is exactly +9 new cutover pins passing, +3 live legs skipping, −3 old pins failing.
Every red pin is class (a); there are no class (b) regressions. The parity suite
(`python/repark-parity/tests`), baselined green in §4 and not re-run in round 2, red 2
tests on this unit's tree — found by CI, fixed by the orchestrator in `3037dc97`.
True blast radius across the full-suite runs: **5 reds** (facade 3 + parity 2), all
class (a) / truth-up mirrors. The 2 Rust pins (found by `make verify`) and the live
dynflatten pin (standalone measurement) are the same class; the G2/G13 corpus cells
arrived flipped (uncommitted work found at round-2 pickup).

| Pin | Class | Disposition |
|---|---|---|
| `test_ctas_decimal_type_preserved[ctas_add_money_preserves_decimal128]` + `[ctas_mul_money_qty_preserves_decimal128]`, write-back half (line 821) | (a) | The SELECT half still reads non-null `(11,2)`/`(21,2)`, equal to the recorded Spark SELECT oracle — the analyzer rule correctly leaves null-safe decimal→decimal widenings alone. Only the post-CTAS read moved (nullable), which is Spark's CTAS-optional derivation. The rows split the post-write shape into `expected_written`; cite `CUTOVER-CTAS-REQ-1`. |
| `test_v3_statement_row_reproduces_the_measured_repark_answer[ctas-v3]` | (a) | repark half re-measured required → optional; verdict stays DIVERGES on width (`long` vs `int`). Cite the `V3-COV-8` nullability half. |
| G2 `int_times_decimal_promotes_wider_in_repark` + the three G13 nullability cells (the uncommitted flip found at pickup) | (a) | Kept: Spark-answer flips caused by the analyzer rule. repark halves move non-null → equality with the already-nullable recorded Spark halves (overflow-exposed operand casts propagate through the op). Cite `DEC-9`, narrowed; DEC-9 proper stays BACKLOG. The G2 cell is registry `DEC-5`, flipped FIXED in round 3 (both faces Spark-equal 2026-09-05). |
| `test_live_dynflatten_matches_spark_explode` (live; skipped in the facade run, repark half measured standalone on all three bed shapes) | (a) | repark `id` `False` → `True`, equal to the recorded Spark `True`. Pin flipped; `DYNFLATTEN-READNULL-1` FIXED. |
| `repark-spark/src/tests/decimal.rs::pin_int_times_decimal_is_12_2_i128` + `::pin_mul_single_digit_nullability_non_null_i128` (found by `make verify`, not the facade run) | (a) | Rust-door twins of the G2/G13 flips: assert nullable now, Spark-equal. Names kept per the row-name precedent. |
| `test_cap_1_exception_tables_equal_the_measured_debt` + `test_cited_pins_exist_and_dec9_stays_open` (parity suite; red on the lane tree, fixed by the orchestrator in `3037dc97`) | (a) | Mirror pins asserting this unit's own truth-ups: CAP-1 follows the two Rust size ratchets (`session.rs` 1040→1039, `dataframe.rs` 1171→1127); REG-1 follows DEC-9's narrowed rationale. Reproduced in round 3 by checking out the pre-fix pins: 2 failed, 570 passed. |

Held green without flips: every `printSchema` / `DESCRIBE` / `dtypes` / `StructType` pin,
the SE-1 tighten pins (the refusal still fires on its case), all other `V3-COV-*` rows,
the `COUNT(*)` nullability cell (`CUTOVER-DATE-1` notes the convergence). `G6-4` /
`G12-1` / `G12-2` pins pass unchanged — this unit's rules do not touch those cells
(decimal targets and null-safe equal are out of scope); each row carries a dated
re-measured note. The `V3-COV-8` width half stays BACKLOG. Round 3 re-ran the parity
suite (`python/repark-parity/tests -q`, now a gate): green on the tip (see §7); its
post-fix reds are classified in the table above.

## 6. Mutation table

Red-first battery on `bfef4a62` (`test_cutover_schema_1.py`): 6 red, 3 controls
green, 3 live skipped. Red: read flat (`string_view`/`False`), read nested,
CTAS-star (`required True`), CTAS-coalesce (`long True`), dedup, decimal cast.
Green controls: csv/json already nullable, ANSI cast fence, SE-1 refusal.
The coalesce CTAS pins repark's `long` width (V3-COV-8 width half stays BACKLOG).

| Knob | Measured reds | Result |
|---|---|---|
| revert reader rule (`session.rs`: `read_parquet_nullable` → `ParquetReadOptions::default()` + rebuild) | `test_read_parquet_reports_every_field_nullable`, `test_read_parquet_relaxes_nested_fields`, `test_dedup_arrow_schema_matches_spark`, `s3-dedup-coalesce-cast` repark half | 4 red of 265. CTAS-star stays GREEN: the CTAS relax rule derives optional from any query schema, independent of the reader — the pre-measurement expectation named CTAS-star, and the measurement corrects it. |
| revert analyzer rule (`decimal_spark.rs`: drop the `nullable_decimal_cast` rewrite + rebuild) | `test_decimal_cast_of_non_null_is_nullable`, `test_dedup_arrow_schema_matches_spark`, `s3-dedup-coalesce-cast`, `int_times_decimal_promotes_wider_in_repark`, the three G13 nullability cells | 7 red of 265. Read/CTAS/CTAS-money/ANSI-fence/SE-1 stay green: the rule touches only overflow-exposed decimal casts, so every flipped pin bites exactly its rule. |

Each knob ran the 265-pin battery (166 always-run + 99 live-skipped:
`test_cutover_schema_1.py` 12 + `test_sql_harden_cutover.py` 50 +
`test_decimal128_parity.py` 38 + `test_v3_statement_coverage.py` 165) after a `make
develop` rebuild, then restored (`git checkout -- <file>`) and rebuilt; the restore run is
166 passed, 99 skipped. (Round-3 correction: §6 first recorded 181 / 163+18; the four
files collect 265 / 166+99 — re-derived by collecting the files, all 99 skips live-tier.)
`string_view` (brief item 4) SHIPPED in this unit — the Arrow export boundary coerces
Utf8View to Utf8 (boundary-only, committed in `a472272`) — so no `CUTOVER-STRINGVIEW-1`
row was filed; the dedup pins assert `string` throughout.

## 7. Gates

| Gate | Exit |
|---|---|
| `make ci` | 0 |
| `make verify` | 0 |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| facade `python/repark/tests -q` | 0 — 4614 passed, 194 skipped (634 s); round-2 run was 1, see note |
| parity `python/repark-parity/tests -q` (new gate in round 3) | 0 — 572 passed |
| live trio `REPARK_PARITY_LIVE=1 … test_parity_live.py test_cutover_schema_1.py test_sql_harden_cutover.py -q` | 0 — 185 passed (98 s) |
| `make check-map-sync` (186 maps) | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| added-comment grep | 5 lines — see note |

Facade note: the round-2 full run (`/tmp/cutsch1_facade.log`) gave 3 failed / 4607
passed / 194 skipped; all three are class (a) Spark-answer flips (§5), each flipped and
verified in targeted re-runs. Round 3 re-ran the full suite on the tip: 4614 passed,
194 skipped, 0 failed (634 s) — the first full green; the +4 collected over round 2 is
the four new current-answer pins.

Parity note: 572 passed, exit 0 — the same count as the §4 baseline. The two post-fix
reds (§5) were fixed by the orchestrator in `3037dc97` and reproduce in round 3 on the
pre-fix pins (2 failed, 570 passed).

Live-trio note: green in round 1 (12 passed) on code byte-identical to this HEAD for
every exercised file; the round-2 re-run was not observed (sibling lanes held every JVM
for 75+ min; the one-JVM rule forbids concurrent runs and their processes are protected
state). The dynflatten live pin's changed assertion was measured directly on the exact
bed instead (repark `id` True, all three shapes); its Spark half is the recorded True.
Round 3 waited out one sibling JVM for an empty box, then ran the trio on the tip: 185
passed, 0 failed (98 s); no JVM left behind.

Comment-grep note: round 2's single hit is an in-place wording edit of a pre-existing
comment (same count, density gate green), not an addition; committing the stale wording
instead would violate the repo's comment-truth rule, which outranks the lane grep on
conflict. Round 3 carries four pre-existing comment lines verbatim from
`crates/repark-python/src/dataframe.rs` to the moved `StreamingBatchReader` items in
`crates/repark-python/src/arrow_export.rs` (moved, not new — the rule is never delete a
pre-existing comment); the ban grep shows those four plus the round-2 rewording, nothing
else.

Clock-flaky note (not a unit red): `test_date_fn_1.py::
test_unix_timestamp_zero_arg_repeats_once_per_input_row` asserts two successive
`unix_timestamp()` queries agree (`len(set(sql_rows)) == 1`, `sql_rows == facade_rows`).
A second ticking over between the two queries reds it. Clock-flaky by construction;
owned by DATE-FN-1, untouched by this unit.

## 8. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: cutover-schema-1
  artifacts_verified:
    ledger: task/ledgers/staging/cutover-schema-1-ledger.md
    coverage_attestation: §5 — 3 facade + 2 Rust + 1 live-pin flips, all class (a), zero regressions
    findings_ledger: CUTOVER-CTAS-REQ-1, CUTOVER-DEDUP-SCHEMA-1, DYNFLATTEN-READNULL-1 FIXED; V3-COV-8 nullability half closed; DEC-9 narrowed; G6-4/G12-1/G12-2 re-measured unchanged
    shipped_flag_register: none — no flags
  done_gate: static gates green; live trio transfers by identity from round 1; dynflatten execution + DEC-9 residual probe ride the nightly live tier (non-blocking)
  status_update: STATUS.md untouched (PINNED); registry rows flipped
  verdict: done
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: cutover-schema-1
  flags: []
  count: 0
```

## 9. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cutover-schema-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Six charter clauses each pinned; red-first battery 6 red on base, green after; mutations red exactly each rule's pins (§6).
      artifacts: [python/repark/tests/test_cutover_schema_1.py, task/ledgers/staging/cutover-schema-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Flat and nested struct/list/map relax; 600-deep nesting completes under the depth cap; parquet/csv/json; overflow-exposed vs null-safe cast boundary pinned both ways.
      artifacts: [crates/repark-core/src/spark_nullable.rs, python/repark/tests/test_cutover_schema_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: SE-1 tighten-derived refusal still fires on its case; ANSI-door cast fence unchanged; no new failure paths (pure schema derivation).
      artifacts: [python/repark/tests/test_cutover_schema_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, locks, or spawned tasks added; the two parquet opens are sequential awaits in one call; relax is a pure function over the schema.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, secrets, .github, or dependency change; the read path takes the same caller-supplied path string with unchanged validation.
    - id: AT-6
      status: ATTACKED
      evidence: CTAS/MERGE goldens keep byte-identical rows and next-row-id; the schema change is the charter, blast radius classified with zero regressions (§5).
      artifacts: [python/repark/tests/test_sql_harden_cutover.py, task/ledgers/staging/cutover-schema-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: The second parquet open reads footer metadata only; data is scanned once; no unbounded growth, N+1, or hot loop introduced.
    - id: AT-8
      status: ATTACKED
      evidence: Spark read/coalesce/cast nullability rules measured on the live oracle before implementing (R-1..R-6); DataFusion coalesce rule verified in its source; no dependency change.
      artifacts: [python/repark/tests/test_cutover_schema_1.py]
    - id: AT-9
      status: N/A
      justification: Pure derivation change with no new failure mode; mismatches surface as schema pins that name the cell.
    - id: AT-10
      status: ATTACKED
      evidence: Revert-reader reds 4 pins, revert-analyzer reds 7 (§6); every flipped pin bites its rule; every relax arm and both overflow-exposure outcomes have pins that change output.
      artifacts: [python/repark/tests/test_cutover_schema_1.py, crates/repark-spark/src/tests/decimal.rs]
```

## 10. Round-3 review-gap block (2026-09-05) — the critic FAILED the ledger, not the engine

The Opus critic re-measured everything and confirmed the engine: all decimal-target cast
cells Spark-exact, reads/CTAS/dedup Spark-equal, writers unchanged, the live trio green,
both mutation red sets reproducing. What failed is what the ledger and registry SAID.
Every finding below is closed in this round; §§1–9 above carry the corrections (§8/§9
stay the round-2 record, this block the round-3 delta).

| # | Severity | Finding | Disposition |
|---|---|---|---|
| F-1 | S1 | R-4 false as stated: decimal targets are not uniformly nullable, and non-decimal targets do not uniformly keep the child. | R-4 rewritten to nullable-iff-overflow-exposed (§3, citing `decimal_cast_can_overflow`); C-001/C-003 corrected alongside. The four non-decimal cells are one new §7 row `CAST-NULL-1` (BACKLOG, measured both engines + both ANSI modes, current-answer pin); BL-1's "ONE remaining divergence is G6-4" now names CAST-NULL-1 too. Engine untouched by design. |
| F-2 | S1 | Unclassified reds: the parity suite's post-fix result was never recorded (CI found CAP-1 + REG-1 reds, orchestrator fixed in `3037dc97`). | §5 records the post-fix result with the true blast-radius count (5 reds: facade 3 + parity 2, all class (a) / truth-up mirrors; parity reds reproduced in round 3: 2 failed, 570 passed on the pre-fix pins). `python/repark-parity/tests -q` is a gate (§7). |
| F-3 | S2 | `DEC-5` agrees with Spark on both faces but still reads BACKLOG. | Row flipped FIXED (2026-09-05, this unit); repark line and Pin bullet rewritten (both flipped pins assert Spark's answer); cited in §5. |
| F-4 | S2 | `DYNFLATTEN-READNULL-1` Pin bullet still says "(asserts repark False and Spark True…)". | Prose fixed to True/True (verified against the pin body). |
| F-5 | S2 | Nested depth bound unstated: relax stops at `MAX_NESTED_TYPE_DEPTH = 32` (non-null from depth 34 on a 40-deep struct; Spark relaxes every level). | Bound stated in C-001; residual filed as §7 row `CUTOVER-NULLDEPTH-1` (BACKLOG, measured both engines; pins = existing Rust `deep_nesting_completes_with_nullable_flags` + new facade depth-40 pin). |
| F-6 | S2 | Dead arm: `DataType::Boolean => target_integer_digits < 1` is unobservable — every `BOOLEAN → DECIMAL` cast refuses downstream on both doors. | Arm removed (one line; refusal behavior unchanged); refusal filed as §7 row `CAST-BOOL-DEC-1` (BACKLOG, measured both engines + both ANSI modes, refusal pin on both doors). |
| F-7 | S2 | Four pre-existing comments dropped in the `StreamingBatchReader` move. | All four carried over verbatim to `arrow_export.rs` (moved, not new); recorded in the §7 comment note. |
| F-8 | S2 | Tz-naive `TIMESTAMP` reads `string` via `dtypes`/`schema` (facade-side, pre-existing; Rust mapper right, Arrow right, Spark says `timestamp_ntz`). | Filed as §7 row `READ-TSNTZ-DTYPE-1` (BACKLOG, measured both engines, current-answer pin). Not fixed here by design. |
| F-9 | S3 | §6 battery size 181 wrong (four files collect 265); C-004/`CUTOVER-DEDUP-SCHEMA-1` silent on the per-batch-copy coercion surface; `V3-COV-8` heading still says "required"; clock-flaky `unix_timestamp` test unnoted. | §6 records 265 (166+99, same red sets); C-004 and the DEDUP row state the per-batch `cast` copy + safe-options coercion surface with the two new Rust coercion pins; `V3-COV-8` keeps its anchor with a first-line nullability-half note; the flaky test is noted in §7 as not a unit red. |

Round-3 measurement basis: live PySpark 4.1.2 + repark on this lane, every cast cell
under both ANSI modes, verbatim output in `/tmp/cutsch3_measure_spark.out` and
`/tmp/cutsch3_measure_repark.out` (repark run twice, byte-identical).
