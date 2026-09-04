# Unit ledger — CUTOVER-SCHEMA-1 · nullability derived the way Spark derives it

**Retires:** this ledger moves to `../completed/` when the orchestrator merges this lane.

**Unit:** CUTOVER-SCHEMA-1 · **Date:** 2026-09-04 · **Model:** muse-spark-1.3 ·
**Branch:** `fix/cutover-schema-1` · **Base:** `main` `bfef4a62`
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`CUTOVER-CTAS-REQ-1`, `CUTOVER-DEDUP-SCHEMA-1`; sibling `V3-COV-8` (nullability half);
converged `DYNFLATTEN-READNULL-1`.
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
| C-001 | `read.parquet` of a required-field file reports every field nullable, recursive over struct/list/map; csv/json likewise; Spark-equal on the pinned shapes. | `test_cutover_schema_1.py` read pins + live legs | **OPEN** |
| C-002 | CTAS stores every derived column optional on both doors, including `SELECT coalesce(x, 0)`; the SE-1 tighten-derived refusal still fires on its case. | `test_cutover_schema_1.py` CTAS pins + SE-1 control; existing tighten pins green | **OPEN** |
| C-003 | Analyzer nullability after `coalesce` + `cast` is Spark's: decimal cast nullable, INT/STRING keep the child; the dedup Arrow schema equals Spark field for field. | `test_cutover_schema_1.py` cast + dedup pins + live legs | **OPEN** |
| C-004 | `to_arrow`/`collect` present Utf8 for Utf8View (Binary for BinaryView) at the export boundary only; written row sets and `next-row-id` unchanged. | `test_cutover_schema_1.py` + flipped goldens (rows/ids byte-identical) | **OPEN** |
| C-005 | Registry rows flipped with date + unit id; verdict tables moved; V3-COV-8 width half stays BACKLOG; ledger + maps lockstep. | flipped rows + `test_sql_harden_*` green + maps | **OPEN** |
| C-006 | Red-first battery red on base, green after; revert-reader and revert-analyzer mutations red the named subsets; gates green. | mutation table §6 + gate table §7 | **OPEN** |

`LOGIC_SCORE` = **0/6 `PROVEN`** (red-first commit; §6/§7 fill as the unit lands).

## 2. Oracle table

| Engine | Pin |
|---|---|
| live PySpark 4.1.2 + Iceberg 1.11.0 | banner `version 4.1.2, tz UTC, ansi true`; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, jar `/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, catalog `m1` |
| repark 1.0.1 (`bfef4a62` + this unit) | memory catalog `ice`, facade session |

Measured 2026-09-04. Single-file bronze parquet (the `_BRONZE_SCHEMA` shape). Live-cell
rules: shared session where the harness owns one, module-private catalogs, single JVM.

## 3. The measured rules (verbatim oracle output in `/tmp/cutsch1_measure*.out`)

| # | Rule | Oracle evidence |
|---|---|---|
| R-1 | `read.parquet` marks every column nullable, including nested struct children, list elements, map values. | `read_flat` all `true`; `read_nested_json` inner `true`; list `containsNull true`, map `valueContainsNull true` |
| R-2 | csv/json reads mark every column nullable. | `read_csv` / `read_json` all `true` |
| R-3 | CTAS stores every column optional, even provably non-null outputs (`coalesce`, literals). | `ctas_coalesce_required` all `false`; `ctas_literal_required` `int`/`string` `false` |
| R-4 | `CAST` to DECIMAL is nullable; to INT/BIGINT/STRING/DOUBLE/TIMESTAMP/DATE/BOOLEAN keeps the child; identical ANSI on/off. | `cast_literal_ansi_on` == `cast_literal_ansi_off`; `cast_nullable_col` all `true` |
| R-5 | `coalesce` is non-null when any argument is non-null. | `coalesce(a=false, b=true, c=true)`; DataFusion already implements this rule |
| R-6 | Arrow export says `string`, never `string_view`. | `read_flat_arrow` `string` |

R-5 needs no code change (verified in DataFusion 54.1 `coalesce.rs`:
`nullable = args.all(nullable)`). R-3 forces a create-path relax beyond the reader fix.
R-4 scopes the analyzer rule to decimal targets only.

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
Every red pin is class (a); there are no class (b) regressions.

| Pin | Class | Disposition |
|---|---|---|
| `test_ctas_decimal_type_preserved[ctas_add_money_preserves_decimal128]` + `[ctas_mul_money_qty_preserves_decimal128]`, write-back half (line 821) | (a) | The SELECT half still reads non-null `(11,2)`/`(21,2)`, equal to the recorded Spark SELECT oracle — the analyzer rule correctly leaves null-safe decimal→decimal widenings alone. Only the post-CTAS read moved (nullable), which is Spark's CTAS-optional derivation. The rows split the post-write shape into `expected_written`; cite `CUTOVER-CTAS-REQ-1`. |
| `test_v3_statement_row_reproduces_the_measured_repark_answer[ctas-v3]` | (a) | repark half re-measured required → optional; verdict stays DIVERGES on width (`long` vs `int`). Cite the `V3-COV-8` nullability half. |
| G2 `int_times_decimal_promotes_wider_in_repark` + the three G13 nullability cells (the uncommitted flip found at pickup) | (a) | Kept: Spark-answer flips caused by the analyzer rule. repark halves move non-null → equality with the already-nullable recorded Spark halves (overflow-exposed operand casts propagate through the op). Cite `DEC-9`, narrowed; DEC-9 proper stays BACKLOG. |
| `test_live_dynflatten_matches_spark_explode` (live; skipped in the facade run, repark half measured standalone on all three bed shapes) | (a) | repark `id` `False` → `True`, equal to the recorded Spark `True`. Pin flipped; `DYNFLATTEN-READNULL-1` FIXED. |
| `repark-spark/src/tests/decimal.rs::pin_int_times_decimal_is_12_2_i128` + `::pin_mul_single_digit_nullability_non_null_i128` (found by `make verify`, not the facade run) | (a) | Rust-door twins of the G2/G13 flips: assert nullable now, Spark-equal. Names kept per the row-name precedent. |

Held green without flips: every `printSchema` / `DESCRIBE` / `dtypes` / `StructType` pin,
the SE-1 tighten pins (the refusal still fires on its case), all other `V3-COV-*` rows,
the `COUNT(*)` nullability cell (`CUTOVER-DATE-1` notes the convergence). `G6-4` /
`G12-1` / `G12-2` pins pass unchanged — this unit's rules do not touch those cells
(decimal targets and null-safe equal are out of scope); each row carries a dated
re-measured note. The `V3-COV-8` width half stays BACKLOG. The parity suite
(`python/repark-parity/tests`) is not in this unit's gate list and was not re-run.

## 6. Mutation table

Red-first battery on `bfef4a62` (`test_cutover_schema_1.py`): 6 red, 3 controls
green, 3 live skipped. Red: read flat (`string_view`/`False`), read nested,
CTAS-star (`required True`), CTAS-coalesce (`long True`), dedup, decimal cast.
Green controls: csv/json already nullable, ANSI cast fence, SE-1 refusal.
The coalesce CTAS pins repark's `long` width (V3-COV-8 width half stays BACKLOG).

| Knob | Expected | Result |
|---|---|---|
| TBD (revert reader rule) | read + CTAS-star + dedup id/part pins red | TBD |
| TBD (revert analyzer rule) | decimal-cast + dedup amount pins red | TBD |

## 7. Gates

| Gate | Exit |
|---|---|
| TBD | TBD |

## 8. Delivery template

```yaml
DELIVERY_SIGNOFF:
  pr_unit: cutover-schema-1
  artifacts_verified:
    ledger: TBD
    coverage_attestation: TBD
    findings_ledger: TBD
    shipped_flag_register: TBD
  done_gate: TBD
  status_update: STATUS.md untouched (PINNED); registry rows flipped
  verdict: TBD
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: cutover-schema-1
  flags: []
  count: 0
```
