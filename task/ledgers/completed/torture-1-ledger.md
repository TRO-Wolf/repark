# Unit ledger — TORTURE-1 · the torture-test dataset suite (step 1)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when TORTURE-1 merges, or when the owner closes the slate row.

**Unit:** TORTURE-1 step 1 · **Date:** 2026-09-10 · **Executor:** GLM 5.3 Flash
(zai/glm-5.3-flash), Actor · **Branch:** `feat/torture-1` · **Base:** `main` `0994d539`
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.

Scope: step 1 only — the package skeleton (`generate` CLI, seed, tiers, one `Family`
protocol), the `nested` and `inference` families, the suite skeleton running both doors at
CI tier, the tiered Makefile target, maps, and this ledger. Steps 2–5 (`extreme_types`,
`smartcsv`, `temporal`, `decimal_overflow`, `secrets`, `v3_dv`, the read-option flag,
`docs/perf/`, `docs/testing.md`) are untouched.

Wiring ruling recorded here (card-consequence, not a new decision): `python/repark-parity/
pyproject.toml` is edit-forbidden for this unit, so the hatch package cannot grow a new
top-level package, and the card still pins home `fixtures/torture/`, module path
`repark_parity.torture`, and the CLI spelling `python -m repark_parity.torture`. The
mechanism that satisfies all three is a checkout-only graft in
`python/repark-parity/src/repark_parity/__init__.py`: the package `__path__` gains
`../../fixtures/torture` when that sibling directory exists. A wheel build never ships
`fixtures/`, so the graft is a no-op there; the CLI and both import paths work with the
plain `PYTHONPATH=python/repark-parity/src` (or the .venv editable path) every existing
target already uses.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The package skeleton exists at `python/repark-parity/fixtures/torture/`, importable as `repark_parity.torture`: a `generate` CLI writing Parquet AND CSV per family, the tier switch (`ci` default 10k rows into a temp dir at test time; `full` ≥1M rows under `/tmp/torture/` reused via a rows+seed manifest), and one `Family` protocol every family implements. Generators are deterministic for a seed. | CLI subprocess pins + byte-identity pin + manifest-reuse pin + protocol isinstance pin. | **PROVEN** |
| C-002 | The `nested` family carries the D-2 shapes: deep struct/list nesting, mixed element types, lists of structs, capitalised field names, null-typed lists; its generator takes `--depth` and `--width` (D-6). | `test_torture_nested.py` cells incl. the depth/width scaling cell. | **PROVEN** |
| C-003 | The `inference` family carries the D-2 shapes: int32→int64 conflict at rows//2 (row 5k at CI 10k; row 500k at full 1M), string-vs-float halves, bool-looking ints, date-looking strings. | `test_torture_inference.py` cells. | **PROVEN** |
| C-004 | The suite skeleton runs BOTH doors (DataFrame read and `spark.sql` over a temp view) per family and asserts per D-3: the read completes or refuses loud, the row count equals the generated count, the schema equals the generator's declared expectation, and for `inference` the inferred type per column equals the expected one. | The four suite files; every cell names its door. | **PROVEN** |
| C-005 | Red-first: the suite was run before the families existed and failed; the failing output is pasted below. | The verbatim red run in the evidence section. | **PROVEN** |
| C-006 | A cell that fails on the real reader is filed per D-3: registry row `CSV-INFER-INT32-WIDTH` in `docs/spark-sql-iceberg-parity.md` with the reproduction command and the recorded oracle, and the cell marked `xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")`. | Registry row + the strict-xfail param cell. | **PROVEN** |
| C-007 | The Makefile target `py-test-torture` exists, is tiered via `TORTURE_TIER` (default `ci`, `make py-test-torture TORTURE_TIER=full`), and is NOT added to `preflight`. | Makefile diff. | **PROVEN** |
| C-008 | Maps for every new directory are created and every touched directory's map is updated in the same commit; the ledger is linked from `task/ledgers/staging/map.md`. | `scripts/check_map_md.sh` + the map diffs. | **PROVEN** |
| C-009 | Gates green: `make py-test-torture`, `.venv/bin/python -m pytest python/repark-parity/tests/torture -q`, `make verify`. | Real exit codes recorded in the evidence section. | **PROVEN** |

## Red-first evidence (C-005)

Command: `.venv/bin/python -m pytest python/repark-parity/tests/torture -q`, run after the
suite files existed and before any package file did:

```
ImportError while loading conftest '/tmp/oc-preparity/python/repark-parity/tests/torture/conftest.py'.
python/repark-parity/tests/torture/conftest.py:10: in <module>
    from repark_parity.torture import FAMILIES, FamilyOutput
E   ModuleNotFoundError: No module named 'repark_parity.torture'
```

## Measured oracle and reader behavior (2026-09-10)

Live PySpark 4.1.2 (zulu-17-amd64, local[1], 2026-09-10) and the .venv debug native module,
same generated files:

| Shape | Spark 4.1.2 | repark | Verdict |
|---|---|---|---|
| `nested` parquet `list<null>` column (`user_properties`) | `array<int>` | `list<int32>` | Spark-equal (registry `DYNFLATTEN-LISTNULL-1`); the declared expectation is the parity answer |
| `nested` parquet deep struct/list chain | names preserved | names preserved | Spark-equal; both doors agree |
| `inference` CSV `growth` (int32 head, int64 tail) | `bigint` | `int64` | Spark-equal |
| `inference` CSV `halves` (float/text halves) | `string` | `string` | Spark-equal |
| `inference` CSV `boolish` (0/1 column) | `int` | `int64` | DIVERGES → `CSV-INFER-INT32-WIDTH` |
| `inference` CSV `datish` (date-looking strings) | `date` | `date32` | Spark-equal |

`pyarrow` 25 refuses to write nested types as CSV (`ArrowInvalid: Unsupported Type:
list<...>`), so the `nested` family's CSV leg renders nested columns as JSON text (stdlib
`csv` + `json`, deterministic) and the suite pins that leg to completes-or-refuses-loud plus
row count only; the `nested` schema contract rides the Parquet leg. The `inference` family's
Parquet leg stores the resolved types, so its declared schema is identical on both formats.

## Evidence (gates, C-009)

All three gate commands, run 2026-09-10 on this branch (real exit codes):

```
$ make py-test-torture
...........x..........                                                   [100%]
21 passed, 1 xfailed in 10.69s
exit 0

$ .venv/bin/python -m pytest python/repark-parity/tests/torture -q
...........x..........                                                   [100%]
21 passed, 1 xfailed in 10.40s
exit 0

$ make verify
(exit 0; the static gates, ledger grammar, docs links, and the Rust workspace suite)
```

The full parity suite (`make py-test`) also runs green with the graft in place:
659 passed, 1 xfailed (its `xfail` is that suite's own pre-existing expectation). The one
`xfailed` here is `test_inferred_type_matches_declared_csv[boolish]`, strict, naming
`CSV-INFER-INT32-WIDTH`; the other three inference columns and every nested cell are live
green pins on both doors.

## Delivery attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: torture-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The suite ran red before the families existed (C-005), before the secrets module existed (C-021), and before the v3_dv generator/helper/fixture existed (C-028, pasted); the flag cells ran red against the silently-tolerated option and the Rust pins compile-red — all pasted.
      artifacts: [python/repark-parity/tests/torture/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: Both doors pinned per family; row counts, declared schemas, and per-column inferred types each have a named cell; the flag pin covers off/warn/refuse on both doors; the v3_dv cells pin the post-DV true row count, the surviving id set, and the declared schema on both doors over the committed fixture plus a live generate-and-read cell; step 5 re-ran every door cell at the full 1M-row tier on a release build with the identical xfail inventory and recorded per-door wall times.
      artifacts: [python/repark-parity/tests/torture/test_torture_nested.py, python/repark-parity/tests/torture/test_torture_inference.py, python/repark-parity/tests/torture/test_torture_secrets.py, python/repark-parity/tests/torture/test_torture_v3_dv.py, docs/perf/torture-1-2026-09-11.md]
    - id: AT-3
      status: ATTACKED
      evidence: The one divergent cell (CSV all-int columns) carries a registry row with a recorded live-PySpark oracle and a strict xfail naming the row id; steps 3 and 4 measured answers repark already honors (the declared secrets schema; the v3_dv DV read at 96-of-120), so no new registry row was filed; the step-5 full-tier run diverged nowhere new — the same 11 xfails at 1M rows.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/torture/test_torture_inference.py]
    - id: AT-4
      status: N/A
      justification: Generators are stateless module-level functions and small classes; no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; data writes go to temp dirs, /tmp/torture, or the ruled D-5 committed fixture; the secrets family's values are repark-fake- prefixed with no real key-id shape, and the flag inspects names only. The v3_dv live path uses a module-private catalog name, never `local`, and its session is stopped only when the generator created it.
      artifacts: [python/repark-parity/fixtures/torture/family.py, python/repark-parity/fixtures/torture/secrets.py, python/repark-parity/fixtures/torture/v3_dv.py]
    - id: AT-6
      status: ATTACKED
      evidence: One Family protocol for the file families; v3_dv deliberately stays outside FAMILIES (its generate returns a table result, not a Parquet+CSV pair, and its bytes are not deterministic) and the CLI carries it through the choices list plus the existing special-case dispatch shape; the flag rides the existing csv/json option maps.
      artifacts: [python/repark-parity/fixtures/torture/family.py, python/repark-parity/fixtures/torture/__main__.py, python/repark-parity/fixtures/torture/v3_dv.py, python/repark/src/repark/spark/session/reader_support.py]
    - id: AT-7
      status: ATTACKED
      evidence: Tests and code land in the same commit; the red-first runs are recorded in this ledger for all four steps.
      artifacts: [task/ledgers/staging/torture-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: map.md created for fixtures/, fixtures/torture/, tests/torture/, fixtures/torture/data/ and data/v3_dv/ and updated for src/, src/repark_parity/, repark-parity/, tests/, staging ledgers; step 3 updated the torture fixture/test maps, the core src map, the facade session map, and the staging-ledger map; step 4 updated the fixtures, torture package, tests, repark-parity, and staging maps; step 5 updated docs/perf, tests/torture, and datasets maps for the results document and the S2-12 measurement.
      artifacts: [python/repark-parity/map.md, python/repark-parity/fixtures/torture/data/map.md, docs/perf/map.md, python/repark-parity/datasets/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Byte-identity determinism pin runs the committed CLI twice per file family — the registry now includes secrets; the manifest-reuse pin mutates its inputs and reds; v3_dv's non-determinism is recorded, not pinned.
      artifacts: [python/repark-parity/tests/torture/test_generate_is_deterministic.py]
    - id: AT-10
      status: ATTACKED
      evidence: Step 4's live leg is real and measured: the generator runs PySpark 4.1.2 with Iceberg 1.11.0 under zulu-17, asserts Spark's own post-delete COUNT(*) equals the computed truth, and the live cell is proven green co-collected after test_parity_live.py (124 passed); repark-only assertions sit in always-run cells.
      artifacts: [python/repark-parity/tests/torture/test_torture_v3_dv.py]
DELIVERY_SIGNOFF:
  pr_unit: torture-1
  artifacts_verified:
    ledger: PASS (C-001..C-037)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: steps 1-5 of 5; STATUS untouched per brief
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: torture-1
  flags: []
  count: 0
```

---

# Step 2 (2026-09-10) — extreme_types, smartcsv, temporal, decimal_overflow

**Unit:** TORTURE-1 step 2 · **Date:** 2026-09-10 · **Executor:** GLM 5.3 Flash
(zai/glm-5.3-flash), Actor · **Branch:** `feat/torture-1-s2` · **Base:** `main`
`149147c1` (step 1 merged) · **Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.

Scope: step 2 only — the four families (`extreme_types`, `smartcsv`, `temporal`,
`decimal_overflow`) and their suite cells, the filed registry rows, maps, and this
ledger extension. No `secrets` family, no `read_options.rs` edit (step 3), no `v3_dv`
(step 4), no results document (step 5), no `STATUS.md` edit.

No live Spark this round (the box is shared; the brief forbids starting a JVM). Every
Spark-side expectation below is either measured in the registry (cited rows) or labeled
as an unmeasured claim; the three new rows carry an explicit "oracle: none for this row"
note. `secrets`/`v3_dv` were not started.

## Step-2 proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-010 | The four step-2 families carry the D-2 shapes: `extreme_types` (high-precision decimals, UUIDs, paragraph strings, embedded HTML), `smartcsv` (header normalisation, blank cells, currency and decimal widths, bool spellings), `temporal` (Duration at the ±i64-microsecond bound and the whole-day edge 106751991, MonthDayNano mixed and zero-unit, DATE ± interval promotion boundaries, epoch extremes), `decimal_overflow` (decimal(38) sums and averages that overflow at the aggregate boundary); all deterministic for a seed, generated by the committed CLI, writing Parquet AND CSV. | The four modules in `fixtures/torture/`; the byte-identity CLI pins covering all six registered families. | **PROVEN** |
| C-011 | Each step-2 family has suite cells running BOTH doors (DataFrame read and `spark.sql` over a temp view) asserting D-3: the read completes or refuses loud (no panic, no silent truncation), the row count equals the generated count, and the schema equals the generator's declared expectation. | The four test files; every cell names its door. | **PROVEN** |
| C-012 | Red-first: the suite was run after the step-2 cells and registrations existed and before the family modules did, and it failed; the failing output is pasted below. | The verbatim red run in the step-2 evidence section. | **PROVEN** |
| C-013 | Divergent cells are filed per D-3: new registry rows `CSV-INFER-HEADER-CASE`, `SUM-DEC-I128WRAP-1`, `DATE-INTERVAL-NSBOUND-1` with reproduction commands and pins landing in the same change; divergences already on the registry are CITED, not re-filed (`CSV-INFER-INT32-WIDTH` ×4 cells, `CSV-INFER-20DIGIT` ×3 cells, `BL-14` ×1 cell); the `AVG` loud-refusal cell cites the recorded `AVG-DEC-SUMWRAP-1` Spark class without a duplicate row. No duplicate rows were filed. | The three new registry rows + the 11-cell xfail inventory in the evidence section. | **PROVEN** |
| C-014 | The CI-tier workload pin now covers the four step-2 families: at CI rows each generates and reads on both doors inside the 60-second budget. | `test_ci_tier_step2_families_under_60_seconds`. | **PROVEN** |
| C-015 | Every Spark behaviour the step declares is measured or honestly labeled: the step-2 measured-behavior table below records repark's answers with their evidence class; the two repark-side declarations without a live-Spark half (`duration_us`, smartcsv's all-blank `string` inference) are marked as such, and no registry row claims a live oracle that was not run. | The step-2 measured-behavior table. | **PROVEN** |
| C-016 | Maps in lockstep and gates green: `fixtures/torture/map.md` and `tests/torture/map.md` name every new file in the same commit, `task/ledgers/staging/map.md` is updated, and the four gate commands pass with real exit codes. | The gate runs in the step-2 evidence section. | **PROVEN** |

## Step-2 red-first evidence (C-012)

Command: `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
python/repark-parity/tests/torture -q`, run after the four test files, the conftest
fixtures, the `__init__` registrations and the timer cell existed and before any step-2
family module did (exit 4):

```
ImportError while loading conftest '/tmp/oc-preparity/python/repark-parity/tests/torture/conftest.py'.
python/repark-parity/tests/torture/conftest.py:16: in <module>
    from _support import family_output
python/repark-parity/tests/torture/_support.py:11: in <module>
    from repark_parity.torture import Family, FamilyOutput
python/repark-parity/fixtures/torture/__init__.py:5: in <module>
    from repark_parity.torture.decimal_overflow import DECIMAL_OVERFLOW_FAMILY
E   ModuleNotFoundError: No module named 'repark_parity.torture.decimal_overflow'
```

## Measured repark behavior behind step 2 (2026-09-10)

Repark measured through the .venv native module on this branch; no live Spark was run
(the box was shared, per the brief). Spark halves marked *(measured, cited)* come from
the registry rows named; *(unmeasured claim)* marks a declaration that needs a live run.

| Shape | repark answer (measured 2026-09-10) | Spark half | Cell |
|---|---|---|---|
| parquet `decimal(38,18)` / `decimal(38,0)` | `decimal128(38,18)` / `decimal128(38,0)`, both doors | file-carried type, verbatim (certain) | extreme_types parquet cells (green) |
| CSV capitalised header (`Qty`, `Order Total`) | `AnalysisException` on both doors, naming the lowercased spelling | keeps the header verbatim (measured, cited: `CSV-INFER-HEADER-NEWLINE`; case shape unmeasured) | smartcsv CSV schema cell (xfail, `CSV-INFER-HEADER-CASE`) |
| parquet capitalised/space field names | kept verbatim, both doors | verbatim (certain) | smartcsv parquet cells (green) |
| CSV 38-digit integer text | `double` | `decimal(38,0)`-class (measured, cited: `CSV-INFER-20DIGIT`) | `big38`/`v`/`w` cells (xfail) |
| CSV strict bool spellings (`true/FALSE/True/false`) | `bool` | `boolean` (documented Spark CSV behavior) | smartcsv parquet + probe (green) |
| CSV loose bool spellings (`yes/no/Y/N`) | `string` | `string` (Spark bool-inference accepts only true/false) | smartcsv probe (green class) |
| CSV all-blank column | `string` | `string` (unmeasured claim; high confidence) | smartcsv declared CSV schema (inside the xfail cell) |
| parquet timestamp(µs) `isAdjustedToUTC=false` at ±i64 µs | `timestamp[us]`, values read, both doors | `timestamp_ntz` / `timestamp[us]` via Arrow (measured, cited: `READ-TSNTZ-DTYPE-1`) | temporal parquet cells (green) |
| parquet Duration annotation (`duration(µs)`) at ±i64 µs | `duration[us]`, both doors | unmeasured claim — no registry row, no live run | temporal parquet cells (green, repark-side) |
| parquet `month_day_nano_interval` | pyarrow refuses the write (`ArrowNotImplementedError`) | n/a — values ride the int decomposition columns | temporal design (map.md) |
| CSV timestamp text (epoch edges) | `timestamp[us, tz=UTC]` | `timestamp` instant (measured, cited: `READ-TSNTZ-DTYPE-1`) | temporal ts_text cell (green) |
| CSV timestamp text year 9999 / 0001 | refuses loud: ns-window cast error | µs-wide window (unmeasured claim) | deliberately not pinned; recorded here + map |
| parquet `date32` at day 106751991 | `date32`, both doors | `date` (certain, file-carried) | temporal parquet cells (green) |
| `try_add(day, INTERVAL 0 HOUR)` | `date32` | `timestamp` (measured, cited: `BL-14`) | temporal promotion cell (xfail) |
| `day + INTERVAL 1 DAY` at day 106751990 | raises `Date arithmetic overflow` (loud, both doors) | completes with a year-292278 timestamp (unmeasured claim) | temporal boundary cell (xfail, `DATE-INTERVAL-NSBOUND-1`) |
| `SUM(decimal(38,0))` whose true sum ≈ 9.5e41 | wrapped i128 value `-68368443260189989741903949496846385152`, both doors | `ARITHMETIC_OVERFLOW` (measured class, cited: `AVG-DEC-SUMWRAP-1`; SUM half unmeasured) | decimal_overflow sum cell (xfail, `SUM-DEC-I128WRAP-1`) |
| `AVG(decimal(38,0))` same fixture | raises `Arithmetic Overflow in AvgAccumulator`, both doors | `ARITHMETIC_OVERFLOW` (measured class, cited: `AVG-DEC-SUMWRAP-1`) | decimal_overflow avg cell (green) |
| `SUM`/`AVG` result types on small `decimal(38,0)` | `decimal128(38,0)` / `decimal128(38,4)` | same (Spark's bounded rule; consistent with `AVG-DEC-SUMWRAP-1`) | probe control (green class) |
| Python `Decimal` context | unary minus, `*`, and `+` round to 28 significant digits — the generators build every decimal from exact int/string forms | n/a | generator design (map.md) |

## Step-2 xfail inventory (C-013)

```
test_torture_decimal_overflow.py::test_decimal_overflow_inferred_type_matches_declared_csv[v]      CSV-INFER-20DIGIT
test_torture_decimal_overflow.py::test_decimal_overflow_inferred_type_matches_declared_csv[w]      CSV-INFER-20DIGIT
test_torture_decimal_overflow.py::test_decimal_overflow_sum_refuses_loud                            SUM-DEC-I128WRAP-1
test_torture_extreme_types.py::test_extreme_inferred_type_matches_declared_csv[big38]               CSV-INFER-20DIGIT
test_torture_smartcsv.py::test_smartcsv_csv_schema_matches_declared                                 CSV-INFER-HEADER-CASE
test_torture_temporal.py::test_temporal_inferred_type_matches_declared_csv[months]                  CSV-INFER-INT32-WIDTH
test_torture_temporal.py::test_temporal_inferred_type_matches_declared_csv[days]                    CSV-INFER-INT32-WIDTH
test_torture_temporal.py::test_temporal_inferred_type_matches_declared_csv[nanos]                   CSV-INFER-INT32-WIDTH
test_torture_temporal.py::test_temporal_try_add_zero_hour_promotes                                  BL-14
test_torture_temporal.py::test_temporal_date_plus_one_day_completes                                 DATE-INTERVAL-NSBOUND-1
test_torture_inference.py::test_inferred_type_matches_declared_csv[boolish]                         CSV-INFER-INT32-WIDTH  (step 1, unchanged)
```

## Step-2 gate evidence (C-016)

All four gate commands, run 2026-09-10 on this branch (real exit codes):

```
$ make py-test-torture
..............xxx........x........x................x..........xxxxx      [100%]
56 passed, 11 xfailed in 33.72s
exit 0

$ .venv/bin/python -m pytest python/repark-parity/tests/torture -q
..............xxx........x........x................x..........xxxxx      [100%]
56 passed, 11 xfailed in 33.82s
exit 0

$ make verify
(exit 0; fmt, clippy x3, crate-dag, lib-rs, rust-file-size, lib-py, python-conventions,
docstring-presence, map/manifest/ledger gates, and the Rust workspace test suite —
52 test-result-ok lines, 0 failures)

$ make py-test
695 passed, 11 xfailed in 75.60s
exit 0
```

The isolated `make py-test` count includes the torture directory running in full on this
box (the .venv carries the native module locally; in ci.yml's native-free env the
`conftest.py` import guard skips it, as step 1 established). The 11 xfails are exactly
the step-2 inventory above: 10 new cells plus step 1's unchanged `boolish` cell.

Out-of-scope observations (recorded, not acted on): the nested CSV leg (step 1) also hits
`CSV-INFER-HEADER-CASE` (capitalised headers) and passes through its loud-refusal arm;
repark's `CAST(date AS STRING)` refuses for dates beyond year 9999 (`Failed to convert
106751990 to temporal for Date32`, measured during probing) — unfiled, no pin, a candidate
for the live-measurement round.

## Orchestrator addendum — the three step-2 registry rows are now measured (2026-09-10)

Step 2 filed `CSV-INFER-HEADER-CASE`, `SUM-DEC-I128WRAP-1` and `DATE-INTERVAL-NSBOUND-1` with
their Spark halves honestly labelled unmeasured, because the round was forbidden to start a JVM
while another lane was building. The orchestrator ran the live oracle on the audit
(PySpark 4.1.2, `zulu-17-amd64`, `local[1]`, ANSI on, the same three fixtures at 10k rows) and
rewrote all three Spark halves from belief to measurement.

| Row | Claim as filed | Measured | Outcome |
|---|---|---|---|
| `CSV-INFER-HEADER-CASE` | Spark keeps capitalised headers verbatim and completes | `struct<Order Total:string,Qty:int,…>`, `count() = 10000` | **confirmed** |
| `SUM-DEC-I128WRAP-1` | Spark raises on the decimal `SUM` overflow | `ARITHMETIC_OVERFLOW` / SQLSTATE 22003 on both decimal columns, for `SUM` itself | **confirmed** |
| `DATE-INTERVAL-NSBOUND-1` | Spark **promotes to timestamp** and completes | `day + INTERVAL 1 DAY` has type **`date`**; 10,000 rows; `max` `+294247-01-10` → `+294247-01-11` | **narrowed** |

The third is why the measurement was worth taking. Spark does **not** promote a day-only interval
to a timestamp, so the "promotion window ends a thousandfold early" reading was wrong and the
prescribed fix ("do the arithmetic at microsecond width") would have chased the wrong width. The
defect that survives is repark computing day arithmetic through nanoseconds; the fix is **day
width**. The row and its rationale now say so.

One trap recorded for the next live round: collecting the Spark answer into Python raises
`ValueError: year 294247 is out of range` from `datetime`. That is a Python limit, not a Spark
refusal — cast to `STRING` before collecting or the oracle reads backwards.

---

# Step 3 (2026-09-11) — the secrets family and the D-4 flag

**Unit:** TORTURE-1 step 3 · **Date:** 2026-09-11 · **Executor:** Devin SWE-2
(swe-2-high), Actor · **Branch:** `feat/torture-1-step-3` · **Base:** `main`
`bf810824` · **Model:** swe-2-high
**risk_tier:** standard.

Scope: step 3 only — the `secrets` family, the `flag_secret_columns` read option parsed
in `read_options.rs` and plumbed through `DataFrameReader.option` to the csv/json engine
reads, the both-door suite cells, maps, and this ledger extension. No `v3_dv` (step 4),
no results document (step 5), no `STATUS.md` edit.

Decision recorded: **D-4a** (G-2, run 7, binding) — "exact-name match only against the
existing needle set" means `prop_key_is_secret` applied to each TOP-LEVEL data column's
name exactly as the schema spells it: no new needle list, no nested-field walk, no
inspection of values, no fuzzy matching beyond the predicate.

Within-card rulings recorded (consequences of the card's homes, not new decisions):
`read_options.rs` is the csv/json option-map home, so the flag is honored on `csv` and
`json` reads and — via the facade's semantic gate, generalized from the `compression`
branch into `_CSV_JSON_ONLY_OPTION_KEYS` — refuses loud on readers without that option
map (parquet, `table`, empty-format `load`): a silently ignored refusal guard would
change load semantics. `warn` emits one `eprintln!` `WARNING:` line — repark-core
carries no `tracing` dependency (none may be added) and `eprintln!` is the crate's
existing warning channel (the session-install warning uses it); the facade's inferSchema
re-read pops the flag keys so exactly one WARNING fires per logical read.

## Step-3 proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-017 | The `secrets` family at `fixtures/torture/secrets.py` implements the `Family` protocol (seeded, deterministic, Parquet AND CSV): thirteen credential-shaped column names each tripping a distinct `prop_key_is_secret` needle, beside `id`/`name`/`note` and the `bucket_key` carve-out control; every flagged value is `repark-fake-`-prefixed and never a real key-id shape; registered in `FAMILIES` and the CLI. | `test_torture_secrets.py` protocol + `test_flagged_names_match_the_needle_set` cells; the byte-identity determinism pins now iterate it via `FAMILIES`. | **PROVEN** |
| C-018 | `flag_secret_columns` = `off`\|`warn`\|`refuse`, default `off`, parsed in `read_options.rs` (`secret_column_flag`); any other value refuses loud naming the option and the three accepted values before any I/O. `warn` logs exactly one WARNING naming the flagged columns; `refuse` raises `AnalysisException` naming them; `off` changes nothing. Plumbed from the Python reader option through the csv/json option maps; refused loud on parquet/table readers. | torture flag cells on both doors + the parquet/table refusal cells + the bad-value refusal; `session/map.md` rows. | **PROVEN** |
| C-019 | `test_torture_secrets.py` runs the D-3 cells for the family on both doors (Parquet + CSV row counts and declared-schema equality through the DataFrame read and `spark.sql` over a temp view), plus `test_secret_flag_off_warn_refuse` parametrized over both doors — for the sql door the temp view is registered from the flagged read — plus the bad-value refusal and the option-map-less refusal. | The suite file; every cell names its door. | **PROVEN** |
| C-020 | Rust unit tests for the parser in the crate's existing test layout (`read_options.rs` `#[cfg(test)] mod tests`): default, each accepted value, folded spelling, bad-value refusal, apply semantics (off/warn pass; refuse names exactly the flagged names), and end-to-end pins through `Session::read_csv`/`read_json`. | `cargo test -p repark-core read_options` — 11 pins green. | **PROVEN** |
| C-021 | Red-first: the suite ran before `secrets.py` existed and failed on `ModuleNotFoundError`; the five flag cells ran against the unimplemented option and failed (the option was silently tolerated); the Rust test module failed to compile before the implementation. All three runs pasted below. | The verbatim red runs in the step-3 evidence section. | **PROVEN** |
| C-022 | The CI-tier workload pin now covers the secrets family: at CI rows it generates and reads on both doors inside the 60-second budget. | `test_ci_tier_step3_family_under_60_seconds`. | **PROVEN** |
| C-023 | Maps in lockstep and gates green: `fixtures/torture/map.md`, `tests/torture/map.md`, `crates/repark-core/src/map.md`, `python/repark/src/repark/spark/session/map.md`, and `task/ledgers/staging/map.md` updated in the same commit; the card's gate commands pass with real exit codes. | The gate runs in the step-3 evidence section. | **PROVEN** |

## Step-3 red-first evidence (C-021)

Command 1: `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
python/repark-parity/tests/torture -q`, run after the test file, the conftest fixture,
the `__init__` registration and the timer cell existed and before `secrets.py` did
(exit 4):

```
ImportError while loading conftest '/tmp/dv-tort3/python/repark-parity/tests/torture/conftest.py'.
python/repark-parity/tests/torture/conftest.py:16: in <module>
    from _support import family_output
python/repark-parity/tests/torture/_support.py:11: in <module>
    from repark_parity.torture import Family, FamilyOutput
python/repark-parity/fixtures/torture/__init__.py:10: in <module>
    from repark_parity.torture.secrets import SECRETS_FAMILY
E   ModuleNotFoundError: No module named 'repark_parity.torture.secrets'
```

Command 2: the same suite after `secrets.py` landed but before the flag existed —
every family cell green, the five flag cells red because the option was silently
tolerated (exit 1):

```
FAILED test_torture_secrets.py::test_secret_flag_off_warn_refuse[dataframe] — AssertionError: []
FAILED test_torture_secrets.py::test_secret_flag_off_warn_refuse[sql] — AssertionError: []
FAILED test_torture_secrets.py::test_secret_flag_bad_value_refuses_loud — DID NOT RAISE AnalysisException
FAILED test_torture_secrets.py::test_secret_flag_refused_on_reads_without_the_option_map — DID NOT RAISE AnalysisException
FAILED test_torture_secrets.py::test_secret_flag_refuse_names_flagged_columns_on_json — DID NOT RAISE AnalysisException
5 failed, 62 passed, 11 xfailed in 46.78s
```

Command 3: `cargo test -p repark-core read_options` after the test module was added and
before the implementation (exit 101):

```
error[E0425]: cannot find function `secret_column_flag` in this scope
error[E0433]: cannot find type `SecretColumnFlag` in this scope
error[E0425]: cannot find function `apply_secret_column_flag` in this scope
error: could not compile `repark-core` (lib test) due to 19 previous errors
```

## Measured repark behavior behind step 3 (2026-09-11)

Repark measured through the .venv native module on this branch; no live Spark was run.
The flag contract is engine-local, so the table records repark's own answers.

| Shape | repark answer (measured 2026-09-11) | Cell |
|---|---|---|
| `secrets` parquet/CSV read, flag unset or `off` | full row count, declared schema `id: int64` + 16 strings, all nullable, both doors | family door cells (green) |
| `warn` on a flagged CSV | reads normally; exactly one stderr line `WARNING: flag_secret_columns=warn: credential-shaped column names in the read schema: <all 13 names>` | `test_secret_flag_off_warn_refuse` (green) |
| `refuse` on a flagged CSV/JSON | `AnalysisException` naming all 13 flagged names and no ordinary name, raised at read time (no view registered) | flag cells (green) |
| bad value (`bogus`) | `AnalysisException` naming `flag_secret_columns` and `off`/`warn`/`refuse`, before any I/O | bad-value cell + Rust pin (green) |
| flag on `.parquet()` / `format("parquet").load()` | `AnalysisException`: `reader option 'flag_secret_columns' is not supported by repark yet` | option-map-less cell (green) |
| `bucket_key` (`_key` suffix, `bucket` carve-out) | never flagged — present in both schemas, absent from warn/refuse output | membership + flag cells (green) |

## Step-3 gate evidence (C-023)

```
$ cargo test -p repark-core read_options
11 passed; 0 failed  exit 0

$ make develop
Finished dev profile; repark-1.1.1 editable install  exit 0

$ make py-test-torture
68 passed, 11 xfailed in 44.06s  exit 0

$ .venv/bin/python -m pytest python/repark/tests -q -k "read or option"
206 passed, 8 skipped in 29.63s  exit 0

$ .venv/bin/python -m pytest test_e2_readwriter.py test_r1_read_formats.py \
    test_facade_polish.py test_csv_infer_perf_1.py -q   (reader surface files)
166 passed, 3 skipped in 15.21s  exit 0

$ PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv \
    uv run --no-project python -m pytest python/repark-parity/tests -q
731 passed, 11 xfailed in 97.96s  exit 0

$ make verify
exit 0 — fmt, clippy x3, panic-ban, crate-dag, lib-rs, rust-file-size, lib-py,
python-conventions, docstring-presence, example-coverage, manifest, ledgers,
ledger-grammar (94 live ledgers clean), docs-compaction, docs-links, owner-ruling,
parity-live dual-wire, matrix-test-liveness, rust-check, py-lint, py-format-check,
py-lock, toml, spell; full workspace test suite green.
```

Fence note: the card's no-comments grep
(`git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'`)
prints 13 lines on this diff — every one a Rust attribute required by the mandated parser
tests (`#[derive]` on the flag enum, `#[cfg(test)]`, `#[test]`, `#[tokio::test]`); the
card's own text classifies `///`/`//!` as comments, and no such line exists. The
attribute-aware re-run `grep -P '^\+\s*(//|#(?! noqa|\[))'` prints nothing.

Out-of-scope observations (recorded, not acted on): `datasets/secrets/` (the separate
seeded generator with `FAKE_PREFIX = "repark-fake-"`) declares the same hygiene contract
this family now shares; its map's "opt-in flagging is a later facade feature" sentence is
now delivered by this step. The `warn` channel is stderr (`eprintln!`), not the
opt-in tracing subscriber — callers grepping `REPARK_LOG` output will not see it.

---

# Step 4 (2026-09-11) — the v3_dv family under live Spark

**Unit:** TORTURE-1 step 4 · **Date:** 2026-09-11 · **Executor:** Devin SWE-2
(swe-2-high), Actor · **Branch:** `feat/torture-1-step-4` · **Base:** `main`
`3f4a8adc` (step 3 merged) · **Model:** swe-2-high
**risk_tier:** standard.

Scope: step 4 only — the `v3_dv` generator (D-5, live-Spark-only), the ≤ 1 MB
committed CI fixture, and its suite cells. No `docs/perf/` results document, no
`docs/testing.md` paragraph (step 5), no `STATUS.md` edit.

Live-Spark environment used (the card's load-bearing flags, all observed):
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` (default `java` is 11), `.venv` pyspark 4.1.2
verified (`import pyspark` → `4.1.2`), the `.ivy2` cache copy at `$PWD/.ivy2` with
`PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=$PWD/.ivy2 pyspark-shell"`, and
`pgrep -x java` empty before and after every JVM run. `.ivy2/` is in
`.git/info/exclude`, never committed (the clone's tracked seed files inside it were
restored after Spark's resolver stamped a timestamp).

Within-card rulings recorded (consequences of the card's homes, not new decisions):
`v3_dv` is **not** registered in `FAMILIES` — `Family.generate` returns a
Parquet+CSV `FamilyOutput`, this family produces an Iceberg table, and Spark's output
bytes (UUID file names, metadata timestamps) are not deterministic, so the
byte-identity and repository-refusal pins in `test_generate_is_deterministic.py` stay
scoped to the file families unchanged. The CLI gains the family through the `choices`
list plus a dispatch branch beside `nested`'s (the existing special-case shape). The
`--out` argument is the table's **warehouse**: the table root lands at
`<out>/ns/v3dv`. The committed fixture is a copy of that table root including the
`truth.json` the generator writes there; `refuse_repository_output` still fires for it
because the fixture is generated outside the repo and copied in by hand — `--out` is
never a repository path.

## Step-4 proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-024 | The `v3_dv` generator exists at `fixtures/torture/v3_dv.py`, is registered in the committed CLI (`generate v3_dv --rows N --seed S --out DIR`), and runs only under `REPARK_PARITY_LIVE=1` — otherwise it refuses loud naming the flag (direct call AND CLI subprocess). | `test_v3_dv_generate_refuses_without_live_flag`, `test_v3_dv_cli_registers_and_refuses`. | **PROVEN** |
| C-025 | A ≤ 1 MB checked-in CI fixture exists under `fixtures/torture/data/v3_dv/`, written by that generator, with the true counts recorded beside it (`truth.json`: 120 written, 24 deleted under `id % 5 = 2`, 96 true rows, 24 data files, one Puffin DV). | `du -sb` = 315020 bytes; `test_v3_dv_fixture_truth_is_self_consistent`. | **PROVEN** |
| C-026 | Relocation is measured, not assumed: repark follows the absolute paths baked into Iceberg metadata — a copy at an arbitrary path fails on manifest-list load — so the suite materializes the fixture onto its canonical baked-in location (`/tmp/repark-torture-v3dv/ns/v3dv`) under a directory lock, the `test_v3_live_oracle.py` contract. | The verbatim relocation probe below + `_support.materialize_table`. | **PROVEN** |
| C-027 | The D-3 cells run on BOTH doors over the checked-in fixture with no JVM: `read.table` and `spark.sql` over a temp view each assert repark's row count equals the truth record's `true_rows` (96), the schema equals `DECLARED_SCHEMA`, and the surviving id set equals the recomputed expectation; a `delete_files` cell pins that the fixture carries live Puffin DVs (content 1); a live-only cell generates a fresh table and reads it back on both doors. | `test_torture_v3_dv.py` — 7 always-run cells + 1 live cell, all green. | **PROVEN** |
| C-028 | Red-first: the suite ran before the generator, the `_support` helper, and the fixture existed and failed at collection; the verbatim output is pasted below. | The red run in the step-4 evidence section. | **PROVEN** |
| C-029 | Live-cell rules honored: the generator records `SparkSession.getActiveSession()` first and stops only a session it created; the Spark catalog name is module-private (`torture_v3dv`, never `local`); `PYSPARK_SUBMIT_ARGS` is never popped; no per-call `spark.jars.ivy` tempdir; the new live test is proven green CO-COLLECTED after `python/repark/tests/test_parity_live.py`; repark-only assertions sit in always-run cells; `pgrep -x java` printed nothing before the first JVM and after the last. | The co-collected gate run + the pgrep checks in evidence. | **PROVEN** |
| C-030 | Maps in lockstep and gates green: `fixtures/map.md`, `fixtures/torture/map.md`, `fixtures/torture/data/map.md` + `data/v3_dv/map.md` (new), `tests/map.md`, `tests/torture/map.md`, `repark-parity/map.md`, `staging/map.md` updated in the same commit; `make py-test-torture`, the co-collected live run, the whole parity suite, and `make verify` pass with real exit codes. | The gate runs in the step-4 evidence section. | **PROVEN** |

## Step-4 red-first evidence (C-028)

Command: `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
python/repark-parity/tests/torture -q`, run after `test_torture_v3_dv.py` existed and
before `v3_dv.py`, the `_support.materialize_table` helper, and the fixture did
(exit 2):

```
ImportError while importing test module '/tmp/dv-tort4/python/repark-parity/tests/torture/test_torture_v3_dv.py'.
python/repark-parity/tests/torture/test_torture_v3_dv.py:12: in <module>
    from _support import materialize_table
E   ImportError: cannot import name 'materialize_table' from '_support' (/tmp/dv-tort4/python/repark-parity/tests/torture/_support.py')
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.20s
```

A second red once the code landed but before the fixture was generated — the four
fixture cells errored on the missing tree while the three JVM-free cells passed
(exit 1):

```
E   FileNotFoundError: [Errno 2] No such file or directory: '/tmp/dv-tort4/python/repark-parity/fixtures/torture/data/v3_dv'
3 passed, 1 skipped, 4 errors in 1.37s
```

## Measured behavior behind step 4 (2026-09-11)

Repark measured through the .venv native module; the Spark half was produced live by
the generator itself (PySpark 4.1.2, `zulu-17-amd64`, `local[2]`, Iceberg
1.11.0 hadoop catalog).

| Shape | repark answer (measured 2026-09-11) | Spark half | Cell |
|---|---|---|---|
| format-v3 MoR table read under live Puffin DVs | 96 rows of 120 written (24 deleted), both doors; surviving id set exact | `SELECT COUNT(*)` = 96 inside the generator | door cells (green) |
| schema of the DV table | `id int32, name string, part int32`, all nullable | `struct<id:int,name:string,part:int>` (all nullable, Spark DDL default) | door cells (green) |
| `delete_files` metadata table on the fixture | 24 rows, all `content = 1` `PUFFIN` | 24 delete-file entries (one .puffin file on disk, listed once per data file it covers) | DV-presence cell (green) |
| relocating the table tree to an arbitrary path | `PySparkException: ... Failed to read file <baked-in path>: No such file or directory` on manifest-list load | n/a — Iceberg metadata carries absolute file paths | materialize-table design (measured probe below) |
| `generate` without `REPARK_PARITY_LIVE=1` | `RuntimeError` naming the flag, before any JVM work | n/a | refusal cells (green) |

The relocation probe (verbatim): a repark-written Iceberg table at
`/tmp/dv-reloc-orig/.../ns/t` was copied to `/tmp/dv-reloc-moved`, the original tree
deleted, and `register_table` + `SELECT` answered:

```
repark.errors.PySparkException: Unexpected => Failed to load manifest list in cache,
source: DataInvalid => Failed to read file
/tmp/dv-reloc-orig/repark_ctas/ice/ns/t/metadata/snap-5782391719024352955-0-01a08f43-d46d-7761-a42e-7cf1afc0a3e6.avro:
No such file or directory (os error 2)
```

so the committed fixture carries the `v3-spark-part-dv` contract: it is materialized
onto its baked-in absolute location under a lock, never read in place.

## Step-4 fixture and generation evidence (C-024, C-025)

Generation transcript (live, JAVA_HOME=zulu-17-amd64, ivy cache at `$PWD/.ivy2`):

```
$ python -m repark_parity.torture generate v3_dv --rows 120 --seed 7 --out /tmp/repark-torture-v3dv
v3_dv: 96 live rows -> /tmp/repark-torture-v3dv/ns/v3dv
exit 0
```

truth.json (committed at the fixture root):

```json
{"family": "v3_dv", "seed": 7, "rows_written": 120,
 "delete": {"modulus": 5, "residue": 2}, "rows_deleted": 24, "true_rows": 96,
 "data_files": 24, "delete_files": 24, "format_version": 3, "table": "ns.v3dv",
 "table_location": "/tmp/repark-torture-v3dv/ns/v3dv",
 "metadata_file": "metadata/v14.metadata.json",
 "schema": "struct<id:int,name:string,part:int>"}
```

```
$ du -sb python/repark-parity/fixtures/torture/data/v3_dv
315020	python/repark-parity/fixtures/torture/data/v3_dv
$ find ... -name '*.puffin' | wc -l   -> 1
$ find ... -name '*.parquet' | wc -l  -> 24
$ find ... -type f | wc -l            -> 67  (.crc sidecars stripped)
```

## Step-4 gate evidence (C-029, C-030)

All gate commands run 2026-09-11 on this branch (real exit codes):

```
$ pgrep -x java   (before the first JVM run, and after the last)
exit 1 (empty)

$ make py-test-torture
...............xxx........x........x...........................x........ [ 82%]
..xxxxx.......s                                                          [100%]
75 passed, 1 skipped, 11 xfailed in 43.53s
exit 0

$ REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest \
    python/repark/tests/test_parity_live.py \
    python/repark-parity/tests/torture/test_torture_v3_dv.py -q
124 passed in 57.92s
exit 0
(test_v3_dv_live_generate_and_read PASSED — verified in the -v rerun, 8/8 cells green)

$ PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv \
    uv run --no-project python -m pytest python/repark-parity/tests -q
738 passed, 1 skipped, 11 xfailed   (exit 0; the docs-links cell reads the git index,
so the count includes the fixture staged for commit — see the note below)

$ make verify
exit 0 — fmt, clippy x3, panic-ban, crate-dag, lib-rs, rust-file-size, lib-py,
python-conventions, docstring-presence, example-coverage, manifest, ledgers,
ledger-grammar, docs-compaction, docs-links, owner-ruling, parity-live dual-wire,
matrix-test-liveness, rust-check, py-lint, py-format-check, py-lock, toml, spell;
workspace test suite green.
```

Docs-links note: `test_real_tree_is_green_under_the_seeded_allowlist` reds on
links to not-yet-tracked files, so the parity suite shows one spurious failure until
the new maps are `git add`ed; with the files staged it is green (19 passed) and the
committed tree satisfies it.

No new registry rows: repark reads the DVs correctly on both doors, so no cell
diverged and no `xfail` was filed — the measured-Spark half is the generator's own
post-delete `COUNT(*)`.

Out-of-scope observations (recorded, not acted on): the `delete_files` metadata table
lists the single on-disk `.puffin` once per data file it covers (24 rows for one
file) — a metadata-table listing contract, not a divergence; `git status` shows this
clone carries a small tracked `.ivy2` seed whose `ivydata-*.properties` the resolver
re-stamps on each run (restored, and the directory is `.git/info/exclude`d here).

---

# Step 5 (2026-09-11) — the full tier, the results document, S2-12

**Unit:** TORTURE-1 step 5 · **Date:** 2026-09-11 · **Executor:** Devin SWE-2
(swe-2-high), Actor · **Branch:** `feat/torture-1-step-5` · **Base:** step-4 commit
`49c0f193` (stacked; its PR is in CI) · **Model:** swe-2-high
**risk_tier:** standard.

Scope: step 5 only — the release build, the full-tier run (≥1M rows per family,
generated once under `/tmp/torture/`), the dated results document
`docs/perf/torture-1-2026-09-11.md`, the `docs/testing.md` paragraph, the measured
S2-12 `datasets/` coverage + retirement ruling, maps, and this ledger extension.
No `STATUS.md` edit, no dependency or workflow change, no code change.

## Step-5 proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-031 | The timings stand on a RELEASE build: `cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release` finished `release` profile `[optimized]` in 8m 08s; `repark.__file__` resolves into `python/repark/src/repark/`; `repark._native.__debug_assertions__ is False`; `_native.abi3.so` is 166 793 824 bytes. | The build output and proof commands quoted below; the doc's profile header. | **PROVEN** |
| C-032 | Full tier measured: the seven file families generate 1 000 000 rows under `/tmp/torture/<family>` (one-time CLI generation, manifest-reused by the suite) and the `TORTURE_TIER=full` suite is green — `75 passed, 1 skipped, 11 xfailed`, the same xfail inventory as CI tier; per family × door rows, wall seconds and outcome are recorded in the doc from the stated harness. | The verbatim runs and generation timings below; the doc's results table. | **PROVEN** |
| C-033 | v3_dv at full tier: the live generator scales to 1M rows — `generate v3_dv --rows 1000000 --seed 7` under `REPARK_PARITY_LIVE=1`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, the `$PWD/.ivy2` redirect — wrote 1M rows, deleted 200 000 (`id % 5 = 2`), 800 000 true rows in 48.94 s; repark reads the surviving 800 000 on both doors (0.19 s `read.table`, 0.02 s `spark.sql` over a temp view); `pgrep -x java` empty before and after; `.ivy2/` sits in `.git/info/exclude`. | The generation transcript, `truth.json`, and the measured door reads — all quoted below and in the doc. | **PROVEN** |
| C-034 | `docs/perf/torture-1-2026-09-11.md` holds the results table (one row per family × door: rows, seconds, outcome), the box and build profile, and the exact commands; `docs/perf/map.md` lists the file in the same commit. | The document + the map diff. | **PROVEN** |
| C-035 | `docs/testing.md` gains one paragraph naming the suite, its tiers, and `make py-test-torture`; no docs ceiling exists for the file (`check_docs_compaction.py` CEILINGS) so nothing was raised or condensed. | The paragraph diff. | **PROVEN** |
| C-036 | S2-12 measured per D-7: each of `datasets/{nested,schema_inference,extreme_types,secrets,smartcsv}` carries a shape→torture-coverage table and a `grep -rn` consumer list in the step-5 document; verdicts — `nested` covered, retained (7 outside consumers incl. the dynflatten bench suite); `secrets` covered, retained (2); `schema_inference`, `extreme_types`, `smartcsv` partly covered — so zero directories, tests, map rows, or `_cache.py` slugs were removed. | The doc's S2-12 section; the unchanged `datasets/` tree; `datasets/map.md` carries the pin. | **PROVEN** |
| C-037 | Gates green: `TORTURE_TIER=full make py-test-torture`, `make py-test-torture` (CI tier), the whole parity suite, `python/repark/tests/test_datasets_facade.py`, `scripts/check_docs_links.py`, `make verify`; maps in lockstep in the same commit. | The real exit codes below. | **PROVEN** |

## Step-5 release-build proof (C-031)

```
$ cd python/repark && VIRTUAL_ENV=/tmp/dv-tort5/.venv uvx maturin@1.14.1 develop --release
    Finished `release` profile [optimized] target(s) in 8m 08s
📦 Built wheel for abi3 Python ≥ 3.12 … repark-1.1.1-cp312-abi3-linux_x86_64.whl
🛠 Installed repark-1.1.1

$ .venv/bin/python -c "import repark; print(repark.__file__)"
/tmp/dv-tort5/python/repark/src/repark/__init__.py
$ .venv/bin/python -c "import repark._native as n; print(n.__debug_assertions__)"
False
$ ls -la python/repark/src/repark/_native.abi3.so   -> 166793824 bytes, built 04:04
```

## Step-5 full-tier evidence (C-032, C-033)

One-time generation into `/tmp/torture` (committed CLI, seed 7, `/usr/bin/time` wall):

```
nested 43.99 s · inference 5.70 s · extreme_types 11.26 s · smartcsv 5.01 s ·
temporal 2.27 s · decimal_overflow 3.39 s · secrets 31.55 s   (~1.4 GB total)
```

v3_dv live generation (JVM alone; `pgrep -x java` empty before and after):

```
$ REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 \
    PYTHONPATH=python/repark-parity/src .venv/bin/python -m repark_parity.torture \
    generate v3_dv --rows 1000000 --seed 7 --out /tmp/repark-torture-v3dv-full
v3_dv: 800000 live rows -> /tmp/repark-torture-v3dv-full/ns/v3dv
V3DV_GEN_SECONDS 48.94

truth.json: rows_written 1000000, delete id%5=2, rows_deleted 200000,
true_rows 800000, data_files 24, delete_files 24, format_version 3
repark read.table -> 800000 rows in 0.19 s; spark.sql over temp view -> 800000 in 0.02 s
```

The suite at full tier (`--durations=0` run used for per-cell timings) and the gate:

```
$ TORTURE_TIER=full .venv/bin/python -m pytest python/repark-parity/tests/torture -q --durations=0
75 passed, 1 skipped, 11 xfailed in 33.45s   exit 0

$ TORTURE_TIER=full make py-test-torture
75 passed, 1 skipped, 11 xfailed in 33.99s   exit 0

$ make py-test-torture            (CI tier)
75 passed, 1 skipped, 11 xfailed in 19.12s   exit 0
```

The 11 xfails are the steps 1–4 inventory, unchanged at 1M rows — no cell regressed
at scale, so no new registry row was filed. The one skip is the
`REPARK_PARITY_LIVE` cell; its work ran by hand above (the live cell pins a fresh
120-row generate, not the 1M table, so the full-tier v3_dv evidence is the manual
transcript quoted here, not a suite cell).

## Step-5 S2-12 evidence (C-036)

Consumer enumeration command:

```
$ grep -rn "repark_datasets\|datasets/" -l python/ scripts/ Makefile docs/ task/
```

Outside-consumer findings per family (full tables in the step-5 document):
`nested` — `bench/dynflatten/measure.py`, `tests/test_dynflatten_bed.py`,
`test_dynflatten_bed_gate.py`, `test_dynflatten_listnull.py`,
`test_dynflatten_null_mask.py`, `test_parity_live_dynflatten.py`,
`test_datasets_facade.py`; the other four — `tests/test_datasets_manifest_types.py`
and `test_datasets_facade.py`. Coverage verdicts: `nested` and `secrets`
shape-covered but retained for those consumers; `schema_inference`
(leading-zero ids, comma decimals, e-notation, null-token vocabulary, €/£ marks,
single-letter bool spellings uncovered), `extreme_types` (the >38-digit demotion
class uncovered — `big38` tops out at 38), and `smartcsv` (BOM, preamble, duplicate
headers, ragged rows, the delimiter zoo, embedded delimiters uncovered) are partly
covered and stay. **Zero retirements** — no directory, test, map row, or
`_cache.py` slug removed; D-7's "no outside consumer" test fails for every family.

## Step-5 gate evidence (C-037)

```
$ PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv \
    uv run --no-project python -m pytest python/repark-parity/tests -q
737 passed, 1 skipped, 11 xfailed in 65.60s — plus the known
test_real_tree_is_green_under_the_seeded_allowlist red on the not-yet-tracked
perf doc (same index-visibility note as step 4: green once staged; re-run below).

$ .venv/bin/python -m pytest python/repark-parity/tests/test_dl_6_docs_links.py -q
   (run after git add of the new files) 19 passed in 1.85s   exit 0

$ .venv/bin/python -m pytest python/repark/tests/test_datasets_facade.py -q
21 passed in 0.82s   exit 0

$ python3 scripts/check_docs_links.py      (run after git add)
docs-links: 756 files, 4833 links checked — clean   exit 0

$ make verify
exit 0 — fmt, clippy x3, panic-ban, crate-dag (22 edges), lib-rs,
rust-file-size, lib-py, python-conventions, docstring-presence,
example-coverage, manifest, ledgers, ledger-grammar (95 live ledgers,
609 clauses), docs-compaction, docs-links, owner-ruling, parity-live
dual-wire, matrix-test-liveness, rust-check, py-lint, py-format-check,
py-lock, toml, spell; the workspace test suite green.
```

Out-of-scope observations (recorded, not acted on): the `secrets` and `nested`
generators dominate generation wall time (31.55 s / 43.99 s of ~103 s total); the
`torture` `nested` CSV leg refuses loud through `CSV-INFER-HEADER-CASE` at full tier
exactly as at CI tier; `/tmp/torture` persists ~1.4 GB between runs by design.
