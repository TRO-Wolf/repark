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
      evidence: The suite ran red before the families existed; output pasted in C-005 evidence.
      artifacts: [python/repark-parity/tests/torture/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: Both doors pinned per family; row counts, declared schemas, and per-column inferred types each have a named cell.
      artifacts: [python/repark-parity/tests/torture/test_torture_nested.py, python/repark-parity/tests/torture/test_torture_inference.py]
    - id: AT-3
      status: ATTACKED
      evidence: The one divergent cell (CSV all-int columns) carries a registry row with a recorded live-PySpark oracle and a strict xfail naming the row id.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/torture/test_torture_inference.py]
    - id: AT-4
      status: N/A
      justification: Generators are stateless module-level functions and small classes; no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; data writes go to temp dirs or /tmp/torture and a generator refuses repository-internal outputs.
      artifacts: [python/repark-parity/fixtures/torture/family.py]
    - id: AT-6
      status: ATTACKED
      evidence: One Family protocol; both families implement it; the CLI adds no second dispatch surface.
      artifacts: [python/repark-parity/fixtures/torture/family.py, python/repark-parity/fixtures/torture/__main__.py]
    - id: AT-7
      status: ATTACKED
      evidence: Tests and code land in the same commit; the red-first run is recorded in this ledger.
      artifacts: [task/ledgers/staging/torture-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: map.md created for fixtures/, fixtures/torture/, tests/torture/ and updated for src/, src/repark_parity/, repark-parity/, tests/, staging ledgers.
      artifacts: [python/repark-parity/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Byte-identity determinism pin runs the committed CLI twice per family; the manifest-reuse pin mutates its inputs and reds.
      artifacts: [python/repark-parity/tests/torture/test_generate_is_deterministic.py]
    - id: AT-10
      status: N/A
      justification: No live-oracle tier belongs to step 1; the recorded oracle is stated in the measured-behavior table above.
DELIVERY_SIGNOFF:
  pr_unit: torture-1
  artifacts_verified:
    ledger: PASS (C-001..C-009)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: step 1 of 5; STATUS untouched per brief
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
