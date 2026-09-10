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
