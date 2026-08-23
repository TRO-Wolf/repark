# Unit ledger — Y-6 / H-2 gap G10: facade-boundary container-shape corpus

**Unit:** H-2 gap **G10** of the V2 Engine Hardening campaign · **Date:** 2026-08-12 ·
**Lane:** overnight conductor #4 Y-6 · **Worktree:** `/tmp/grok-y6` · **Branch:**
`grok/y6-g10-boundary-shapes` · **Base freeze:** `origin/main` `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7`

**This ledger covers §0 recon + 10 boundary-shape rows.** Critic: `octo` ×2 + `claims_critic=true`.
Out of scope: census cohorts/allowlists (A11, ruled untouched); engine fixes; X-5 VALUES
families; `_live_parity.py` / live pins; registry file; ML / pandas-udf.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Boundary-shape corpus | [`python/repark/tests/test_boundary_shapes_parity.py`](../../../../python/repark/tests/test_boundary_shapes_parity.py) | 10 rows + semantics-gated budget + classifier |
| Record driver | [`python/repark/tests/_record_boundary_shapes_goldens.py`](../../../../python/repark/tests/_record_boundary_shapes_goldens.py) | re-derive Spark halves; `--emit` |
| Maps | `python/repark/tests/map.md`, `task/map.md` | lockstep navigation |
| This ledger | `task/y6-boundary-shapes-ledger.md` | linked from [`task/map.md`](../../../map.md) |

### 1.1 §0 home (recorded choice)

`python/repark/tests/map.md` already lists `test_interchange_parity.py` (G-INT primitives,
inline goldens, no `ROWS` / record driver). X-5 `test_nested_container_parity.py` is
engine VALUES via createDataFrame tuples / SQL. **Opened a sibling**
`test_boundary_shapes_parity.py` so G10 can follow the corpus mold (importable `ROWS`,
record driver, CONVERGED classifier) without bloating G-INT or duplicating X-5 families.

### 1.2 Budget (met; cycle-1 pin fix)

Floors are **semantics-gated** so a CONTROL/equality cannot satisfy a disclosure family.

| Bucket | Budget | Landed |
|---|---|---|
| G10 differential rows | 8–10 | **10** (6 equality + 4 disclosure) |
| Equalities (min) | ≥1 | **6** |
| Disclosures (min) | ≥3 | **4** |
| typed-Map (recipe `out_map` / Arrow map + `map_topandas_*` disclosure) | ≥1 disclosure | **1** (`map_topandas_cell_shape`) |
| `*struct_*` both directions | ≥1 in + ≥1 out | **2** |
| `*binary_*` both directions | ≥1 in + ≥1 out | **2** |
| `*array_*` **and** item-vs-element disclosure | ≥2 + `array_from_pandas_object` | **3** |
| `*pandas_timestamp_unit_*` **and** us ingest disclosure | ≥1 disclosure | **2** (us disc + ns inbound twin) |
| `*_from_pandas_*` (in) | ≥2; glob = every inbound row | **6** |
| `*_topandas_*` (out) | ≥2 | **4** |

### 1.3 Row inventory (10)

**Equalities (6)** — repark == Spark on the named surface:

| # | Name | Surface | Intent |
|---|---|---|---|
| 1 | `binary_topandas_bytes_shape` | pandas | OUT typed binary → object + bytes cells |
| 2 | `array_topandas_ndarray_shape` | pandas | OUT typed array → object + ndarray cells |
| 3 | `pandas_timestamp_unit_from_pandas_ns` | pandas | IN `datetime64[ns]` → toPandas `[ns]` both engines |
| 4 | `array_from_pandas_arrowdtype` | arrow | IN pandas ArrowDtype list keeps `element` |
| 5 | `binary_from_pandas_bytes` | arrow | IN pandas bytes → `binary` |
| 6 | `struct_from_pandas_object_dict` | arrow | IN pandas dicts infer struct (not a map) |

**Disclosures (4)** — both halves pinned; type-sensitive compare so `20 == 20.0` cannot
launder the struct row:

| # | Name | Spark half | repark half |
|---|---|---|---|
| 7 | `map_topandas_cell_shape` | object + **dict** cells | object + **list-of-pairs** cells |
| 8 | `struct_topandas_cell_shape` | dict; Long field row-1 is Python **float 20.0** (recorded live-Spark fact; row-0 is int 10) | dict; Long field stays **int 20** |
| 9 | `array_from_pandas_object` | `list<element: int64>` | `list<item: int64>` |
| 10 | `pandas_timestamp_unit_from_pandas_us` | toPandas `datetime64[ns]` | toPandas `datetime64[us]` |

Replaced (cycle-1): `pandas_timestamp_unit_sql_cast_ns` (wrong-door ns equality) by
`pandas_timestamp_unit_from_pandas_ns`. Renamed `binary_from_pandas` →
`binary_from_pandas_bytes` (inbound glob) and `map_from_pandas_object_dict` →
`struct_from_pandas_object_dict` (not a typed Map).

### 1.4 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  python python/repark/tests/_record_boundary_shapes_goldens.py
```

Authoring record: **`record mode: 10 rows re-derived, 0 mismatch(es)`** (2026-08-12,
zulu-17, PySpark 4.1.2, `local[2]`, ANSI on, shuffle=2, `session.timeZone=UTC`,
`arrow.pyspark.enabled=true`). Marker `MARKER=y6-g10-boundary`.

Cycle-1 re-record (renames + inbound-ns twin): same driver, imports `ROWS` from this
module, **`record mode: 10 rows re-derived, 0 mismatch(es)`**. Serialized under
`/tmp/grok-jvm-record.lock` marker `MARKER=y6-g10-fix` (verified before `--emit` and
before the 10/0 pass). `--emit` printed every Spark half; ns twin pasted from that
emit (equality, `repark=None`). **No lock removal.**

---

## 2. §0 live-Spark probe (facts, not hopes)

Probed live PySpark 4.1.2 (zulu-17, arrow-on, UTC) then the same recipes on repark.

| Probe | Spark 4.1.2 | repark |
|---|---|---|
| map `toPandas` | object, cells `dict` | object, cells `list` of pairs |
| struct `toPandas` | object, cells `dict`; row-1 `x` is `float` 20.0 | object, cells `dict`; `x` is `int` 20 |
| array `toPandas` | object, cells `ndarray` | same |
| binary `toPandas` | object, cells `bytes` | same |
| SQL CAST ts `toPandas` | `datetime64[ns]`, wall 12:30 | same unit + wall |
| IN pandas `datetime64[us]` → `toPandas` | `datetime64[ns]` | `datetime64[us]` |
| IN pandas object-list → Arrow | `list<element: int64>` | `list<item: int64>` |
| IN pandas ArrowDtype list | `list<element: int64>` | same |
| IN pandas bytes | `binary` | same |
| IN pandas object-dict (no schema) | struct over key union | same |
| IN pandas + `MapType` schema | map (arrow-opt falls back) | `ArrowNotImplementedError` cast struct→map |
| IN Arrow `Table` | accepted | `PySparkTypeError` (not an API) |
| IN polars list | Spark `CANNOT_INFER_TYPE_FOR_FIELD _s` | `large_list<item: int64>` |

---

## 3. Decisions, with rationale

**D-Y6-1 — Sibling module, not an interchange append.** G-INT is an inline-golden primitive
matrix; G10 needs the corpus mold. X-5 stays the VALUES home.

**D-Y6-2 — Dual surface (`arrow` / `pandas`).** G10 is an interchange-shape gap. Arrow rows
use `assert_frames_equal` (value + type + nullability). Pandas rows pin `str(dtype)` +
`type(cell).__name__` + type-sensitive normalized values. A pandas-only equality cannot
re-pin X-5's list field-name disclosure.

**D-Y6-3 — Type-sensitive pandas values.** Python `20 == 20.0` is true. Without a
`type is type` compare, `struct_topandas_cell_shape` collapsed into a false equality.
P2 proves `==` agrees and the helper still reds.

**D-Y6-4 — `array_from_pandas_object` is a pandas-ingest sibling, not an X-5 duplicate.**
Same type class (`item` vs `element`) as G18-1, different entry (pandas object-list, not
createDataFrame tuples). Name-gated `array_*`; ledger cites G18.

**D-Y6-5 — Census / live pins / registry untouched (A11).** New facade node ids are a
**ledger finding** for a future census-additions ruling, not tonight's allowlist edit.
Zero-diff proof in §4.6.

**D-Y6-6 — Ledger-only leftovers (not manufactured rows).** (a) repark refuses
`createDataFrame(pa.Table)` — Spark accepts; inbound is pandas. (b) repark cannot apply a
`MapType` schema onto pandas object-dicts (struct→map cast). (c) Spark `createDataFrame`
from polars lists failed in this probe; not a stable oracle. (d) naive Python-datetime
createDataFrame is TZ-environment-sensitive on Spark; recipes use SQL CAST or pandas
Timestamps.

**D-Y6-7 — Registry rows stay in this ledger.** Ready-to-paste §6 only.

**D-Y6-8 — Semantics-gated floors (cycle-1 critic).** A `map_` / `pandas_timestamp_unit_*`
name-count is not enough: the map floor requires a typed-Map disclosure (`out_map` /
`map_topandas_*`); the timestamp-unit floor requires the inbound-us **disclosure** plus
an inbound-ns twin on the same `createDataFrame(pandas)` path. SQL CAST ns outbound was
the wrong-kind control (L-002) and was replaced (charter stays 8–10). Map pair order is
order-insensitive like X-5. Struct Spark mixed `10`/`20.0` is a re-recorded live fact.

---

## 4. Gate evidence

### 4.1 Record mode

```
pyspark 4.1.2 matches record extra pin
[G10] map_topandas_cell_shape PASS
[G10] struct_topandas_cell_shape PASS
[G10] binary_topandas_bytes_shape PASS
[G10] array_topandas_ndarray_shape PASS
[G10] array_from_pandas_object PASS
[G10] array_from_pandas_arrowdtype PASS
[G10] binary_from_pandas_bytes PASS
[G10] struct_from_pandas_object_dict PASS
[G10] pandas_timestamp_unit_from_pandas_us PASS
[G10] pandas_timestamp_unit_from_pandas_ns PASS

record mode: 10 rows re-derived, 0 mismatch(es)
```

(cycle-1 re-record, marker `y6-g10-fix`; Spark halves imported via `ROWS` / `run_row`.)

### 4.2 Corpus suite

```
python/repark/tests/test_boundary_shapes_parity.py — 14 passed
(10 differential + 1 budget + 2 classifier + 1 map pair-order)
```

### 4.3 Ruff

```
uvx ruff@0.15.22 check <touched py> — All checks passed
```

### 4.4 Oracle re-derivation spot-check (≥3 rows)

Full driver re-run: all 10 PASS bit-for-bit against committed goldens (sampled among them:
`map_topandas_cell_shape`, `pandas_timestamp_unit_from_pandas_us`,
`array_from_pandas_object`).

### 4.5 `make verify` / `make preflight`

Authoring (cycle 0): both exit 0 (`/tmp/y6-verify.log`, `/tmp/y6-preflight.log`).

Cycle-1 pin fix:

```
make verify > /tmp/y6-fix-verify.log 2>&1; echo $?
# verify_exit=0

python/repark/tests/test_boundary_shapes_parity.py — 14 passed

make preflight > /tmp/y6-fix-preflight.log 2>&1; echo $?
# preflight_exit=0
# facade pytest: 2836 passed, 71 skipped (was 2835; +1 pair-order pin)
```

### 4.6 Census zero-diff (A11)

Replayable against the freeze (CL-002: worktree-vs-index is not the proof):

```
git diff --name-only a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7..HEAD -- \
  task/port/added-python-tests.txt task/port/deferred-python-tests.txt \
  task/census docs/port/census.md python/repark/tests/_live_parity.py \
  docs/spark-sql-iceberg-parity.md
# (empty)
```

Census files, `_live_parity.py`, and the registry are not in the change set. New node
ids are a finding for a future additions-ledger ruling (D-Y6-5), not a silent census
extension.

---

## 5. Provocations

### 5.1 Verbatim (2026-08-12)

```
=== P1: equality golden value corrupted (binary) ===
RED as expected: FrameMismatchError: pandas value mismatch: actual=(('id', (1, 2, 3)),
('blob', (b'hello', b'\x00\x01\xff', None))) expected=(('id', (1, 2, 3)),
('blob', (b'NOPE', b'\x00\x01\xff', None)))

=== P2: type-sensitive 20 vs 20.0 on struct disclosure halves ===
spark cell {'x': 20.0, 'y': 'b'} <class 'dict'>
repark cell {'x': 20, 'y': 'b'} <class 'dict'>
Python == on those dicts: True
RED as expected (type-sensitive): pandas value mismatch: actual=…{'x': 20.0}…
expected=…{'x': 20}…

=== P3: classifier CONVERGED ===
RED as expected: map_topandas_cell_shape: repark and Spark have CONVERGED - repark now
produces the RECORDED SPARK output, so this disclosure is stale. Do not delete the row:
flip it to an equality row (repark=None) and record the convergence.

=== P4: classifier regression ===
RED as expected: map_topandas_cell_shape: repark moved OFF its pinned disclosure and does
NOT match the recorded Spark golden either - this is a regression, not a convergence.
Re-derive both halves in record mode
(python/repark/tests/_record_boundary_shapes_goldens.py --emit).

=== P5: budget pin — struct family emptied ===
BUDGET RED as expected: G10 must keep the struct family (≥1 rows named *struct_*); got 0
restored 10 rows
```

P3/P4 are also committed tests (`test_disclosure_classifier_converged_arm` /
`test_disclosure_classifier_regression_arm`).

---

## 6. Ready-to-paste divergence-registry rows

> **Do not edit `docs/spark-sql-iceberg-parity.md` in this unit.** Orchestrator pastes after merge.

- **repark** — `toPandas` of a typed map column yields object-dtype **list-of-pairs** cells
  (raw Arrow map → pandas). A typed struct Long field stays Python `int`. Inbound pandas
  `datetime64[us]` round-trips as `datetime64[us]`. Inbound pandas object-list arrays export
  Arrow `list<item: int64>`.
- **Apache Spark 4.1.2** — the same recipes: map cells are **dict**; struct Long on the
  recorded second row lands as Python **float** `20.0` (stable under arrow-on toPandas);
  inbound `datetime64[us]` exports as `datetime64[ns]`; inbound object-list arrays export
  `list<element: int64>`. *(oracle: recorded.)*
- **Pin**
  - `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[map_topandas_cell_shape]`
  - `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[struct_topandas_cell_shape]`
  - `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[array_from_pandas_object]`
  - `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[pandas_timestamp_unit_from_pandas_us]`
- **Rationale** — interchange SHAPE disclosure class (pandas cell Python type / timestamp
  unit / Arrow list field name on the pandas ingest path). Values otherwise match.
  Unlocked by X-5's nested comparator; fix is G10 follow-on, not silent absorption.

---

## 7. Critic record (octo ×2 + claims_critic)

### Cycle 1 — Actor self-check vs corpus-failure taxonomy (CP-1…CP-12)

| CP | Finding | Disposition |
|---|---|---|
| CP-1 Dead classifier | CONVERGED + regression arms committed + P3/P4 | PASS |
| CP-2 Tautological coverage | Families name-gated `*map_*` / `*struct_*` / `*binary_*` / `*array_*` / `*pandas_timestamp_unit_*` / `*_from_pandas_*` / `*_topandas_*`/`*_sql_cast_*` | PASS |
| CP-3 Simulated-not-executed | Recipes call createDataFrame / sql / toPandas / to_arrow for real; driver runs same `run_row` | PASS |
| CP-4 Vacuous refuse | No refuse rows (Arrow-Table / MapType schema leftovers are ledger-only) | N/A |
| CP-5 Non-paste-true handoff | §6 uses paste-true bullets + fully-qualified node ids (collect-only verified) | PASS |
| CP-6 Message-format defect | Guidance strings are plain / f-strings; P3 printed CONVERGED | PASS |
| CP-7 Golden drift | Full record re-derive 10/0 PASS | PASS |
| CP-8 Oracle-pin tautology | Driver derives pyspark pin from `python/repark-parity/pyproject.toml` record extra | PASS |
| CP-9 Landmine non-vacuity | N/A | N/A |
| CP-10 Budget drift | Budget pin asserts min/max + family floors | PASS |
| CP-11 Entry-point blind spot | Claim scoped to facade interchange; both in (createDataFrame pandas) and out (toPandas / to_arrow) | PASS |
| CP-12 Leftover state | Session stop + `_reset_active_session_for_tests` after each row | PASS |

### Cycle 2 — Claims critic (claims_critic=true)

| Claim | Evidence | Verdict |
|---|---|---|
| 8–10 rows | 10 rows; budget pin 8–10 | PROVEN |
| Coverage floors name-gated | budget test | PROVEN |
| Spark halves recorded | driver 10/0; imports `ROWS` | PROVEN |
| Census zero-diff | §4.6; allowlists not in diff | PROVEN |
| X-5 families not duplicated | no collect_list / tuple-roundtrip VALUES rows | PROVEN |
| Registry / `_live_parity.py` not edited | not in the change set | PROVEN |

### Cycle 2 / hats (spawn unavailable — sequential context-break; weaker independence)

**Critic-1 (quality / test adequacy).** Attacked: corpus mold vs X-5/TVL exemplars; budget
name-gates; classifier reachability; type-sensitive `20` vs `20.0`; record driver imports
`ROWS`. No hollow pin. Verdict: CLEAN.

**Critic-2 (safety).** Attacked: no AWS; no secrets; JVM lock marker-verify; no
`_live_parity` / registry / census edits; record driver never rewrites goldens. Verdict:
CLEAN (N/A atomicity — test-only).

**Critic-3 (logic).** Attacked: pandas `==` laundering `20.0`; map dict vs list-of-pairs;
timestamp unit ns vs us; inbound list field name; equality controls cannot satisfy
`pandas_timestamp_unit_*`. Verdict: CLEAN after D-Y6-3.

**Critic-4 (claims).** Attacked: 10-row count vs `ROWS`; §6 node ids vs `--collect-only`;
record 10/0 vs driver; census zero-diff; identity after commit (CL-IDENTITY, §11).
Verdict: CLEAN.

---

## 11. Identity (CL-IDENTITY)

```
git log -1 --format='%ae'
64240326+TRO-Wolf@users.noreply.github.com
```

Byte-exact. Trailer `Authored-By: Grok (grok-4.5) <noreply@x.ai>` (live session model-id).
Hygiene two-pass (diff + commit metadata) includes `tro-wolf.local` — clean. Hooks fired
on commit (crate-dag / lib-rs / lib-py / manifest / taplo / typos).

---

## 8. Deviations from brief

1. **10 rows (budget 8–10)** — full matrix + both directions where the API allows.
   Cycle-1 replaced SQL CAST ns with inbound ns (same count; right entry point).
2. **No engine production source edits** — disclosures are honest shape pins.
3. **Registry / live pins / census files not edited** — paste-true §6 + D-Y6-5.
4. **Arrow-Table inbound not rowed** — repark has no such API; leftover (a).
5. **Census node-id additions not listed in `added-python-tests.txt`** — A11 forbids the
   edit; finding for morning.
6. **Map inbound not rowed** — `MapType` schema on pandas dicts is leftover (b); the
   inbound object-dict row is named `struct_*` because both engines infer a struct.

## 12. Cycle-1 critic dispositions (octo Half-B)

| ID | Sev | Disposition |
|---|---|---|
| Q-001 / L-001 | S1 | **FIXED** — timestamp-unit floor requires the inbound-us **disclosure**; tautological `all(name in name)` removed. Dropping `pandas_timestamp_unit_from_pandas_us` now reds the budget pin. |
| Q-002 / L-003 | S1 | **FIXED** — map floor is typed-Map (`out_map` / Arrow map + `map_topandas_*` disclosure). `map_from_pandas_object_dict` renamed `struct_from_pandas_object_dict` so a struct-inference equality cannot green the map family. |
| L-002 | S1 | **FIXED** — inbound `datetime64[ns]` twin (`pandas_timestamp_unit_from_pandas_ns`) on the same createDataFrame(pandas) path; recorded equality. SQL CAST ns control dropped (charter 8–10). Ingest-always-us reds this equality. Values on the ns twin carry microseconds; unit lives on `str(dtype)`. |
| L-004 | S1 | **FIXED** — per-family direction: struct/binary/array have inbound `_from_pandas_*` and outbound `_topandas_*`. Array floor requires `array_from_pandas_object` disclosure. `binary_from_pandas` renamed `binary_from_pandas_bytes`. Map IN stays leftover (D-Y6-6 b). |
| CL-001 | S2 | **FIXED** — inbound glob now matches all 6 inbound rows (was 4 vs claimed 5). |
| Q-003 | S2 | **FIXED** — `_values_match` treats tuple-of-pairs as order-insensitive (X-5 key-sort). `test_pandas_map_cell_pairs_are_order_insensitive`. |
| L-005 | S2 | **FIXED** — re-recorded; live Spark **is** mixed int `10` / float `20.0`. Note + ledger state the recorded fact. Goldens not hand-edited. |
| CL-002 | S2 | **FIXED** — §4.6 now quotes `git diff --name-only a985edf7..HEAD --` (plus `_live_parity.py` / registry). |
| CL-003 | S2 | **FIXED** — leftover cardinality is **4** here and in COMPLETE: (a) Arrow Table ingest, (b) MapType schema, (c) Spark+polars list, (d) naive Python-datetime TZ. |

---

## 9. Diff size / sequencing

- One PR (conductor: no stacked PRs). Test-only + maps + ledger.

## 10. JVM lock

- Acquired `/tmp/grok-jvm-record.lock` with `MARKER=y6-g10-boundary` (atomic noclobber).
- Marker verified before record and before this write.
- Standing containerized `SparkSubmit` (HiveThriftServer2) ignored.
- **No lock removal** this lane (marker is ours; leave for the operator / wave close).
