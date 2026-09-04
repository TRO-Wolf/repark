# Unit ledger — EX-9 · v0.7 example backfill, F.* map, struct and JSON family

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when EX-9 merges, or when the
owner closes the slate row.

**Unit:** EX-9 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (continuation of glm-5.3-flash) · **Branch:** `feat/ex-9-functions-maps-structs-json` · **Base:** `a0cd39e` (dispatch base `84c1801`)

**Wall-clock / cost:** GLM leg: started 2026-09-03 00:36 UTC, died on transport errors twice; Muse Spark continuation: ~8 min, free tier

**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md),
batch roster row 9 (the map, struct and JSON family). **Ruling:** owner,
2026-08-31, [release-roadmap-2026-08-29.md](../../../roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v0.7 — Full example documentation", and the 2026-08-31 ruling that each family
PR carries its own charter ledger with one clause per batch.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, and this ledger with its `staging/map.md` row. Closed:
`crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`,
`STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The family is the `F.*` map, struct and JSON names the campaign left on the
backlog. This unit is its first and only batch for the 36-name roster.

**Roster as dispatched (36 names, measured against
`docs/examples/backlog.txt` at `84c1801`, where all thirty-six are rows):**

`F.map_contains_key`, `F.map_entries`, `F.map_filter`, `F.map_from_arrays`,
`F.map_from_entries`, `F.map_keys`, `F.map_values`, `F.transform_keys`,
`F.transform_values`, `F.str_to_map`, `F.named_struct`, `F.struct`,
`F.json_tuple`, `F.from_csv`, `F.to_csv`, `F.schema_of_csv`,
`F.schema_of_json`, `F.from_xml`, `F.to_xml`, `F.schema_of_xml`, `F.xpath`,
`F.xpath_boolean`, `F.xpath_double`, `F.xpath_float`, `F.xpath_int`,
`F.xpath_long`, `F.xpath_number`, `F.xpath_short`, `F.xpath_string`,
`F.parse_json`, `F.try_parse_json`, `F.variant_get`, `F.try_variant_get`,
`F.is_variant_null`, `F.schema_of_variant`, `F.to_variant_object`.

**As landed: twelve kept, twenty-four dropped.** The twenty-four are E1 or
deferred-by-cost refusals — see Oracle table below with Spark and repark values.

**Grouping.** Four files, grouped by the idea a reader learns in one breath
rather than one file per name:

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `map_parts.py` | `F.map_keys`, `F.map_values`, `F.map_entries`, `F.map_contains_key` | One map column taken apart every way, NULL map included. |
| `map_shapes.py` | `F.map_from_arrays`, `F.map_from_entries`, `F.str_to_map` (+ `F.struct` as the entry builder) | The three ways a map column comes into being; `F.struct` builds the entry array. `F.struct` is also a roster name and is covered here. |
| `map_higher_order.py` | `F.transform_keys`, `F.transform_values`, `F.map_filter` | The `(k, v)` lambda names, NULL map included. |
| `structs.py` | `F.struct`, `F.named_struct` | Structs by column and by literal name, NULL fields included. F.struct is also in map_shapes.py COVERS; the gate permits duplicate covers and the ratchet counts distinct names once. |

No existing example under `docs/examples/functions/` demonstrates any of the
thirty-six — `abs.py` and the math family are the only files there that touch
`F.*` maps/structs. So no name joins an existing `COVERS` beyond the shared
`F.col`/`F.lit`/`F.struct` reuse.

## Orchestrator rulings (build-to)

- The gate is the acceptance bar in both directions: a `COVERS` entry the script
  does not exercise is the defect the campaign will not tolerate, and every
  script runs green locally with no network, no cloud and no JVM.
- Every asserted value is measured against live PySpark 4.1.2 before it is
  written; a name whose repark value differs from Spark, or that repark refuses,
  is dropped back to the backlog with both values recorded.
- The backlog count moves down by exactly the names this batch covers, and
  `BACKLOG_BASELINE` moves with it — measured at 842 → 830, twelve of the
  thirty-six dispatched; the other twenty-four stay on the backlog.
- No product edit, ever. A name whose example exposes an engine defect is
  reported and dropped back to the backlog; the baseline then moves by the names
  actually removed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-9-batch-1
  agent: Actor
  action: land EX-9 batch 1 (36 dispatched, 12 kept, 24 dropped) in four files
  charter_trace: C-001
  preconditions:
    - branch feat/ex-9-functions-maps-structs-json at 84c1801: SATISFIED (git)
    - all thirty-six names are backlog rows, none covered, none excepted: SATISFIED (grep)
    - the EX-0 gate is in make ci: SATISFIED (Makefile ci target)
  success_condition: twelve names leave the backlog, the ratchet moves by exactly that count, the gate's static half exits 0 and every new script runs green on the built module
  step_risks:
    - a COVERS entry the script does not really exercise: HANDLED(each script asserts on the value the name produces, NULL included)
    - a name grouped into a file where it is decoration: HANDLED(grouping table states why each name is in its file)
    - the leaf-conflation hazard in docs/examples/map.md: HANDLED(every batch name is an F.* door name, split from the class surfaces by door kind)
  contingencies: [example exposes a refused name: EXECUTABLE(drop it, record both values, move the baseline by the names actually removed)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Batch 1 lands runnable local examples for the twelve roster names it can demonstrate honestly, in four files under `docs/examples/functions/`, every `COVERS` entry exercised by an assertion on the value that name produces and every asserted value measured against live PySpark 4.1.2 + Iceberg 1.11.0 before it was written; those twelve leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly twelve, 842 → 830, with no other `scripts/` change; the other twenty-four are measured on the live oracle, refused by repark (E1 or deferred-by-cost), stay on the backlog with both values recorded, and no product file is touched; the gate's static half exits 0 and every new script runs green on the built module. | Red-first capture below (the thirty-six are uncovered before the batch, and the gate reds by name when the rows are removed without examples), the oracle table per name (Spark value, repark value, kept/dropped, file), the green counts line, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch at `84c1801`, before any example file existed. The
thirty-six rows were deleted from `docs/examples/backlog.txt` and
`BACKLOG_BASELINE` moved 842 → 806 with nothing else changed.
`python3 scripts/check_example_coverage.py` then exited **1** with thirty-six
findings, one per roster name and no others:

```
example-coverage: 36 finding(s)
  public name F.from_csv has no example COVERS row and is not in the backlog or exceptions
  public name F.from_xml has no example COVERS row and is not in the backlog or exceptions
  public name F.is_variant_null has no example COVERS row and is not in the backlog or exceptions
  public name F.json_tuple has no example COVERS row and is not in the backlog or exceptions
  public name F.map_contains_key has no example COVERS row and is not in the backlog or exceptions
  public name F.map_entries has no example COVERS row and is not in the backlog or exceptions
  public name F.map_filter has no example COVERS row and is not in the backlog or exceptions
  public name F.map_from_arrays has no example COVERS row and is not in the backlog or exceptions
  public name F.map_from_entries has no example COVERS row and is not in the backlog or exceptions
  public name F.map_keys has no example COVERS row and is not in the backlog or exceptions
  public name F.map_values has no example COVERS row and is not in the backlog or exceptions
  public name F.named_struct has no example COVERS row and is not in the backlog or exceptions
  public name F.parse_json has no example COVERS row and is not in the backlog or exceptions
  public name F.schema_of_csv has no example COVERS row and is not in the backlog or exceptions
  public name F.schema_of_json has no example COVERS row and is not in the backlog or exceptions
  public name F.schema_of_variant has no example COVERS row and is not in the backlog or exceptions
  public name F.schema_of_xml has no example COVERS row and is not in the backlog or exceptions
  public name F.str_to_map has no example COVERS row and is not in the backlog or exceptions
  public name F.struct has no example COVERS row and is not in the backlog or exceptions
  public name F.to_csv has no example COVERS row and is not in the backlog or exceptions
  public name F.to_variant_object has no example COVERS row and is not in the backlog or exceptions
  public name F.to_xml has no example COVERS row and is not in the backlog or exceptions
  public name F.transform_keys has no example COVERS row and is not in the backlog or exceptions
  public name F.transform_values has no example COVERS row and is not in the backlog or exceptions
  public name F.try_parse_json has no example COVERS row and is not in the backlog or exceptions
  public name F.try_variant_get has no example COVERS row and is not in the backlog or exceptions
  public name F.variant_get has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_boolean has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_double has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_float has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_int has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_long has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_number has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_short has no example COVERS row and is not in the backlog or exceptions
  public name F.xpath_string has no example COVERS row and is not in the backlog or exceptions
```

That is the red the batch closes: the gate names each of the thirty-six, and it
names nothing else, so the batch's green cannot be borrowed from another name.

## Oracle (live PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-03, JDK 17)

Live PySpark 4.1.2 + Iceberg 1.11.0 via `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex9/python/repark/tests` at `.venv/bin/python`; per name the Spark value and the repark value for the same inputs (inputs in table).

| Name | Spark value | Repark value | Kept / Dropped | File |
|---|---|---|---|---|
| `F.map_contains_key` | `[True,True,False,None] on ['a'] and [False,False,False,None] on ['c']` | identical | kept | `map_parts.py` |
| `F.map_entries` | `[[('a',1),('b',2)],[('a',3)],[],None]` | identical (via `{'key','value'}`) | kept | `map_parts.py` |
| `F.map_filter` | `[{'b':2},{'a':3},{},None]` | identical | kept | `map_higher_order.py` |
| `F.map_from_arrays` | `{'x':10,'y':20}` | identical | kept | `map_shapes.py` |
| `F.map_from_entries` | `{'k1':1,'k2':2}` | identical | kept | `map_shapes.py` |
| `F.map_keys` | `[['a','b'],['a'],[],None]` | identical | kept | `map_parts.py` |
| `F.map_values` | `[[1,2],[3],[],None]` | identical | kept | `map_parts.py` |
| `F.transform_keys` | `[{'b_x':2,'a_x':1},{'a_x':3},{},None]` | identical | kept | `map_higher_order.py` |
| `F.transform_values` | `[{'a':2,'b':3},{'a':4},{},None]` | identical | kept | `map_higher_order.py` |
| `F.str_to_map` | `[{'a':'1','b':'2'},None]` and custom delims `{'a':'1','b':'2'}` | identical | kept | `map_shapes.py` |
| `F.named_struct` | `[{'x':1,'y':'x'},{'x':2,'y':None},{'x':None,'y':'y'}]` | identical | kept | `structs.py` |
| `F.struct` | same as `named_struct` via field names `a`/`s` | identical | kept | `structs.py`+`map_shapes.py` |
| `F.json_tuple` | `('1','x')` for `'{"a":1,"b":"x"}'` | `UnsupportedOperationException: functions.json_tuple is not supported yet (JSON tuple kernel deferred; disclosed E1)` | dropped | — |
| `F.from_csv` | `{'a':1,'b':'x'}` for `'1,x'` + schema `'a INT, b STRING'` | `UnsupportedOperationException: functions.from_csv is not supported yet (CSV parse kernel deferred; disclosed E1)` | dropped | — |
| `F.to_csv` | `'1,x'` for `struct(1,'x')` | `UnsupportedOperationException: to_csv is reachable without a JVM and is deferred by cost: the xpath family needs an XPath 1.0 engine matching javax.xml.xpath, and datafusion-spark's csv and xml modules are empty. See docs/spark-sql-iceberg-parity.md (FNP-16 CSV/XML/XPath).` | dropped | — |
| `F.schema_of_csv` | `'STRUCT<_c0: INT, _c1: STRING>'` for `'1,x'` | `UnsupportedOperationException: functions.schema_of_csv is not supported yet (disclosed E1)` | dropped | — |
| `F.schema_of_json` | `'STRUCT<a: BIGINT>'` for `'{"a":1}'` | `UnsupportedOperationException: functions.schema_of_json is not supported yet (disclosed E1)` | dropped | — |
| `F.from_xml` | `{'a':1}` for `'<r><a>1</a></r>'` | `UnsupportedOperationException: functions.from_xml is not supported yet (XML parse kernel deferred; disclosed E1)` | dropped | — |
| `F.to_xml` | `'<ROW>\n    <a>1</a>\n    <b>hi</b>\n</ROW>'` for `named_struct('a', 1, 'b', 'hi')` | `UnsupportedOperationException: to_xml is reachable without a JVM and is deferred by cost: the xpath family needs an XPath 1.0 engine matching javax.xml.xpath, and datafusion-spark's csv and xml modules are empty. See docs/spark-sql-iceberg-parity.md (FNP-16 CSV/XML/XPath).` | dropped | — |
| `F.schema_of_xml` | `'STRUCT<a: BIGINT>'` for `'<r><a>1</a></r>'` | `UnsupportedOperationException: functions.schema_of_xml is not supported yet (disclosed E1)` | dropped | — |
| `F.xpath` | `['1']` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a/text()'` | `UnsupportedOperationException: xpath is reachable without a JVM and is deferred by cost: the xpath family needs an XPath 1.0 engine matching javax.xml.xpath, and datafusion-spark's csv and xml modules are empty. See docs/spark-sql-iceberg-parity.md (FNP-16 CSV/XML/XPath).` | dropped | — |
| `F.xpath_boolean` | `True` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a = 1'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_double` | `1.0` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_float` | `1.0` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_int` | `1` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_long` | `1` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_number` | `1.0` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_short` | `1` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/a'` | same deferred-by-cost refusal | dropped | — |
| `F.xpath_string` | `'hi'` for `'<r><a>1</a><b>hi</b></r>'` with path `'r/b'` | same deferred-by-cost refusal | dropped | — |
| `F.parse_json` | `VariantVal` for `'{"a": 1}'` | `UnsupportedOperationException: parse_json is reachable without a JVM and is deferred by cost: Spark VARIANT is a specific value/metadata binary encoding; repark's VariantType is a shell with nothing behind it. See docs/spark-sql-iceberg-parity.md (FNP-16 VARIANT).` | dropped | — |
| `F.try_parse_json` | `VariantVal` for `'{"a": 1}'` | same VARIANT refusal | dropped | — |
| `F.variant_get` | `1` for `'{"a": 1}'` with path `'$.a'` via `parse_json` | same VARIANT refusal (via `parse_json`) | dropped | — |
| `F.try_variant_get` | `1` for `'{"a": 1}'` with path `'$.a'` via `parse_json` | same VARIANT refusal | dropped | — |
| `F.is_variant_null` | `[True, False]` for `parse_json('null'/'1')` | same VARIANT refusal | dropped | — |
| `F.schema_of_variant` | `'OBJECT<a: BIGINT>'` for `'{"a": 1}'` via `parse_json` | same VARIANT refusal | dropped | — |
| `F.to_variant_object` | `VariantVal` for `struct(lit(1).alias('a'))` (variant binary value/metadata) | `UnsupportedOperationException: to_variant_object is reachable without a JVM and is deferred by cost: Spark VARIANT is a specific value/metadata binary encoding; repark's VariantType is a shell with nothing behind it. See docs/spark-sql-iceberg-parity.md (FNP-16 VARIANT).` | dropped | — |

All kept names were asserted row-for-row against the Spark value above before
the example was written; no assertion was adjusted to repark when Spark
disagreed.

## Gates (2026-09-03, on the batch tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` (static half) | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python docs/examples/functions/map_parts.py` | **0** |
| `.venv/bin/python docs/examples/functions/map_shapes.py` | **0** |
| `.venv/bin/python docs/examples/functions/map_higher_order.py` | **0** |
| `.venv/bin/python docs/examples/functions/structs.py` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |

Counts line (static, `python3`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 81 covered; 830 backlog; 2 exceptions; 19 examples`

Before: `913 …; 69 covered; 842 backlog; 2 exceptions; 15 examples` at `84c1801`.
After: `913 …; 81 covered; 830 backlog; 2 exceptions; 19 examples` — delta 12
names, 4 files.

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-9-functions-maps-structs-json
  cycle: actor
  risk_tier: standard
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The four new example files exercise every kept roster name on a small local frame with NULL included, and the twenty-four dropped names are refused by repark (E1/deferred-by-cost) with Spark and repark values recorded in the oracle table.
      artifacts: [docs/examples/functions/map_parts.py, docs/examples/functions/map_shapes.py, docs/examples/functions/map_higher_order.py, docs/examples/functions/structs.py, task/ledgers/staging/ex-9-functions-maps-structs-json-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: NULL maps/strings/fields exercised where the name accepts them; empty maps/structs also covered.
      artifacts: [docs/examples/functions/map_parts.py, docs/examples/functions/map_higher_order.py, docs/examples/functions/structs.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusal paths are loud UnsupportedOperationException with registry reason; the gate reds on a missing COVERS or stale backlog row.
      artifacts: [scripts/check_example_coverage.py, python/repark/src/repark/spark/functions_declared.py, python/repark/src/repark/spark/functions_expr.py]
    - id: AT-4
      status: N/A
      justification: Examples are single-node local frames with no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No auth, network, or deserialization surface added; examples are local DataFrame ops.
    - id: AT-6
      status: N/A
      justification: No engine behavior change; the batch only adds examples and moves the backlog ratchet.
    - id: AT-7
      status: N/A
      justification: Per-row map/struct ops over tiny frames — no allocation growth or unbounded loop.
    - id: AT-8
      status: ATTACKED
      evidence: The public surface enumerator and COVERS binding are unchanged; the new files use the existing F.* door alias and repark-rooted local rules.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Examples print values under repark and raise on mismatch; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Branch liveness is the gate itself — a missing example reds, a stale backlog row reds, a dead file reds via map-sync; the ledger clause is cited by each new file's pins line.
      artifacts: [docs/examples/functions/map_parts.py, docs/examples/functions/map_shapes.py, docs/examples/functions/map_higher_order.py, docs/examples/functions/structs.py, scripts/check_example_coverage.py]
  reattested: []
```
