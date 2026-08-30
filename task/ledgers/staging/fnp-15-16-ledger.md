# Unit ledger — FNP-15/16 · register every unreachable and declared-deferred family

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when FNP-15/16 merges, or when
the owner closes the slate row.

**Unit:** FNP-15/16 (honesty: 62 names, no kernels) · **Date:** 2026-08-30 ·
**Executor:** Grok (grok-4.6), Actor · **Branch:** `feat/fnp-15-16` ·
**Base:** `origin/main` at pickup.
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) C-007, C-008
(the unreachable partition and the four D-7 families).
**Design:** [docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md)
§7 rows FNP-15 and FNP-16, §7.1, §8.
**Slate:** [briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md)
(FNP-16 wording rule); [briefs/next-sequence.md](../../../briefs/next-sequence.md) row 1.

**Rubric:** STANDARD (public facade interface; new declared-divergence sections).

**Writable paths:** refusing stubs on both SQL doors and the facade; new FNP-15/16
sections of [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md);
maps in lockstep; this ledger. Closed: `Cargo.toml [patch]`, lockfiles, `.github/`,
STATUS.md (FNP-Z), `briefs/next-sequence.md`, wholesale `__all__` completion and
dispatch-table restructuring (FNP-Z).

This unit builds no kernel. It converts 62 missing names from `AttributeError`
into loud refusing surfaces that name a divergence-registry section.

## Re-count of the §8 roster (2026-08-30)

Measured from [task/fnp-0-census/pyspark-gap.md](../../fnp-0-census/pyspark-gap.md)
F12/F13/F14/F16, not copied from the design's headline counts.

| Family | n | Members |
|---|---|---|
| Sketches (HLL / theta / KLL) | 32 | HLL (4): `hll_sketch_agg`, `hll_sketch_estimate`, `hll_union`, `hll_union_agg`. Theta (7): `theta_difference`, `theta_intersection`, `theta_intersection_agg`, `theta_sketch_agg`, `theta_sketch_estimate`, `theta_union`, `theta_union_agg`. KLL (21): `{kll_merge_agg,kll_sketch_agg,kll_sketch_get_n,kll_sketch_get_quantile,kll_sketch_get_rank,kll_sketch_merge,kll_sketch_to_string}_{bigint,double,float}`. |
| CSV / XML / XPath | 11 | `to_csv`, `to_xml`, `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_number`, `xpath_short`, `xpath_string`. (`from_csv` / `from_xml` / `schema_of_csv` / `schema_of_xml` already refuse as E1 stubs; they are not in this 11.) |
| VARIANT | 8 | `parse_json`, `try_parse_json`, `is_variant_null`, `variant_get`, `try_variant_get`, `schema_of_variant`, `schema_of_variant_agg`, `to_variant_object`. |
| Geospatial | 5 | `st_asbinary`, `st_geogfromwkb`, `st_geomfromwkb`, `st_setsrid`, `st_srid`. |

32 + 11 + 8 + 5 = **56**. FNP-15 adds six unreachable names. **62** total.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Refusal mechanism: a stub reachable from the Spark SQL door, the ANSI SQL door, and the facade raises a loud, specific refusal that names the registry section. Never a silent None. Never a generic `NotImplementedError` without the registry reason. Facade class is `UnsupportedOperationException`; Rust doors use `DataFusionError::NotImplemented` (the existing fold to that class). Follows the G15 / `schema_of_json` declared-divergence pattern: parse-altitude valve on both SQL doors, call-time raise on the facade. | Pin each of the three entry points on at least one FNP-15 name and one FNP-16 name; a control that a live function still plans. | **PROVEN** (62 names: facade, Spark SQL, `repark.sql()`, crate `execute`) |
| C-002 | `java_method` is **unreachable** — it loads a Java class by name and invokes a static method by reflection, which needs a live JVM. Register, do not build. Registry section states that mechanism. | Facade + both SQL doors refuse naming the mechanism; pin red-before as `AttributeError`. | **PROVEN** |
| C-003 | `reflect` is **unreachable** — Spark's `CallMethodViaReflection` spelling of the same JVM reflection. Register, do not build. | Same three-door pin as C-002, distinct name. | **PROVEN** |
| C-004 | `try_reflect` is **unreachable** — `reflect` with exception-to-NULL, still JVM reflection. Register, do not build. | Same three-door pin as C-002, distinct name. | **PROVEN** |
| C-005 | `unwrap_udt` is **unreachable** — Spark `UserDefinedType` unwrap walks the JVM UDT registry; with no JVM there is no UDT system to unwrap from. Register, do not build. | Same three-door pin as C-002, distinct name. | **PROVEN** |
| C-006 | `input_file_block_start` is **unreachable** until `input_file_name` is destubbed — it reads Spark's `InputFileBlockHolder` thread-local, populated by `HadoopRDD`/`FileScanRDD`. DataFusion has no equivalent surface. | Same three-door pin; message names `InputFileBlockHolder` and the `input_file_name` stub. | **PROVEN** |
| C-007 | `input_file_block_length` is **unreachable** by the same `InputFileBlockHolder` mechanism as C-006. | Same three-door pin, distinct name. | **PROVEN** |
| C-008 | FNP-16 sketches family: the 32 HLL/theta/KLL names are **reachable, deferred by cost** (Apache DataSketches byte format; DataFusion `hyperloglog.rs` is a different format). One refusing stub per name; one registry section for the family. | Parametrized pin over all 32 names × three entry points; roster test counts 32. | **PROVEN** |
| C-009 | FNP-16 CSV/XML/XPath family: the 11 names (`to_csv`, `to_xml`, nine `xpath_*`) are **reachable, deferred by cost** (XPath 1.0 matching `javax.xml.xpath`; `datafusion-spark` csv/xml modules are empty). | Parametrized pin over all 11 names × three entry points; roster test counts 11. | **PROVEN** |
| C-010 | FNP-16 VARIANT family: the 8 names are **reachable, deferred by cost** (Spark VARIANT value/metadata encoding; RePark `VariantType` is a shell). | Parametrized pin over all 8 names × three entry points; roster test counts 8. | **PROVEN** |
| C-011 | FNP-16 geospatial family: the 5 names are **reachable, deferred by cost** (GEOGRAPHY/GEOMETRY have no Arrow representation and no vendored WKB codec). | Parametrized pin over all 5 names × three entry points; roster test counts 5. | **PROVEN** |
| C-012 | Registry wording: FNP-15 sections say **unreachable** and state why each name cannot exist here. FNP-16 sections say **reachable, deferred by cost**. Writing "unsupported" as the classification for both fails the unit. `UnsupportedOperationException` remains the facade exception class (PySpark's name for this raise). | A test reads the new registry sections: every FNP-15 heading/body carries "unreachable" and not a cost-deferral claim; every FNP-16 heading/body carries "deferred by cost"; the new sections do not classify both families as "unsupported". | **PROVEN** (six FNP-15 sections; four FNP-16 family sections) |
| C-013 | The declared-absent roster is 56 (32+11+8+5) plus FNP-15's 6, total 62. No silent extra name and no missing §8 name. | A test enumerates the catalog and asserts those counts and the exact member sets. | **PROVEN** |
| C-014 | Docs and maps stay in lockstep: new FNP-15/16 registry sections only (no existing registry row edited); every touched directory's `map.md` updated in the same commit as the file add. | `make check-map-sync` green; the registry test in C-012; map citations. | **PROVEN** |
| C-015 | Gates before done: `make verify`, `make preflight`, full `make py-test`. Real exit codes. | Recorded in the execution record below at close. | **PROVEN** |
| C-016 | This unit exports its 62 names on the facade so `AttributeError` ends for them, including the `repark.spark.sql.functions` re-export path. It does **not** close campaign C-009 for names outside the 62 (`__all__` completion is FNP-Z). | Pins that each of the 62 is a public attribute of `repark.spark.functions` and of `repark.spark.sql.functions`; a control that a still-missing name (outside the 62) remains `AttributeError`. | **PROVEN** (62 names on `functions` and `sql.functions`) |
| C-017 | Native/ANSI existing behaviour is unchanged: a default `SELECT 1` and a live Spark function still plan. The new names refuse; they do not change dialect, arithmetic, or any previously valid query. | Control pins on both doors (default select + one live function). | **PROVEN** |

VERDICT: 17/17 PROVEN, 0 OPEN, 0 REJECTED. A clause flips `PROVEN` only with a
`pins: fnp-15-16/C-NNN` citation from a tracked test docstring or a `map.md` under
`crates/`, `python/`, or `scripts/`.

## Sequence

1. This ledger (grammar-gate clean, verdicts OPEN).
2. Mechanism + FNP-15's six names (red-first per name, registry section each).
3. FNP-16 sketches (32).
4. FNP-16 CSV/XML/XPath (11).
5. FNP-16 VARIANT (8).
6. FNP-16 geospatial (5).
7. Gates. Ledger verdicts flip when the pins exist. Departure `move` is the
   orchestrator's, not this Actor session.

## Disk (AGENTS.md "Resource discipline")

Checked 2026-08-30 at pickup: `/` 467 G free of 1.8 T (74% used). No worktree.
Incremental `target/` reuse. No `cargo clean`. Scoped cleanup: none; this unit
adds stubs, not a rebuild-heavy kernel.

## Execution record (2026-08-30)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make py-test-facade` | 0 (4018 passed, 75 skipped) |
| `make audit` | 0 |
| `make workflows-lint` | 0 |
| `make py-test` | 0 (459 passed) |

`make preflight` as one invocation hit a pre-existing timing flake in
`repark-ta` `p1c_microbench::hour0_bbands_three_vs_one_1e6` (unrelated; retry of
that test alone exited 0). The preflight constituents were then run separately
and each exited 0. pins: fnp-15-16/C-015

## Remediation execution record (2026-08-30)

Critic F-1..F-6 + orchestrator O-1.

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make py-test` | 0 (459 passed) |
| `cargo test -p repark-sql --lib refuse` | 0 (91 passed, includes `execute_refuses_every_armed_declared_name`) |
| `cargo test -p repark-functions declared_refuse` | 0 (7 passed) |
| `cargo test -p repark-spark declared_refuse` | 0 (10 passed) |
| `uv run --package repark pytest python/repark/tests/test_fnp15_16_declared_refuse.py` | 0 (319 passed) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: FNP-15/16
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: 62 names refuse on facade, Spark SQL, and repark.sql() with the registry reason.
      artifacts: [python/repark/tests/test_fnp15_16_declared_refuse.py, crates/repark-functions/src/declared_refuse.rs]
    - id: AT-2
      status: ATTACKED
      evidence: F.expr, sql.functions re-export, and aes_encrypt remaining AttributeError.
      artifacts: [python/repark/tests/test_fnp15_16_declared_refuse.py]
    - id: AT-3
      status: ATTACKED
      evidence: UnsupportedOperationException / NotImplemented, never silent None.
      artifacts: [python/repark/tests/test_fnp15_16_declared_refuse.py]
    - id: AT-4
      status: N/A
      justification: parse-altitude name table and call-time raise; no shared mutable state.
    - id: AT-5
      status: N/A
      justification: no AWS, IAM, or secrets; no SQL built from user input beyond the name table.
    - id: AT-6
      status: ATTACKED
      evidence: FNP-15 unreachable vs FNP-16 reachable deferred by cost, pinned in registry §9.
      artifacts: [python/repark/tests/test_fnp15_16_declared_refuse.py]
    - id: AT-7
      status: N/A
      justification: constant-time name lookup at parse; no new kernel.
    - id: AT-8
      status: ATTACKED
      evidence: each refusal names the function and the FNP-15/16 registry section.
      artifacts: [crates/repark-functions/src/declared_refuse.rs, python/repark/src/repark/spark/functions_declared.py]
    - id: AT-9
      status: ATTACKED
      evidence: per-name per-door pins, family sorted-count tests, split-identity prefix pin.
      artifacts: [python/repark/tests/test_fnp15_16_declared_refuse.py, python/repark/tests/test_functions_split_identity.py]
    - id: AT-10
      status: ATTACKED
      evidence: new registry §9 only; maps lockstep; C-015 recorded with real exit codes.
      artifacts: [python/repark/tests/map.md]
  reattested: []
  complete: true
```
