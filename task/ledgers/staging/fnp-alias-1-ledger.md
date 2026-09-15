# Charter ledger — FNP-ALIAS-1 · PySpark alias names over existing kernels

**Date:** 2026-09-15 · **Branch:** `feat/fnp-alias-1` · **Base:** `origin/main` `acbb6a8e` ·
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Registry:** `FNP-ALIAS-DEGREES-1` BACKLOG → **FIXED** (docs/spark-sql-iceberg-parity.md §7).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign's shape rule: a name is done when it is on
`repark.spark.functions` and answers PySpark 4.1.2 for its argument shapes, NULLs and result
types, with pins. Eight PySpark names are absent and one pair of existing names is wrong. The
oracle (`python/repark/tests/fnp_alias_1_spark_oracle.json`) is live PySpark 4.1.2, recorded
2026-09-14 by the orchestrator (98 cells, 14 signatures).

**Not in this unit:** the SQL-door column naming (`sum(DISTINCT t.v)`,
`shiftleft(t.i,Int64(1))`) and the VALUES grouping-key nullability (D-2, run 15c's SQL-door
work); `approx_count_distinct` result nullability (D-3, registry row BL-18).

**Moved out of this unit by ruling D-6 (orchestrator, 2026-09-15); the round-1 finding stands as the record:** `sum_distinct` /
`sumDistinct` cannot be delivered under the card's "no Rust edits" fence. The card's directive —
"follow how `count_distinct`/`countDistinct` builds a DISTINCT aggregate in the facade" —
presupposes a generic native distinct-aggregate builder; the facade's only distinct builders are
count-specific (`PyColumn.count_aggregate(columns, distinct)`) and collect-specific
(`collect_aggregate(argument, distinct)`); `PyColumn.aggregate(kind, ignore_nulls)` has no
distinct modifier, and no other native surface builds one. `test_functions_c.py`'s deferred
census records the same gap ("it is not a kernel — DataFusion spells it `sum(DISTINCT x)`, a
modifier on the aggregate call, so it needs the facade's DISTINCT path rather than a dispatch
arm"). Wiring one needs a Rust arm (banned here). The two names stay on the deferred census and
their oracle cells stay unpinned this round; the delivered six names are pinned. `dataframe/**`
is also fenced by D-1, so the grouped-agg SQL fallback that could have carried a SQL-spelled
DISTINCT aggregate is not reachable either.

## Decisions (orchestrator rulings, binding)

- **D-1** Names live in the functions module family: added where the siblings live
  (`functions_math.py`, `functions_bitwise.py`, `functions_agg.py` or `functions.py`), exported
  in `__all__` like the siblings. No edits to `dataframe/**`, `column.py`, `session/**`,
  `catalog.py`, `types.py`, or any Rust.
- **D-2** SQL-door column naming (`sum(DISTINCT t.v)`, `shiftleft(t.i,Int64(1))`) and the VALUES
  grouping-key nullability are run 15c's SQL-door work: SQL-door cells are pinned on values and
  types only, and the name difference is recorded as an out-of-scope observation, not a fix.
- **D-3** `approx_count_distinct` result nullability (RePark nullable, Spark non-null) is
  registry row BL-18 — left as is; values and type are pinned, nullability is not.
- **D-4** Pins: `python/repark/tests/test_fnp_alias_1.py` — parametrized over the Python-door
  cells (column name, type, rows; nullability only where D-3 does not apply) plus
  warning-message pins, plus SQL-door value pins for `degrees`/`radians`. Red first on the base
  tree. Floats compared exactly (Spark's values are what the engine kernel produces).
- **D-5** Registry: `docs/spark-sql-iceberg-parity.md` §7 gains one row
  `FNP-ALIAS-DEGREES-1 — facade degrees/radians composed the formula — **FIXED 2026-09-15
  (FNP-ALIAS-1)**` in the style of the `FN-*` FIXED rows, oracle line
  `live PySpark 4.1.2, 2026-09-14`.

- **D-6** (orchestrator ruling on Q-1, 2026-09-15) `sum_distinct` / `sumDistinct` leave this unit for FNP-AGG-1, the aggregates unit that runs on the build clone: it adds a DISTINCT flag to the native aggregate builder (`PyColumn.aggregate`) and pins the six `sum_distinct` / `sumDistinct` cells of `python/repark/tests/fnp_alias_1_spark_oracle.json` plus the `Deprecated in 3.2, use sum_distinct instead.` warning. The two names stay on the deferred census until then. Clauses C-001..C-003 are re-scoped to the six delivered names.
- **D-7** (orchestrator ruling on Q-2, 2026-09-15) The `degrees` / `radians` mechanism is accepted as built: a single multiply by the precomputed `180 / pi` (or `pi / 180`) factor under the `DEGREES(x)` / `RADIANS(x)` display. It is the same arithmetic shape as Java's `Math.toDegrees` / `Math.toRadians` that Spark evaluates, and all twelve Python-door cells plus the SQL-door value cell are measured bit-exact against the fixture. A dispatch arm for the engine kernel would add Rust for no observable difference.

## Red-first record (base tree `acbb6a8e`, 2026-09-15)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_alias_1.py -q` → **37 failed, 1
passed**. Breakdown: 1 name-presence pin (`AttributeError: module 'repark.spark.functions' has
no attribute 'approxCountDistinct'`), 6 signature pins, 6 warning pins, and all 24 Python-door
oracle cells. The degrees/radians cells red on the composed formula itself
(`AssertionError: assert '((rad * 180) / pi())' == 'DEGREES(rad)'`, last-digit drift
`179.99984796050427` vs Spark `179.9998479605043`, and `[ARITHMETIC_OVERFLOW]` on
`F.degrees('i')`); the alias cells red on the absent names. The 1 pass is the SQL-door
degrees/radians value control — the SQL door already answers Spark's values, which is exactly
the base the card's C-004 builds on.

## PROPOSITION LEDGER — FNP-ALIAS-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The six delivered names (`approxCountDistinct`, `shiftLeft`, `shiftRight`, `shiftRightUnsigned`, `toDegrees`, `toRadians`; `sum_distinct` / `sumDistinct` moved to FNP-AGG-1 by D-6) are present on `repark.spark.functions` and in `__all__`, with parameter names and defaults equal to PySpark 4.1.2's recorded signatures. | `test_fnp_alias_1.py::test_delivered_names_present_and_exported` + `test_deprecated_signature_matches_the_spark_oracle` (6 names). | **PROVEN** | Six of eight pinned green: `approxCountDistinct`, `shiftLeft`, `shiftRight`, `shiftRightUnsigned`, `toDegrees`, `toRadians` — callable, in `F.__all__` (453→459, FN-SPLIT pin moved), parameter names/defaults equal to the recorded `inspect.signature` strings (annotation text follows the facade's `Column \| str` convention; the oracle's `'ColumnOrName'` spellings are PySpark's). `sum_distinct`/`sumDistinct` stay absent — blocked, Q-1 below; they remain on the deferred census (`test_functions_c.py`). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-001 |
| C-002 | The Python-door oracle cells answer Spark-equal (column name, type, rows; nullability pinned except the D-2 group key and the D-3 approx result). | `test_fnp_alias_1.py::test_python_door_cell_matches_the_spark_oracle` (24 cells). | **PROVEN** | All 24 delivered-subset cells green (card gates: 79 passed; full facade suite 6119 passed, 368 skipped, 0 failed). Names, types and rows are compared, floats exactly; nullable is compared for every column except `g` (D-2) and the `approx_count_distinct` result (D-3). The 6 `sum_distinct`/`sumDistinct` grouped cells are unpinned this round — blocked (Q-1). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-002 |
| C-003 | Calling each delivered deprecated alias warns `FutureWarning` with Spark's exact message. | `test_fnp_alias_1.py::test_deprecated_alias_warns_the_spark_message` (6 messages). | **PROVEN** | Six messages pinned exact: `Deprecated in 2.1, use approx_count_distinct instead.`, `Deprecated in 3.2, use shiftleft instead.` (shiftRight: `use shiftright instead.`, shiftRightUnsigned: `use shiftrightunsigned instead.`), `Deprecated in 2.1, use degrees instead.` (toRadians: `use radians instead.`). The seventh (`Deprecated in 3.2, use sum_distinct instead.`) stays unpinned with its name (Q-1). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-003 |
| C-004 | Facade `degrees`/`radians` answer Spark's values bit-exactly, display `DEGREES(x)`/`RADIANS(x)`, accept INT input, and the SQL door's degrees/radians values are unchanged. | `test_fnp_alias_1.py` degrees/radians cells + `test_sql_door_degrees_radians_values_match_the_spark_oracle`. | **PROVEN** | The card's named route — `_scalar("degrees", …)` — has no `call_scalar` dispatch arm (measured: `call_scalar: unsupported function "degrees"`), and Rust is fenced, so the wrapper builds the same kernel arithmetic as a one-multiply plan by the precomputed `180.0/pi` / `pi/180` doubles (the `dayname`/`monthname` rewrap pattern, homes per D-1: `functions_math.py`). All 12 facade degrees/radians oracle cells and the SQL-door value cell are green, floats exact (`179.9998479605043` now; `F.degrees('i')` answers instead of raising `[ARITHMETIC_OVERFLOW]`); the SQL door's values and types are pinned unchanged. Registry row `FNP-ALIAS-DEGREES-1` filed FIXED (D-5). Deviation from the named mechanism flagged for ratification (Q-2). pins: fnp-alias-1/C-004 |
| C-005 | No regression on the functions suites. | The card's gates: `test_fnp_alias_1.py` + `test_functions_a/b/c.py`; plus `test_functions_f.py` and `test_functions_split_identity.py` (both edited here) and the full facade suite. | **PROVEN** | Card gates 79 passed; the six functions suites + the CAP-1 mirror: 112 passed; full `python/repark/tests`: **6119 passed, 368 skipped, 0 failed** (run in two passes — the suite minus `test_aws_acceptance.py`, which skips its 10 legs offline). The `__all__` surface pin moved 453→459; both deferred-absent censuses ratcheted down; the ceilings moved by recorded shrink only (1962→1960, 2247→2237, mirrored in CAP-1). pins: fnp-alias-1/C-005 |
| C-006 | Registry row + maps. | `docs/spark-sql-iceberg-parity.md` §7 `FNP-ALIAS-DEGREES-1`; map.md lockstep in every touched directory; ledger evidence complete. | **PROVEN** | The registry row is filed in the FN-* style with the `live PySpark 4.1.2, 2026-09-14` oracle line. Maps updated in-lockstep in the same commits: `python/repark/tests/map.md` (new test + fixture + census/surface ratchet notes), `python/repark/src/repark/spark/map.md` (three module entries + the move note), `scripts/map.md` and `python/repark-parity/tests/map.md` (ceiling ratchets), `task/ledgers/staging/map.md` (this ledger). pins: fnp-alias-1/C-006 |
| C-007 | The unit's review round ran on the merge head: a Grok critic-logic pass and an S2-21 Python performance pass, every P1/P2 remediated or ruled in this ledger, and AT-1..AT-10 attested from their evidence. | Reports `/tmp/oc-worker/pa-alias-crit/report.md` (critic-logic, $0.70) and `/tmp/oc-worker/pa-alias-perf/report.md` (S2-21, $0.49). | **PROVEN** | S2-21 performance: clean, no P1/P2/P3 — facade `degrees` is 0.986 of base end-to-end on 10M rows (median of 15), the isolated kernel 0.779, construction 0.847; alias `warnings.warn` costs 4 % under default filters. Critic-logic: L-001 (P1, `_rescaled` dropped the origin and `join_sql_expr`, so `F.degrees(right['k'])` after a semi join bound the left column) REMEDIATED by GLM in `5fdbc63b` with the semi-join raise pins and an inner-join ON pin; L-002 (P2, negative shift counts unpinned on the Python door) REMEDIATED in `06295f7b`. The critic's mutation of `_DEGREES_PER_RADIAN` reds `F.degrees(col('rad'))` and `F.toDegrees(col('rad'))`. pins: fnp-alias-1/C-007 |

## Critic-logic findings (crit-logic-1, 2026-09-15) — dispositions

- **L-001 (P1) — REMEDIATED.** `_rescaled` dropped `join_sql_expr` and the origin keywords, so
  after `leftsemi`/`leftanti` the four rescaled names silently converted the LEFT `k` (where
  `F.abs(right["k"])` raises `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`), and an
  inner join whose ON clause used `F.degrees` on both sides failed with an ambiguous-reference
  error. Fix follows `bitwise_not`'s rewrap: `join_sql_expr=result.join_sql_part()` and
  `**_thread_origin(column)`. Pins (`test_fnp_alias_1.py`):
  `test_rescaled_right_ref_raises_after_semi_family_join` (4 names x leftsemi/leftanti, select
  and filter), `test_rescaled_left_ref_still_resolves_after_semi_family_join` (left-ref control),
  `test_inner_join_on_degrees_both_sides_resolves_and_matches`. Red on the unfixed tree: **9
  failed, 4 passed** — the 8 right-ref pins `Failed: DID NOT RAISE AnalysisException` (silent
  left bind), the inner-join pin `Schema error: Ambiguous reference to unqualified field x`; the
  4 left-ref controls passed. Green after: 51 passed.
- **L-002 (P2) — REMEDIATED.** The fixture's SQL-door negative-count cells (`shiftleft(i, -1)`,
  `shiftright(i, -1)`, `shiftrightunsigned(i, -1)` — Java `& 31` masking on INT) were not
  replayed on the Python door, and the Column-`numBits` shape was unpinned. New pins
  (`test_fnp_alias_1.py`): `test_negative_shift_matches_the_sql_oracle_values_and_types` (values
  and types from the SQL cells; names stay D-2's) and
  `test_column_num_bits_shifts_mask_like_java_on_int` (`F.shiftLeft('i', F.col('n'))` over
  `n ∈ {1, 2, -1, 33}` → `[16, 32, 0, 16]` at `int`). The aliases already answered the oracle
  (pin-gap finding), so the pins are green on arrival; liveness was proven by a scratch mutation
  of the `shiftLeft` wrapper to a constant count: the two shiftLeft pins red (2 failed, 2
  passed), reverted, 4 passed. A kernel masking regression therefore reds this suite.

## Ruling questions — resolved by the orchestrator (Q-1 → D-6, Q-2 → D-7)

- **Q-1 (RULING)** — `sum_distinct`/`sumDistinct`: the card's "follow how `count_distinct`
  builds a DISTINCT aggregate" needs a native distinct-aggregate builder that the facade does
  not have (`count_aggregate` is count-specific; `aggregate(kind, ignore_nulls)` has no
  distinct modifier; no other PyO3 surface builds one), and every route around it is fenced
  (Rust by the card, `dataframe/**` by D-1). Options: a small Rust arm (generalize
  `count_aggregate` or add a distinct flag to `aggregate`) in a follow-up card, or a declared
  LOUD unsupported like the FNP-15 family. The two names stay on the deferred census either way.
- **Q-2 (RULING)** — the C-004 fix deviates from the card's named mechanism: the engine's
  `degrees`/`radians` scalar is reachable on the SQL door but has no facade dispatch arm, so the
  fix composes the kernel's single-multiply form instead and pins it bit-equal. If the critic
  wants the literal engine-scalar call, it is a two-arm Rust dispatch change plus moving the
  wrappers back.

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-alias-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the tree by the Grok critic-logic pass - six names present and exported, sum_distinct and sumDistinct absent per D-6, 24 Python-door cells plus six warning texts plus six signatures green, degrees and radians bit-equal to Java's constants and to the SQL door.
      artifacts: [python/repark/tests/test_fnp_alias_1.py, task/ledgers/staging/fnp-alias-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: degrees and radians over tinyint, smallint, int, bigint at 2^53 and 2^53+1, decimal(38,10), float, double, NULL, NaN, +-inf and -0.0; shifts with negative, 31, 32, 33 and 64 counts and a Column count; approxCountDistinct over empty and NULL-only groups with valid and invalid rsd; the semi-join right reference that exposed L-001.
      artifacts: [python/repark/tests/test_fnp_alias_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Fail-loud paths - boolean and string degrees refuse at analysis, degrees().over() refuses, and after L-001 a right reference after leftsemi raises MISSING_ATTRIBUTES like F.abs instead of failing open.
      artifacts: [python/repark/src/repark/spark/functions_math.py, python/repark/tests/test_fnp_alias_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, locks or concurrent writers - the alias wrappers are pure Column construction plus warnings.warn.
    - id: AT-5
      status: N/A
      justification: No authentication, secrets, parsers or unsafe code; identifier quoting is the existing path.
    - id: AT-6
      status: ATTACKED
      evidence: The origin and join_sql contract of the new rewrap against _scalar and the replaced arithmetic form (L-001, remediated and pinned on semi and inner joins); facade against SQL-door bit identity on the oracle frame; INT and TINYINT promotion to double.
      artifacts: [python/repark/tests/test_fnp_alias_1.py, python/repark/tests/test_g4b_semi_join.py]
    - id: AT-7
      status: ATTACKED
      evidence: S2-21 Python performance review measured facade degrees end to end on 10M rows (0.986 of base, median of 15), the isolated projection (0.779), 100k Column constructions (0.847) and the alias warning cost (1.04x default filters); no finding.
      artifacts: [task/ledgers/staging/fnp-alias-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: inspect.signature names and defaults against the recorded PySpark signatures, FutureWarning category and exact text with stacklevel 2, __all__ install_into once each, and the deprecated_aliases example exiting 0 under check_example_coverage --require-execute.
      artifacts: [python/repark/tests/test_fnp_alias_1.py, docs/examples/functions/deprecated_aliases.py, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: No new log, metric or alarm path; the FutureWarning is the diagnostic and is pinned by text.
    - id: AT-10
      status: ATTACKED
      evidence: Mutating _DEGREES_PER_RADIAN in a scratch copy reds F.degrees(col('rad')) and F.toDegrees(col('rad')); pins compare collect() output against the live-Spark fixture, not the fixture against itself; the L-001 and L-002 pins were red before their fixes.
      artifacts: [python/repark/tests/test_fnp_alias_1.py]
  complete: true
```

