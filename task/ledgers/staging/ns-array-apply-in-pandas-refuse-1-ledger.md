# Unit ledger — NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1 · refuse the nanosecond array applyInPandas stores wrong

**Date:** 2026-10-13 · **Branch:** `fix/ns-array-apply-in-pandas-refuse-1` · **Base:** `8c51fbc4`
(`origin/main`) · **Commit:** item-3 commit on the branch ·
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Verifier finding H-3 (S0): grouped and cogrouped `applyInPandas` convert the
pandas result with `pa.Table.from_pandas(out_pdf, schema=…)`, which takes an object column
of numpy `datetime64[ns]` arrays into `list<timestamp[us]>` without converting the unit and
without raising. Reproduced on this clone first: plain `array(ns)` over `ice.ns.src`
answers flat ticks `[1767323045123456789, -1]` through both doors where the floored values
are `[1767323045123456, -1]`; reading the values back as datetimes overflows (year ~58000).
Owner ruling 2026-10-13: interim fix is REFUSE at that door with a hint, no conversion.

**Scope.** One Python-only change in `python/repark/src/repark/spark/dataframe/` (no cargo
build; the release native module was copied in git-ignored). Both doors share the one
conversion point `grouped_arrow._pandas_result_to_arrow_batches` (`cogroup.py` imports it).
`mapInPandas` and `mapInArrow` are untouched. No guard is relaxed: `guards_relaxed` is empty.

## PROPOSITION LEDGER — NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The verifier cell shape refuses on the grouped door, naive and zoned: plain `array(ns)` and `array(tzns)` through grouped `applyInPandas` raise `PySparkTypeError` naming the column, the declared type, the unit and the hint. | Red pins green after the fix. | PROVEN | `test_grouped_plain_ns_array_refuses`, `test_grouped_plain_tzns_array_refuses`; red on base (no error raised), green at head with the exact class and message parts asserted. |
| C-002 | The same shape refuses on the cogrouped door, naive and zoned, with the one message. | Red pins green after the fix. | PROVEN | `test_cogrouped_plain_ns_array_refuses`, `test_cogrouped_plain_tzns_array_refuses`; red on base, green at head. The shared conversion point is why one message covers both doors. |
| C-003 | A user-built object column of numpy `datetime64[ns]` arrays under a declared `array<timestamp_ntz>` refuses on both doors with no Iceberg table involved. | Red pins green after the fix. | PROVEN | `test_grouped_user_ns_object_column_refuses`, `test_cogrouped_user_ns_object_column_refuses`; base frame is a two-row pandas `id` frame, the function returns the ns arrays. |
| C-004 | The refusal reaches any list depth and struct-held arrays: `array(array(ns))` and `struct(array(ns))` with nanosecond data refuse. | Red pins green after the fix. | PROVEN | `test_grouped_nested_ns_array_refuses`, `test_grouped_struct_holding_ns_array_refuses`; on base the nested shape answers the wrong ticks and the struct shape answers values that overflow on read. |
| C-005 | Microsecond arrays keep answering, including through the hinted cast: literal `array(timestamp)` ticks are exact and casting elements to microseconds in the function answers. | Control pins equal to main. | PROVEN | `test_grouped_microsecond_array_answers` ticks `[0, 1767323045123456]`; `test_grouped_hint_cast_answers` ticks equal numpy floor values. Measured aside: engine `CAST(ns AS timestamp_ntz)` truncates toward zero (`-1 ns` to `0`), numpy `astype` floors (`-1 ns` to `-1`); each control pins its own door absolute. |
| C-006 | A top-level nanosecond column keeps main's answer, whatever it is. | Control pin byte-identical. | PROVEN | `test_grouped_top_level_ns_column_keeps_refusal`: exact class `PySparkException` with `would lose data`; the declared bare timestamp never enters the list walk. |
| C-007 | An empty list and a list of nulls answer as on main. | Control pins equal to main. | PROVEN | `test_grouped_empty_list_answers`, `test_grouped_list_of_nulls_answers`; probed cell shapes are an empty object array and `[None]`, neither decides nanoseconds. |
| C-008 | Nested and struct-held microsecond arrays answer as on main. | Control pins equal to main. | PROVEN | `test_grouped_nested_microsecond_array_answers`, `test_grouped_struct_holding_microsecond_array_answers`; probed cells are `datetime64[us]` arrays, which decide against refusal at the first cell. |
| C-009 | Every neighbour in the named selection keeps its answer. | The `-k` selection green. | PROVEN | `pytest python/repark/tests -k "applyIn or cogroup or grouped or mapIn or pandas_udf"`: 251 passed, 8 skipped (environment-only: no pyspark/JVM, no moto venv, live tier unset), 0 failed. pytest-xdist is absent here so the selection ran serially over the same set. |
| C-010 | The pins are live and the gates are green. | Mutants red then restored. | PROVEN | M1 check skipped: 8 failed 7 passed. M2 grouped-door only: 3 failed 12 passed. M3 depth-1 only: 2 failed 13 passed. Each restored with a clean tree and 15 passed. Gates in Evidence. |

## Evidence

### Base answers recorded before the edit (this clone, `8c51fbc4`)

Plain `array(ns)` grouped and cogrouped: `list<timestamp[us]>` ticks `[1767323045123456789, -1]`
(right: `[1767323045123456, -1]`); zoned twin likewise under `timestamp[us, tz=UTC]`.
User object column of ns arrays: same wrong ticks. Nested ns: wrong ticks at depth two.
Struct holding ns array: answers values that raise `OverflowError: date value out of range` on
read. Top-level ns column: `PySparkException` with `would lose data`. Microsecond, empty,
nulls, nested-us and struct-us controls answer; `mapInPandas`/`mapInArrow` refuse the ns
array with the `mapInArrow schema mismatch` text (untouched, verified unchanged at head).

### Pandas cell shapes that drive the data decision

Plain ns: object cells of `ndarray` `datetime64[ns]`. Nested ns: object `ndarray` cells holding
`datetime64[ns]` arrays. Struct ns: `dict` cells holding `datetime64[ns]` arrays. Zoned ns:
timezone-aware `datetime64[ns]` arrays. Microsecond: `datetime64[us]` arrays (first decisive
cell stops the scan). Empty: zero-length object arrays. Nulls: `[None]`. The loop touches the
cells of that one object column only.

### Gates run

New file 15 passed; `-k` selection 251 passed 8 environment-skipped; `make py-lint
py-format-check check-lib-py check-python-conventions check-docstring-presence
check-ledger-grammar check-docs-links spell-check`; `python3 scripts/sync_map_md.py --check`;
comment gate 0 hits before every commit. Parity row R-019 is the next free ICE-TSNS-SQL-1 id
on this base; open branches may claim the same number.

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-1
  severity: S0
  category: AT-6
  clause: [C-001, C-002]
  claim: applyInPandas stores nanosecond array ticks unconverted into list timestamp us with no error
  evidence: test_ns_array_apply_in_pandas_refuse_1.py and base flat ticks 1767323045123456789 then -1
  disposition: REMEDIATED (interim refusal pinned by the same tests; the floor is future work under R-019)
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ns-array-apply-in-pandas-refuse-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked every brief clause against the landed behavior: refusal shapes, controls,
        neighbours and mutants each have a named pin or run.
      artifacts: [test_ns_array_apply_in_pandas_refuse_1.py, ledger clauses C-001 to C-010]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Exercised naive and zoned units, depth one and two, struct-held arrays, user-built
        object columns, microsecond arrays, empty lists, lists of nulls and top-level columns.
      artifacts: [test_grouped_empty_list_answers, test_grouped_list_of_nulls_answers]
    - id: AT-3
      status: ATTACKED
      evidence: >
        The refusal class and message parts are asserted exactly, and the pre-existing
        top-level refusal text is pinned byte-identical.
      artifacts: [test_grouped_top_level_ns_column_keeps_refusal]
    - id: AT-4
      status: N/A
      justification: per-group conversion holds no shared or mutable state across groups
    - id: AT-5
      status: N/A
      justification: no authentication, secrets, paths or deserialization surface in this door
    - id: AT-6
      status: ATTACKED
      evidence: >
        The defect is silent value corruption; the refusal removes the corrupting path and the
        neighbour run shows no new corruption elsewhere.
      artifacts: [base flat ticks 1767323045123456789 then -1, neighbour run 251 passed]
    - id: AT-7
      status: N/A
      justification: the added scan visits only list-timestamp columns and stops at the first decisive cell
    - id: AT-8
      status: ATTACKED
      evidence: >
        The PySparkTypeError contract matches the neighbouring refusals and the untouched
        mapIn doors keep their exact mismatch texts.
      artifacts: [grouped_arrow.py _refuse_nanosecond_array_result, mapInPandas mismatch text]
    - id: AT-9
      status: ATTACKED
      evidence: >
        The refusal message names the column, the declared type, the data unit and the
        microsecond cast, so the failure diagnoses itself.
      artifacts: [test assertion on message parts]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Mutants M1, M2 and M3 each turned their pins red and were restored; every added
        branch has a pin that flips it.
      artifacts: [M1 8 failed, M2 3 failed, M3 2 failed, restored 15 passed]
  reattested: []
  complete: true
```
