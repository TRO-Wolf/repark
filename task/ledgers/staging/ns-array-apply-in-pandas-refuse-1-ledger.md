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
comment gate 0 hits before every commit. Parity row R-020 (written as R-019 on the first base; renumbered at the merge of main, where
ICE-TSNS-FRAME-DOOR-1 holds R-019).

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-1
  severity: S0
  category: AT-6
  clause: [C-001, C-002]
  claim: applyInPandas stores nanosecond array ticks unconverted into list timestamp us with no error
  evidence: test_ns_array_apply_in_pandas_refuse_1.py and base flat ticks 1767323045123456789 then -1
  disposition: REMEDIATED (interim refusal pinned by the same tests; the floor is future work under R-020)
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

## Fold 1 (2026-10-13), after an independent verify failed `b8f50345`

**Model (fold 1):** Claude Opus 5.5 (executor). The verdict named three fail grounds: a
nanosecond array behind a cell that decides nothing is still stored wrong (H-1), a nanosecond
timestamp inside a struct or map is still stored wrong (H-2), and answers main stores right
are refused (H-3). Two commits by the first executor sit on the failed head (`b3d63d8e`, pins;
`b1c5ae1b`, a narrowed scan). This fold measured them like the failed head, kept the idea of a
walk guided by the declared type, and replaced the pins and the scan by new commits. The
clause ids continue the table above; C-001 to C-010 keep their verdicts and F1.6 names the
cells that pin them now.

### F1.1 Measured before the design

Main is `f3f98823`; pandas 3.0.5, numpy 2.5.1, pyarrow 25.0.1; every probe printed the path
of the `repark` package it ran.

All three fail grounds reproduce on `b8f50345`: 18 mixed-unit cells store
`1767323045123456789` (H-1); `array<struct<t>>`, `array<map<string, t>>`, `struct<t>` and
`map<string, t>` identity over `ice.ns.src` store it (H-2); empty, all-NaT and epoch
nanosecond arrays and the `ArrowDtype` shapes main stores right are refused (H-3). Of this
fold's 548 pins, 129 are red on `b8f50345` and 81 on `b1c5ae1b`, where H-1 is closed: 42
cells of 19 handed-integer cases (H-2); 18 cells of nine container spellings pyarrow accepts
and the walk did not read (a struct as a list of `(name, value)` pairs or with bytes keys, a
map as entry dicts, as entry pair lists, as an object array of tuples, a map key that is an
array, a deque, a `dict_values` view, a pandas Series of arrays); and 21 cells of twelve
cases main answers: the tick `0` and `-1` arrays main stores right; a byte-swapped array, a
struct given as a positional list, a map entry given as a list, a string cell beside a
nanosecond array and a duplicated label, which main refuses with its own text; and three
cells of the facade's own routes (below). On main 169 are red and the 198 equals-main cells
are green: the fixture is main's. (The pin commit `1f408a5c` held 545 pins, 79 red on its
own source; the fix commit moved two GROUPED_AGG cells to equals-main and added five
ordered-window cells, for the reason F1.2 gives.)

Three mechanisms, each measured:

1. **The result side.** `pa.Table.from_pandas(…, schema=…)` reads an object cell with
   pyarrow's Python converter. At a declared list position whose value type is a timestamp, a
   numpy `datetime64` array of any unit is appended tick by tick: no unit conversion, no
   error. pyarrow itself refuses a two-dimensional, a zero-dimensional or a byte-swapped
   array, a list of `np.datetime64` scalars and an Arrow array cell. A pandas Series, a
   `DatetimeIndex` and a list of `pd.Timestamp` are read value by value and floored.
2. **The input side.** `Table.to_pandas()` gives a `timestamp[ns]` that is a direct field of
   a struct, or a direct key or value of a map, to the function as a Python `int`
   (`{'t': 1767323045123456789}`, `[('k', 1767323045123456789)]`); a `timestamp[us]` there
   is a `datetime`. No `to_pandas` option changes that (`timestamp_as_object`,
   `coerce_temporal_nanoseconds` and `maps_as_pydicts` measured) except
   `types_mapper=ArrowDtype`, which changes the column's dtype and every cell the function
   reads. A list of nanosecond timestamps keeps its unit at any depth: a numpy
   `datetime64[ns]` array. On the way back `from_pandas` takes a Python `int` under a
   declared timestamp as a tick of the declared unit, at a top-level position too (an
   `int64`, `Int64` or `int64[pyarrow]` column, or an object column of integers).
3. **The right value.** The registry's rule is the floor on the int64 ticks (R-019: `-1 ns`
   is `-1 µs`). A raw nanosecond tick `t` stored as microseconds is right only when
   `t == t // 1000`: `0` and `-1`. Main stores `1 ns` as `1 µs` (the floor is `0`) and
   `-2 ns` as `-2 µs` (the floor is `-1`); a test of magnitude does not see those.

### F1.2 The design

One chokepoint, `_pandas_result_to_arrow_batches`, as before. Two rules, both decided from
the data, neither from a list of statement shapes.

**When the door refuses.** Only after `from_pandas` under the declared schema has succeeded.
Where pyarrow raises, main's class and text stand by construction. The schema-less retry
branch is untouched: its types are inferred, so the bridge's schema check refuses a
nanosecond list itself.

**Rule 1, the array.** For every declared field that holds a list of microsecond timestamps
at any depth, the object column is walked level by level along the declared type, each
container read as pyarrow reads it. A list position takes a numpy array or any other
sequence. A struct position takes a dict by field name (text or bytes), a tuple by position,
any other sequence as `(name, value)` pairs. A map position takes a dict's items or a
sequence of entries, each entry a struct of key and value. At a list-of-timestamps position a
numpy array of dtype `datetime64[ns]` refuses when one of its ticks differs from that tick's
microsecond floor, NaT left out. Every other cell decides nothing and the walk goes on to the
end of the column, so a microsecond, millisecond, empty, NaT or null cell hides nothing. The
walk never reads the elements of a container whose declared elements are scalars. A container
that raises when it is read decides nothing; pyarrow reads it next, as on main.

**Rule 2, the handed integer.** Per group the door reads, from the Arrow group table (both
tables of a cogroup), the nanosecond ticks that `to_pandas` hands out as integers: every
`timestamp[ns]` that is a direct struct field, map key or map value, at any depth. When the
stored result holds one of them under a declared timestamp (an Arrow set lookup, no row
loop), the pandas result is walked as in rule 1 and a Python `int` equal to a handed tick at
a declared timestamp position refuses: a struct field, a map key or value, a list element, a
top-level object cell, or a value of an integer column under a top-level timestamp. A handed
tick that is the microsecond floor of a handed tick (itself included: `0`, `-1`) is left
out. So the floor of every handed tick always passes: an integer the function made with
`value // 1000` is stored as main stores it.

**The facade's own callbacks are not this door.** The GROUPED_AGG pandas UDF and the
ordered-window pandas UDF run through `GroupedData.applyInPandas` with the facade's own
callbacks (`_grouped_agg_pandas`, `_apply_ordered_window_pandas_udf`). The ordered-window
callback passes every source column of the frame through pandas, also when the query does
not select it: `select("id", mean("id").over(w))` over a frame that only holds an
`array(ns)` column answers `1.0 … 8.0` on main, and `b8f50345` refused it. So the two
callbacks get no scan plan and those routes answer exactly as main, where main is right and
where it is wrong (the selected column, the GROUPED_AGG result). They are `pandas_udf` doors:
a separate card, F1.7.

**Not chosen.** (a) Refusing every group frame that holds a nanosecond timestamp in a struct
or map: it refuses functions main answers right (the column dropped, the integers
floor-divided, turned into `pd.Timestamp`, returned as `bigint`). (b)
`types_mapper=ArrowDtype` for those columns: it changes what the function reads
(`{'t': Timestamp}` for `{'t': int}`), so a function that returns the integers as `bigint`
changes its answer. (c) Flooring at the input: that is the conversion, future work by the
owner's ruling.

**What the door cannot know.** An integer the function derived from a handed tick
(`tick + 1`), and a handed tick that equals the microsecond floor of another handed tick of
the same group, returned unchanged without that other tick. Both are stored as on main; F1.7
records them.

### F1.6 Where the earlier clauses are pinned now

The round-0 test functions are gone; each of their shapes is a cell of the matrix
(`python/repark/tests/_ns_array_apply_in_pandas_refuse_1_cases.py`), named `case|door|zone`.

- **C-001, C-002:** `table_array_ns`, `table_array_ns_built`, `table_array_tzns`, each
  `grouped` and `cogrouped`.
- **C-003:** `ns`, `ns_zoned`, each `grouped` and `cogrouped`.
- **C-004:** `table_nested_array_ns`, `table_struct_array_ns`.
- **C-005:** `table_literal_timestamp_array`, `table_array_us`,
  `table_array_ns_cast_in_function` (the hint: every floor).
- **C-006:** `table_top_level_ns`, `table_top_level_tzns` (class, cause class and text).
- **C-007:** `table_empty_array`, `table_null_array`, and beyond the two shapes round 0
  probed: `empty_ns`, `nat_ns`, `table_array_ns_filtered_empty`,
  `table_array_ns_sliced_empty`, `table_array_ns_null_element`.
- **C-008:** `table_nested_array_cast_ns`, `table_struct_array_cast_ns`,
  `table_nested_array_us`, `table_struct_array_us`.
- **C-009, C-010:** the neighbour run and the mutant runs of this fold, F1.4 and F1.5.

### F1.7 Open: what the door cannot know, and what this unit does not touch

Each is stored on the head exactly as on main. Seven of them are cells of the matrix whose
name starts with `open_`, so a later unit turns them.

- **An integer derived from a handed tick.** `{'t': tick + 1}` over `named_struct('t', ns)`
  stores `1767323045123456790` as microseconds (`open_table_struct_ns_plus_one`). The door
  sees an integer it did not hand out.
- **A handed tick that is the floor of another handed tick, returned alone.** One group
  holds the ticks `1767323045123456789` and `1767323045123456` (its floor); a function that
  returns only the second row unchanged stores `1767323045123456` as microseconds
  (`open_frame_floor_tick_row_only`). The same integer is what `value // 1000` of the first
  tick returns, so the door lets it through.
- **numpy `datetime64[ms]`, `[s]`, `[D]` arrays under `array<timestamp>`** are stored as raw
  ticks (`1767323045123` for a millisecond array, `open_ms_stored_as_raw_ticks`): the same
  unit-blind copy, another unit, not this unit's.
- **`pandas_udf` over nanosecond data, three routes.** The scalar `pandas_udf` over a
  nanosecond array stores `1767323045123456789` as microseconds on main and on the head.
  So do the two routes the facade runs through this bridge: a GROUPED_AGG pandas UDF that
  returns a nanosecond array or a handed integer (`open_grouped_agg_array_ns`,
  `open_grouped_agg_struct_ns_tick`), and an ordered-window pandas UDF over a frame with a
  nanosecond array or struct column the query selects (`open_window_array_ns_selected`,
  `open_window_struct_ns_selected`). Other doors, one separate card.

## PROPOSITION LEDGER — NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1 FOLD 1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-011 | H-1: a nanosecond array that stores a wrong tick refuses wherever it sits; a cell that decides nothing never ends the scan, across the rows of a column, across groups and inside one nested cell, on both doors. | The mixed-unit cells of `test_nanosecond_array_under_a_timestamp_list_refuses`, red before the fix. | OPEN | Pins committed red first; does every mixed-unit cell refuse with the fix? |
| C-012 | H-3: nothing main stores right is refused: empty, all-NaT and floor-equal (`0`, `-1`) nanosecond arrays, `ArrowDtype` columns, microsecond data, non-timestamp lists, top-level columns answer main's type and ticks. | `test_main_answer_stands` against main's recorded fixture. | OPEN | Fixture recorded on `f3f98823`; do all 198 cells equal it with the fix? |
| C-013 | L-1: where main refuses, main's class and text stand; the door raises only after the conversion under the declared schema has succeeded. | The error cells of `test_main_answer_stands` (class, cause class, text with the cause cut). | OPEN | Does a cell the scan flags and pyarrow refuses (a zero-dimensional array, a string cell beside a nanosecond array) keep main's error? |
| C-014 | H-2: an integer the group frame carried for a nanosecond timestamp in a struct or map refuses at every declared timestamp position: struct field, map key and value, list element, top-level object and integer column; grouped, cogrouped (either side), naive and zoned. | `test_handed_nanosecond_integer_under_a_timestamp_refuses`, red before the fix. | OPEN | Do all 42 cells refuse with the fix? |
| C-015 | An integer the function made passes as on main: the microsecond floor of every handed tick (the hint), a `pd.Timestamp`, the column dropped, a `bigint` target, a real timestamp whose tick equals a handed one. | The `table_struct_ns_*`, `frame_*` and `grouped_agg_*` cells of `test_main_answer_stands`. | OPEN | Does the floor of a handed tick pass when it equals another handed tick? |
| C-016 | M-1, M-2, L-2: the walk reads every container spelling pyarrow accepts at a declared list, struct or map position, at any depth, map keys included. | The container cells of the array test; mutants in F1.5. | OPEN | Which mutants die? |
| C-017 | `mapInPandas`, `mapInArrow`, `applyInArrow`, scalar `pandas_udf` and `udf` answer as main, and so do the GROUPED_AGG and ordered-window pandas UDFs, which the facade runs through the same bridge. | The verify stage D probes re-run on both sides; the `grouped_agg` and `ordered_window` cells of `test_main_answer_stands`. | OPEN | Is any other door's answer moved? |
