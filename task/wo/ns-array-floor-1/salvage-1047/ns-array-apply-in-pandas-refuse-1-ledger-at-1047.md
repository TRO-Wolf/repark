# Unit ledger — NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1 · refuse the nanosecond array applyInPandas stores wrong

**Date:** 2026-10-13 · **Branch:** `fix/ns-array-apply-in-pandas-refuse-1` · **Base:** `8c51fbc4`
(`origin/main`) · **Commit:** item-3 commit on the branch; fold 1 is the commits after
`b1c5ae1b`, on main `f3f98823` ·
**Model:** Muse Spark (`muse-spark-1.3-contributor`); fold 1: Claude Opus 5.5 (executor) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
      evidence: Every clause of round 0 and of fold 1 has pins that were red before its change and are green after it. The 604 fold-1 pins were committed red first, 193 of them red on main and 149 on the failed head.
      artifacts: [python/repark/tests/test_ns_array_apply_in_pandas_refuse_1.py, python/repark/tests/_ns_array_apply_in_pandas_refuse_1_cases.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty, all-NaT, zero, minus one, one and minus two nanosecond arrays; masked, strided, byte-swapped, zero and two dimensional arrays; 20,000 leading rows; a group with no rows; an empty frame; a duplicated and an integer label; a cell that raises when read; a tick and its own floor in one group.
      artifacts: [python/repark/tests/ns_array_apply_in_pandas_refuse_1_main.json, python/repark/tests/_ns_array_apply_in_pandas_refuse_1_cases.py]
    - id: AT-3
      status: ATTACKED
      evidence: 206 cells equal main's recorded answer in type, ticks, error class and error text. The verify stage's probes re-run on the final source have no cell where main is right and the head refuses.
      artifacts: [python/repark/tests/ns_array_apply_in_pandas_refuse_1_main.json, python/repark/tests/_record_ns_array_apply_in_pandas_refuse_1_main.py]
    - id: AT-4
      status: N/A
      justification: The checks are pure functions of one group's frames and result; no session, cache or shared state is read or written.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, path or deserialization; the checks read cells the function returned and Arrow columns the engine produced.
    - id: AT-6
      status: ATTACKED
      evidence: The wrong-value paths are a numpy nanosecond array at a list position and a nanosecond tick returned as an integer. 193 cells that main stores wrong refuse; nine cases the door cannot judge or does not own are pinned to main's stored ticks and listed as open.
      artifacts: [python/repark/tests/test_ns_array_apply_in_pandas_refuse_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: Five interleaved cost sets against main. No list column 1.001, numpy microsecond cells 0.901, Python lists of datetimes 0.949, empty lists 1.034; the dearest of six more shapes is a struct of microsecond lists at 1.148.
      artifacts: [python/repark/tests/_time_ns_array_apply_in_pandas_refuse_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The readings mirror the pyarrow 25.0.1 converter, each spelling measured on main. Error cells compare the text with pyarrow's own text cut, so a wording change does not move a pin. Spellings pyarrow refuses are pinned as main's errors, so a new spelling it accepts shows.
      artifacts: [python/repark/src/repark/spark/dataframe/grouped_arrow.py, python/repark/tests/_ns_array_apply_in_pandas_refuse_1_cases.py]
    - id: AT-9
      status: ATTACKED
      evidence: Both refusal texts are compared whole. Each names the column, the declared Arrow type and the unit and carries a hint; each hint is applied in a pin and stores the floors.
      artifacts: [python/repark/tests/test_ns_array_apply_in_pandas_refuse_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: 53 mutants, 50 killed, 3 alive and equal in every answer. The neighbours answer as on main test by test. The whole facade suite passed 17338 with 3 failures of the checkout's environment that main's source shows too.
      artifacts: [python/repark/tests/test_ns_array_apply_in_pandas_refuse_1.py, task/ledgers/staging/ns-array-apply-in-pandas-refuse-1-ledger.md]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-7, AT-8, AT-9, AT-10]
  complete: true
```

The grammar admits one `COVERAGE_ATTESTATION` block per ledger; the block above is the
fold's, and its `reattested` list names what the fold ran again. The round-0 block said the
scan stops at the first decisive cell (AT-7, not applicable then); the verdict showed that
was the defect.

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
fold's 604 pins, 149 are red on `b8f50345` and 99 on `b1c5ae1b`, where H-1 is closed: 60
cells of 28 cases where an integer equal to a nanosecond tick of the frame is stored (H-2
and its kin); 18 cells of nine container spellings pyarrow accepts and the walk did not read
(a struct as a list of `(name, value)` pairs or with bytes keys, a map as entry dicts, as
entry pair lists, as an object array of tuples, a map key that is an array, a deque, a
`dict_values` view, a pandas Series of arrays); and 21 cells of twelve cases main answers:
the tick `0` and `-1` arrays main stores right; a byte-swapped array, a struct given as a
positional list, a map entry given as a list, a string cell beside a nanosecond array and a
duplicated label, which main refuses with its own text; and three cells of the facade's own
routes (below). On main 193 are red and the 206 equals-main cells are green: the fixture is
main's. (The pin commit `1f408a5c` held 545 pins, 79 red on its own source. The fix commits
moved two GROUPED_AGG cells to equals-main, added five ordered-window cells, and added the
cells of the mutants that survived the first run and of rule 2's wider reach, for the
reasons F1.2 and F1.5 give.)

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

**Rule 2, the nanosecond tick returned as an integer.** Per group the door reads, from the
Arrow group tables (both tables of a cogroup), the ticks of every nanosecond timestamp the
frame holds, at any depth. Those inside a struct or map reach the function as integers
(mechanism 2); the others reach it as nanosecond datetimes, and a function can turn them
into integers itself (`.astype("int64")`). When the stored result holds one of those ticks
under a declared timestamp (an Arrow set lookup, no row loop), the pandas result is walked as
in rule 1 and a Python `int` equal to one of them at a declared timestamp position refuses: a
struct field, a map key or value, a list element, a top-level object cell, or a value of an
integer column under a top-level timestamp. A tick that is the microsecond floor of one of
the ticks (itself included: `0`, `-1`) is left out. So the floor of every tick always passes:
an integer the function made with `value // 1000` is stored as main stores it. A frame with
no nanosecond timestamp costs one schema walk; only an object or an integer result column is
looked up.

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

**What the door cannot know.** An integer the function derived from a tick another way
(`tick + 1`); a tick that equals the microsecond floor of another tick of the same group,
returned unchanged without that other tick; and an integer equal to a tick of another group.
All three are stored as on main; F1.7 records them.

### F1.3 The verify stages re-run on the final source (C-011 to C-015, C-017)

The verdict's own probes, each side with its own source on the module path and the same
native module, main `f3f98823` against the final source of this fold. "Refused" means main
stores a wrong value and the head raises the refusal; "equal" means type, ticks, error class
and error text are identical.

| Stage | Cells | Refused | Equal | Right on main, refused on head | Wrong on the head, right on main |
|---|---|---|---|---|---|
| A, the defect | 12 | 12 | 0 | 0 | 0 |
| B, wrong values still stored | 134 | 68 | 66 (36 errors, 30 answers) | 0 | 0 |
| B2, real object arrays | 16 | 10 | 6 | 0 | 0 |
| B3, engine-built mixed cells | 7 | 5 | 2 | 0 | 0 |
| C, right answers refused | 120 | 10 | 110 (26 errors, 84 answers) | 0 | 0 |
| D, other doors | 45 | 0 | 45 | 0 | 0 |
| G, the hint | 4 | n/a | every cell stores the floors | 0 | 0 |

In stage D one cell is equal after the clone path inside an embedded traceback is cut; five
of its cells are the scalar `pandas_udf` storing `1767323045123456789` on both sides (F1.7).
In stage C two equal cells are a nanosecond array under a declared `array<bigint>`: the
nanosecond integers, which is what the type asks for. The probe marks a cell wrong when a
tick is at or above `10^17`, so it marks those two and misses a small tick; this fold added
the cells that test reads wrongly:

- **Small ticks.** `1 ns` is stored as `1 µs` and `-2 ns` as `-2 µs` by main (floors `0`,
  `-1`); the head refuses both (`ns_tick_one`, `ns_tick_minus_two`). `0` and `-1` are their
  own floors; both sides store them (`zero_ns`, `minus_one_ns`,
  `table_array_ns_minus_one_only`).
- **Container spellings (50 more shapes, one door).** 19 refused where main is wrong, 31
  equal (one after a memory address inside a `repr` is cut).
- **Functions over a nanosecond struct or map (27).** 17 refused; 10 equal: the column
  dropped, floor-divided, turned into `pd.Timestamp`, returned as `bigint` (two), four that
  main refuses, and `tick + 1`, stored wrong on both sides (F1.7).
- **The facade's own routes (15).** All equal to main.

### F1.4 Neighbours, the whole suite, gates (C-020)

- **Neighbours**, `-k "applyIn or cogroup or grouped or mapIn or pandas_udf or arrow_udf"`,
  eight workers: 844 passed, 8 skipped on the final source. Outside the pin file the same
  249 tests pass and the same 8 skip on main's source and on the head, test by test.
- **The whole facade suite**, eight workers, in two halves of the test files (each run ends
  inside the tool's ten-minute limit), on the final source and pins: 3 failed, 17338 passed,
  482 skipped, 151 xfailed. The three failures are of this checkout, not of the change: the
  package is not installed in the virtual environment here (the native module is copied in),
  so `test_version_ssot.py` (two tests) reads version `0.0.0` and
  `test_production_file_size.py::test_session_package_imports_without_a_cycle` spawns an
  interpreter that cannot import `repark`. The same three fail on main's source in the same
  environment.
- **Not reproduced, recorded.** A first whole run with a relative module path failed four
  more tests that spawn an interpreter (it could not import through the relative path); they
  pass with an absolute path. One attempt at the first half stopped at 99 % with all eight
  workers idle for ten minutes and was stopped by process id; the same half then ran to its
  end five times (124 to 128 s). The test that stalled was not identified (the box does not
  allow attaching to a process). `test_casesens_1.py::test_s5_iceberg_ddl_binds_exactly`
  failed in two of those runs; alone it fails three times of three on main's source and on
  the head, and its whole file passes on both: it picks the latest metadata file by
  modification time.
- **Gates**: `make py-lint py-format-check check-lib-py check-python-conventions
  check-docstring-presence check-ledger-grammar check-docs-links spell-check`,
  `python3 scripts/sync_map_md.py --check`, and the comment gate (0 added comment lines)
  before every commit.

### F1.5 Mutants (C-016, C-019)

Each mutant is one edit of the final source, made in a copy outside the checkout; the pin
file runs against the copy, and the checkout's own run stays green (604 passed). 53 mutants,
50 killed, 3 alive and argued below. The verdict's survivors M-1, M-2 and L-2 are m04, m06
and m33.

| Mutant | What it changes | Red pins | A case that turns red |
|---|---|---|---|
| m01 | Both rules skipped. | 193 | `array_of_struct_ns_array` |
| m01a | The array rule skipped. | 133 | `array_of_struct_ns_array` |
| m01b | The integer rule skipped. | 60 | `frame_fixed_size_list_struct_ns` |
| m02 | The cogrouped door gets no scan plan: grouped door only. | 98 | `array_of_struct_ns_array` |
| m03 | A list inside a list is not walked: depth 1 only. | 20 | `nested_depth_three_ns` |
| m04 | A list level reads its first cell only. | 40 | `datetime_list_then_ns` |
| m04b | A struct level reads its first cell only. | 6 | `frame_made_integer_then_frame_tick` |
| m04c | The integer leaf reads its first value only. | 2 | `frame_made_integer_then_frame_tick` |
| m05 | Struct fields ignored. | 58 | `array_of_struct_ns_array` |
| m05b | Only the first struct field is walked. | 6 | `struct_us_then_ns` |
| m06 | Map values ignored. | 22 | `map_dict_ns` |
| m06b | Map keys ignored. | 4 | `map_key_ns` |
| m06c | Maps ignored. | 26 | `map_dict_ns` |
| m07 | A non-nanosecond array ends the scan as deciding. | 20 | `empty_us_then_ns` |
| m08 | Every nanosecond array refuses: the empty, NaT and floor pass removed. | 22 | `empty_ns` |
| m08a | NaT is not left out. | 8 | `nat_ns` |
| m08b | `0` and `-1` refuse. | 8 | `minus_one_ns` |
| m08c | The floor replaced by truncation toward zero. | 6 | `minus_one_ns` |
| m09 | The refusal raised as `PySparkException`. | 193 | `array_of_struct_ns_array` |
| m10 | Frame ticks under a list are not read. | 17 | `frame_fixed_size_list_struct_ns` |
| m11 | Microsecond floors are not left out. | 14 | `frame_tick_and_its_floor_floor_divided` |
| m12 | The cogroup door reads the left table's ticks only. | 2 | `table_array_struct_ns` |
| m13 | A stored tick alone refuses, with no integer in the pandas result. | 24 | `frame_tick_and_its_floor_floor_divided` |
| m14 | Frame ticks in map keys are not read. | 2 | `table_map_ns_key` |
| m14b | Frame ticks in map values are not read. | 8 | `table_array_map_ns` |
| m14c | Frame ticks in struct fields are not read. | 46 | `frame_fixed_size_list_struct_ns` |
| m15 | A top-level integer column is not looked up. | 12 | `table_struct_ns_arrow_integer_column` |
| m16 | Both facade callbacks take the refusal. | 6 | `open_grouped_agg_array_ns` |
| m16b | The ordered-window callback takes the refusal. | 4 | `open_window_array_ns_selected` |
| m16c | The GROUPED_AGG callback takes the refusal. | 2 | `open_grouped_agg_array_ns` |
| m17 | The array refusal raised before pyarrow's conversion. | 4 | `ns_then_string_cell` |
| m18 | A container that raises when read is not guarded. | 2 | `unreadable_struct_cell` |
| m19 | A struct given as `(name, value)` pairs is not read. | 4 | `map_entry_pairs_ns` |
| m20 | Bytes keys of a struct dict are not read. | 2 | `struct_bytes_key_ns` |
| m21 | A struct given as a tuple is not read. | 26 | `map_dict_ns` |
| m22 | A map given as a dict is not read. | 4 | `map_dict_ns` |
| m24 | The elements of an object array are not read. | 25 | `frame_fixed_size_list_struct_ns` |
| m25 | Only lists and tuples are read as sequences. | 6 | `nested_deque_ns` |
| m26 | A floor-equal nanosecond array clears its dtype for the cells after it. | 4 | `table_empty_then_ns` |
| m28 | Only the plain list layout counts as a list. | 6 | `frame_fixed_size_list_struct_ns` |
| m29 | The frame's type check never sees a timestamp. | 60 | `frame_fixed_size_list_struct_ns` |
| m30 | The integer leaf never refuses. | 48 | `frame_fixed_size_list_struct_ns` |
| m31 | Any integer at a timestamp position refuses. | 12 | `frame_tick_and_its_floor_floor_divided` |
| m32 | A duplicated label is read as a column. | 2 | `duplicate_label_ns` |
| m33 | A column that is not an object column is walked. | 2 | `arrow_list_ns_whole` |
| m34 | A sliced map reads the whole batch's keys and values. | 2 | `open_frame_tick_of_another_group` |
| m35 | The frame's type check does not look inside a list. | 17 | `frame_fixed_size_list_struct_ns` |
| m37 | A struct dict is read by bytes keys only. | 52 | `array_of_struct_ns_array` |
| m38 | Every result column is looked up, whatever its dtype. | 2 | `open_table_struct_ns_as_arrow_integer_structs` |
| m40 | An integer column never refuses. | 12 | `table_struct_ns_arrow_integer_column` |

**Alive, and why each is not a gap.**

- **m27, the declared unit is not checked.** No declared timestamp of another unit reaches
  the door: every Spark timestamp type maps to `timestamp[us]`, and `timestamp_ns` in a DDL
  schema string becomes a string type (measured). Equivalent on every reachable input.
- **m36, the Arrow set lookup removed.** The lookup only saves the walk of the pandas result
  when no stored tick can match; the walk decides. Equal answers, more work.
- **m39, a frame with no nanosecond timestamp is still looked up.** The same: the tick set is
  empty, so no lookup matches. Equal answers, more work.

The first run of the set left three more alive, each closed with a cell: the integer leaf
reading its first value only (`frame_made_integer_then_frame_tick`), the struct level
reading its first cell only (`struct_us_row_then_ns_row`), and only the plain list layout on
the frame side (`frame_large_list_struct_ns`, `frame_fixed_size_list_struct_ns`,
`frame_list_view_struct_ns`). A later one, a sliced map reading the whole batch, is closed
by `open_frame_tick_of_another_group`.

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

Each is stored on the head exactly as on main. Nine of them are cases of the matrix whose
name starts with `open_`, so a later unit turns them.

- **An integer derived from a tick another way.** `{'t': tick + 1}` over
  `named_struct('t', ns)` stores `1767323045123456790` as microseconds
  (`open_table_struct_ns_plus_one`). The door sees an integer the frame did not hold.
- **A tick that is the floor of another tick, returned alone.** One group holds the ticks
  `1767323045123456789` and `1767323045123456` (its floor); a function that returns only the
  second row unchanged stores `1767323045123456` as microseconds
  (`open_frame_floor_tick_row_only`). The same integer is what `value // 1000` of the first
  tick returns, so the door lets it through.
- **An integer equal to a tick of another group.** The door knows the ticks of the group it
  handed out, not of the others (`open_frame_tick_of_another_group`).
- **An Arrow-typed nested column of integers the function builds from the ticks.** A
  `struct<t: int64>` `ArrowDtype` column under `struct<t:timestamp_ntz>` is cast by pyarrow,
  tick for tick (`open_table_struct_ns_as_arrow_integer_structs`). The door reads object
  columns and top-level integer columns; it never reads an Arrow-typed nested column.
- **numpy `datetime64[ms]`, `[s]`, `[D]` arrays under `array<timestamp>`** are stored as raw
  ticks (`1767323045123` for a millisecond array, `open_ms_stored_as_raw_ticks`): the same
  unit-blind copy, another unit, not this unit's.
- **`pandas_udf` over nanosecond data, three routes.** The scalar `pandas_udf` over a
  nanosecond array stores `1767323045123456789` as microseconds on main and on the head.
  So do the two routes the facade runs through this bridge: a GROUPED_AGG pandas UDF that
  returns a nanosecond array or such an integer (`open_grouped_agg_array_ns`,
  `open_grouped_agg_struct_ns_tick`), and an ordered-window pandas UDF over a frame with a
  nanosecond array or struct column the query selects (`open_window_array_ns_selected`,
  `open_window_struct_ns_selected`). Other doors, one separate card.

### F1.8 Cost (C-018)

`python/repark/tests/_time_ns_array_apply_in_pandas_refuse_1.py`, one million rows in 1,000
groups unless said, five sets, main and head in turn in every set, cores 32 to 47, on the
source of `038b649c` (the one later source edit is reached only after a stored tick has
matched). Each number is head seconds over main seconds of the same set.

| Shape | Five ratios | Median | Target |
|---|---|---|---|
| no list column | 1.001 0.990 1.040 0.994 1.018 | 1.001 | 1.05 |
| numpy microsecond cells, `array<timestamp_ntz>` | 0.901 0.869 0.924 0.873 0.993 | 0.901 | 1.10 |
| Python lists of datetimes | 0.932 0.969 1.010 0.922 0.949 | 0.949 | 1.15 |
| empty lists | 1.062 1.034 0.998 1.023 1.081 | 1.034 | 1.15 |
| a struct of microsecond lists | 1.101 1.148 1.172 1.104 1.191 | 1.148 | none set |
| a nanosecond struct, floor-divided in the function | 0.853 0.864 0.964 1.262 1.153 | 0.964 | none set |
| a nanosecond struct, the column dropped | 1.299 0.750 1.114 1.352 0.835 | 1.114 | none set |
| a nanosecond column, floored with pandas | 1.051 0.995 1.084 1.047 1.023 | 1.047 | none set |
| four top-level timestamp columns | 1.010 1.004 0.996 1.011 1.010 | 1.010 | none set |
| 50,000 groups of four rows | 1.024 1.015 1.003 1.018 1.033 | 1.018 | none set |

Which cells are read, and how:

- **A flat `array<timestamp>` column:** one pass over the cells; a cell that is not a numpy
  array costs one type test; a numpy array costs its dtype (an array whose dtype object was
  already seen as not nanosecond is skipped); only a `datetime64[ns]` array has its ticks
  read, vectorised. No element of a Python list is read.
- **A nested column:** one pass per declared level over the values of that level; the
  elements of a container are read only when its declared elements are containers that can
  hold a list of timestamps.
- **The integer rule:** nothing for a result schema with no timestamp; one schema walk for a
  frame with no nanosecond timestamp; otherwise one dtype test per timestamp column of the
  result and, for an object or integer column, an Arrow set lookup. The pandas result is
  walked value by value only after a stored tick has matched.

Two things the timings show. The walk runs before pyarrow's conversion: pyarrow 25.0.1
converts the columns of a frame with more than 100 rows per column on a thread pool
(`dataframe_to_arrays`), and the first pass over the cells after it cost 0.28 s per million
cells where the same pass repeated cost 0.06 s; walked after the conversion the empty-list
shape measured 1.16. And the nested shape is the dear one, 1.15: a struct level and a list
level of Python work per row, 0.55 s per million rows on the first pass and 0.32 s repeated.
The dropped-column shape does no work at the door (the result schema has no timestamp); its
spread is the spread of the measurement.

### F1.9 The verdict's findings, each closed or recorded

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-2
  severity: S0
  category: AT-6
  clause: [C-011]
  claim: a cell that decided not nanosecond ended the scan, so a later nanosecond array was stored as microseconds (verdict H-1)
  evidence: 28 mixed-unit cells red on b8f50345, mutants m04 m04b m07 m26
  disposition: REMEDIATED (test_nanosecond_array_under_a_timestamp_list_refuses, the 52 mixed-unit cells)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-3
  severity: S0
  category: AT-6
  clause: [C-014]
  claim: to_pandas hands a nanosecond timestamp inside a struct or map to the function as an integer, and from_pandas stores that integer as microseconds (verdict H-2)
  evidence: 60 cells red on b8f50345 and on b1c5ae1b, mutants m01b m10 m12 m14 m15 m30
  disposition: REMEDIATED (test_nanosecond_tick_as_an_integer_under_a_timestamp_refuses, 60 cells)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-4
  severity: S1
  category: AT-3
  clause: [C-012, C-013, C-017]
  claim: answers main stores right, errors main raises itself and an ordered-window query over an unselected nanosecond column were refused (verdict H-3 and L-1)
  evidence: 61 equals-main cells red on b8f50345, mutants m08 m16 m17 m33
  disposition: REMEDIATED (test_main_answer_stands, 206 cells against the fixture of main)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-5
  severity: S2
  category: AT-10
  clause: [C-016, C-019]
  claim: three mutants lived on the round-0 pins, first cell only, map values ignored, the ArrowDtype branch (verdict M-1, M-2, L-2)
  evidence: mutants m04 m06 m33 of this fold, each red
  disposition: REMEDIATED (53 mutants, 50 killed, 3 argued equivalent in F1.5)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-6
  severity: S2
  category: AT-7
  clause: [C-018]
  claim: every element of a Python list was visited when nothing decided, 1.60 and 1.36 of main (verdict P-1)
  evidence: the cost timer, medians 0.949 and 1.034 on the same two shapes
  disposition: REMEDIATED (F1.8, five interleaved sets)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-7
  severity: S3
  category: AT-9
  clause: [C-013]
  claim: the registry row and the maps claimed more than the pins held (verdict L-3)
  evidence: docs/spark-sql-iceberg-parity.md row R-020 and both map rows rewritten from the matrix
  disposition: REMEDIATED (row R-020, 399 cells named by kind, the open list)
```

```yaml
FINDING:
  id: F-ns-array-apply-in-pandas-refuse-1-8
  severity: S0
  category: AT-6
  clause: [C-017]
  claim: the scalar, the GROUPED_AGG and the ordered-window pandas_udf store a nanosecond tick as microseconds on main and on the head
  evidence: stage D rows pandas_udf, cells open_grouped_agg_array_ns and open_window_array_ns_selected
  disposition: OPEN (other doors by the owner's ruling, a separate card; recorded in row R-020)
```

## PROPOSITION LEDGER — NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1 FOLD 1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-011 | H-1: a nanosecond array that stores a wrong tick refuses wherever it sits; a cell that decides nothing never ends the scan, across the rows of a column, across groups and inside one nested cell, on both doors. | The mixed-unit cells of `test_nanosecond_array_under_a_timestamp_list_refuses`, red before the fix. | **PROVEN** | 52 cells of 26 cases, each grouped and cogrouped: a microsecond, millisecond, empty, NaT, null, NaN, empty-list, datetime-list, empty-nanosecond, NaT-nanosecond or zero-nanosecond lead; 20,000 microsecond or null rows; either group order; a group with no rows or an empty frame; a nested, struct or map cell. 28 array cells were red on `b8f50345`; mutants m04, m04b, m07, m26. |
| C-012 | H-3: nothing main stores right is refused: empty, all-NaT and floor-equal (`0`, `-1`) nanosecond arrays, `ArrowDtype` columns, microsecond data, non-timestamp lists, top-level columns answer main's type and ticks. | `test_main_answer_stands` against main's recorded fixture. | **PROVEN** | 206 cells equal the fixture recorded on `f3f98823` (164 answers, 42 errors); on main's source all 206 pass and the 193 refusal pins fail. 61 were red on `b8f50345`. Mutants m08, m08a, m08b, m08c, m33. |
| C-013 | L-1: where main refuses, main's class and text stand; the door raises only after the conversion under the declared schema has succeeded. | The error cells of `test_main_answer_stands` (class, cause class, text with the cause cut). | **PROVEN** | 42 error cells of 21 cases, among them a zero-dimensional nanosecond array and a string cell beside a nanosecond array (the scan flags both, pyarrow refuses, main's error stands), a byte-swapped array, Arrow cells, `ArrowDtype` with sub-microsecond digits, a duplicated and an integer label, a cell that raises when read. Mutants m17, m18, m32. |
| C-014 | H-2: an integer equal to a nanosecond tick of the group frame refuses at every declared timestamp position: struct field, map key and value, list element, top-level object and integer column; whether pandas handed it out (a struct or map) or the function took it from an array or a column; grouped, cogrouped (either side), naive and zoned. | `test_nanosecond_tick_as_an_integer_under_a_timestamp_refuses`, red before the fix. | **PROVEN** | 60 cells of 28 cases, all red on `b8f50345` and on `b1c5ae1b`: identity over a struct, map, array of struct, array of map, map key, struct in struct, map of struct, in a UTC and a New York session; the ticks rebuilt, as tuples, as dicts, in lists, in `int64`, `Int64`, `int64[pyarrow]` and object columns; `large_list`, `fixed_size_list` and `list_view` frames; the right side of a cogroup. Mutants m01b, m10, m12, m14, m14b, m14c, m15, m29, m30, m35, m40. |
| C-015 | A function-made value passes as on main: the microsecond floor of every tick (the hint), a `pd.Timestamp`, the column dropped, a `bigint` target, a real timestamp whose tick equals a frame tick. | The `table_struct_ns_*`, `table_top_level_ns_*` and `frame_*` cells of `test_main_answer_stands`. | **PROVEN** | `table_struct_ns_floor_divided` stores every floor on both doors and in both zones; `frame_tick_and_its_floor_floor_divided` stores a floor that equals another tick of its group; `frame_timestamp_equal_to_a_frame_tick` stores a real timestamp. Mutants m11, m13, m31, m38. |
| C-016 | M-1, M-2, L-2: the walk reads every container spelling pyarrow accepts at a declared list, struct or map position, at any depth, map keys included. | The container cells of the array test; mutants in F1.5. | **PROVEN** | Lists as list, tuple, object array, deque, `dict_values`, pandas Series; structs as dict (text and bytes keys), tuple, `(name, value)` pairs; maps as dict, tuples, entry dicts, entry pairs, object array; depth three; a map key that is an array. Spellings pyarrow refuses stay main's errors. Mutants m03, m05, m05b, m06, m06b, m06c, m19, m20, m21, m22, m24, m25, m28, m37. |
| C-017 | `mapInPandas`, `mapInArrow`, `applyInArrow`, scalar `pandas_udf` and `udf` answer as main, and so do the GROUPED_AGG and ordered-window pandas UDFs, which the facade runs through the same bridge. | The verify stage D probes re-run on both sides; the `grouped_agg` and `ordered_window` cells of `test_main_answer_stands`. | **PROVEN** | Stage D: 45 cells, 44 identical and one identical after a path is cut. Ten facade-route cells equal main, where main is right (a query that does not select the nanosecond column) and where it is wrong (F1.7). Mutants m16, m16b, m16c. |
| C-018 | Cost against main, five interleaved sets, one million rows in 1,000 groups: no list column at most 1.05, numpy microsecond cells at most 1.10, Python lists of datetimes and empty lists at most 1.15; no per-element Python loop over a cell that cannot hold a numpy nanosecond array. | `_time_ns_array_apply_in_pandas_refuse_1.py` on both sides. | **PROVEN** | Medians 1.001, 0.901, 0.949, 1.034; six more shapes in F1.8, the dearest a struct of microsecond lists at 1.148. |
| C-019 | Every pin is live: each rule, each reading and each exemption has a mutant that turns a pin red. | The mutant runs of F1.5. | **PROVEN** | 53 mutants, 50 killed; m27, m36 and m39 alive and equal in every answer. The pin file passes on the unmutated source after the runs (604 passed). |
| C-020 | Nothing else moves: the neighbours and the whole facade suite answer as on main, and the gates pass. | The runs of F1.4. | **PROVEN** | Neighbours outside the pin file: 249 passed, 8 skipped on both sides, test by test. Whole suite: 17338 passed, 3 failed for the checkout's environment on both sides. |
