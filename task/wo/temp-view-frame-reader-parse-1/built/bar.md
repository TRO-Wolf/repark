# TEMP-VIEW-FRAME-READER-PARSE-1 unit 1 — the bar

Bar, against main `8bc26cba`: 382 + 430 zone view cells, 20 of 30 ANSI cells (10 are D2), all 128 view writes (96 by
this unit; the 32 `json_i` writes are EQUAL on main since #1370), E5 12, the nesting 36 and the held-frame family
reach EQUAL in every column; the 92 D4 cells are EQUAL on main and on this head; 0 EQUAL cells lost in step 0's table,
the reader sweep (642 -> 798), the write sweep and part 2's bar sets. Two new pins read a CSV path holding a space and
one holding `#`. `EXPLAIN` under matching settings is unchanged (VERBOSE disclosed, Q11). Cost: C-012 1.002x; six
short rows flag at 1.056x to 1.107x and are attributed to the build by instruction counts (1.0002x to 1.0015x), under
the owner's short-row rule; the fixed cost per query over a reader frame is 0.51 ms.

## The bar

Runs: `evidence/raw/{base,head}` (53 steps each, all exit 0, `_native` printed from the right package);
comparisons in `evidence/cmp/base-head/` (`bar.txt`, `sweep.txt`, `explains.txt`).

| count (ready.md) | first run (aa9fd178 -> f9cc698e) | rebuild (8bc26cba -> ba1957bf) |
|---|---|---|
| Zone view cells, base data | 382 WRONG -> 382 EQUAL | 382 WRONG -> 382 EQUAL |
| `csv_s`/`csv_f` view class | 148 -> EQUAL | 148 -> EQUAL |
| Zone matching, base data | 1,468 EQUAL both, 60 WRONG both | 1,528 EQUAL both (the 60 `b` cells, STRING-HMS) |
| Zone cross "direct" class | 64 EQUAL, 60 WRONG, 264 RAISE (both) | 124 EQUAL, 264 RAISE (both; the 60 `b` cells, STRING-HMS) |
| Midnight view cells | 430 -> 430 EQUAL, + 168 `csv_s`/`csv_f` | 430 -> 430 EQUAL, + 168 (gained 598, the same) |
| Midnight D4 `b` cells (60) | WRONG both | EQUAL both |
| Midnight matching | 1,468 EQUAL, 60 WRONG (both) | 1,528 EQUAL both |
| ANSI view cells | 20 of 30 EQUAL, 10 RAISE (D2) | 20 of 30 EQUAL, 10 RAISE (D2) |
| View writes | 96 of 128 (32 `json_i` D4) | 128 of 128 on the head: 96 by unit 1, 32 EQUAL on the base too |
| rv X (`ffilter`, `union_hour`, `twice_hour`, `sql_join`, `nv_hour`, E5 `cj`/`cjb`/`cross`) | 90 of 90 | 90 of 90 |
| E5 12 | EQUAL | EQUAL (rv X 12; pin cells E5 8 of 8); stop rule not triggered |
| Nesting (rv N) | 36 gained, 90 of 90 | 36 gained, 90 of 90 |
| rv XC (`cache_lazy` 4 WRONG) | 26 EQUAL, 4 WRONG | 26 EQUAL, 4 WRONG |
| rv D (D9-D11) | 18 RAISE | 18 RAISE |
| T | direct 17/54, matching 109/61, 0 moves | direct 27/44, matching 119/51 (+10 +10 `json_i`, STRING-HMS, both), 0 moves |
| S | 8 RAISE | 8 RAISE |
| Held-frame family (Fable p2/p3) | `direct`, `cjdirect`, `pathwrite` 4 each, `cache_after` -> EQUAL; `subq` 6 | the same; p2 and p3 byte-identical to the first run on base and head |
| `udf` (reported) | 2 WRONG of 4, both | the same |
| Pin cells | head 57 of 57, base 19 | head 57 of 57, base 19 |
| `csv_fix` vs Spark's `csv_s` | base 54 of 128 cross, head 0; matching 128 both | the same |
| `ucast.py` (user `CAST`) | head 4 of 4, base 0 of 4 | head 4 of 4, base 0 of 4 |
| `PREPARE` (`rp1x`) | reported | `rp1x` byte-identical to the first run on base and head |
| Reader sweep (1,032) | EQUAL 526 -> 682, +156, 0 lost | EQUAL 642 -> 798, +156, 0 lost |
| joins 219, attack 133, attack2 16, attack3 7, case 7, smart 20 | 0 moved | 0 moved |
| frames, fable-view2 | +1, +1, 0 lost | +1, +1, 0 lost |
| Write sweep 3,102 (`ws` true, false) | 0 moved | 0 moved |
| Part 2's bar sets: true 3,472, unset 43, rc 90, rev 157 | 0 moved (rev: 3 date cells on a rerun) | 0 moved (one date) |
| EQUAL cells lost, base -> head | 0 | 0 |

## Named, not this unit's count

- **7 ANSI cells STRING-HMS exposed:** `A/true_false/{csv_i,json_i}/{eb,ob,osb,sb}_hour` (7: `json_i` has no `sb`
  in this class), a view created under ANSI on and read under ANSI off. Spark raises `CAST_INVALID_INPUT`. Old base:
  raised (EQUAL). New base: STRING-HMS answers `hour` of the bad text under the reading ANSI off, so WRONG
  `(1, 3, …) (2147483648, None, None)`. Head: raises `CAST_INVALID_INPUT` (EQUAL), because the frame is decided under
  the creating ANSI. So the new base has 7 fewer EQUAL cells than the old base; the head holds all 170.
- **7 ANSI cells, the other direction:** `A/false_true/{csv_i,json_i}/*_hour`: Spark raises
  `CANNOT_PARSE_TIMESTAMP`; base raises `CAST_INVALID_INPUT`, head `CANNOT_PARSE_TIMESTAMP`. Both RAISE, EQUAL by
  class on both; the head now names Spark's condition.

## The 92 D4 cells

| set | old base aa9fd178 | new base 8bc26cba | head ba1957bf |
|---|---|---|---|
| 32 `json_i` view writes | WRONG | EQUAL | EQUAL |
| 60 midnight `b` cells (cross: `la_utc`, `utc_la`) | WRONG | EQUAL | EQUAL |
| also: 60 midnight `b` cells under matching zones | WRONG | EQUAL | EQUAL |

92 of 92 EQUAL on the head. Tags: `evidence/cmp/d4-tags.txt`; verdicts: `evidence/cmp/d4-92.txt`.

## The base against the old base

Every cell compared (`rawdiff.py`, `classify_base.py`; per-cell list `evidence/cmp/oldbase-base-moved.txt`).
No cell outside the two allowed classes moved. **No finding.**
- **Unit 0, CSV with a user schema:** 68 `csv_sov` cells in A (the overflow error text: `Cannot cast string
  '2147483648'` -> `Can't cast value 2147483648`, RAISE both); 4 `E` plans of `csv_s` (the cast shield, below);
  4 `EXPLAIN VERBOSE` rows of `csv_s`/`csv_sh` (the URL re-read's typed `column_1:Int64`).
- **STRING-HMS, `hour`/`minute`/`second` over a string:** 466 line cells (`json_i`, `csv_ns`, `csv_0`, `text_v`,
  and `csv_i`/`smart` over a `not a time` column, all in `b`/`hour` bodies: Z 120, midnight 120, W 32, T 26, A 52,
  readers 116); bar-sweep `hour_text` 8; Fable p4 `hour`/`minute`/`second(c)` 6; `EXPLAIN` 20
  (`hour(CAST(ts AS Date32))` -> `hour(to_timestamp(ts))`, `json_i` and `csv_ns`). Verdicts: 308 WRONG -> EQUAL
  (including the 92 and the 60 matching), 7 EQUAL -> WRONG (the 7 ANSI cells above, restored by the head).
- **The clock, not a build:** `overlay/start_time` in attack, attack2, attack3 (it compares a stored
  `current_date()`); bar-rc `current_date` 4; bar-rev `now`/`lts`/`cdate` 15; reader sweep `ice_t/create` 2 (the error
  names a snapshot time). All equal between new base and head.
- **The core count, not a build:** the two runs used 16 and 10 cores.
  `RoundRobinBatch(16)` -> `(10)` and the formatted plan's `1 -> 16` -> `1 -> 10` move 440 `EXPLAIN` rows, explain10
  11, r-E 6 and fable-view2 2 between the runs; normalised, they are equal.
- Pin cells: the first run's base `pins.json` predates `cddd1035`'s cell change, so I re-recorded the current cells on
  the old package: 0 of 57 moved to the new base.
- Cross-check, old head `f9cc698e` -> new head: the same classes (466 STRING-HMS, 68 + 4 unit 0, the clock).

## Pins

- `test_a_path_holding_a_space_or_a_hash_parses_under_the_session_zone[space|hash]`, each under both ANSI fixture
  params: `n INT, ts TIMESTAMP` read from `sp ace/a b.csv` and `ha#sh/a#b.csv`, as the file and as its directory,
  under Los Angeles and Tokyo, against Spark 4.1.2's answers (recorded here: `evidence/pins/spark.json`, 16 of 16
  cells; LA `(1, 1704106800), (2, 1704139200)`, Tokyo `(1, 1704045600), (2, 1704078000)`).
- Green: base `pkg-rp1base-8bc26cba` 50 passed; head `pkg-rp1-ba1957bf` 50 passed (`csvts-*.log`).
- Red: main `2900efde` before unit 0 (`pkg-sum-845766fd`): 4 of 4 failed (the UTC parse, `1704078000`); a mutation
  of the head that builds the URL as `"file://" + str(path)` without percent-encoding (`pkg-rp1mut-ba1957bf`): the
  `#` pin fails 2 of 2 (`AnalysisException`), the space pin stays green (the URL parser encodes a space).
- The probe `pathpins.py` on the base: 16 of 16 EQUAL; on main `2900efde`: 0 of 16.

- `python/repark/tests/session/test_temp_view_frame_reader_parse_1.py`: 57 cells against Spark 4.1.2 plus the G3
  `EXPLAIN` test. Red (base `pkg-rp1base-aa9fd178`, with `test_temp_view_reader_lineage.py`): 38 failed, 36 passed
  (pins2-base.log); green (head `pkg-rp1-6cafd881`): 74 passed (pins2-head.log). The 38 that move: Z 8, A 4,
  E5 8, H 16, N 2; held on both: SQ 8, SS 10, G3 1, the `EXPLAIN` test and part 2's 16 reader-lineage pins.
- Rust (t-spark-viewddl.log 76 passed, t-core-introspect.log 12 passed): the rule first in the optimizer;
  matching settings (the node optimizes to its input's plan, answers as it); differing (closes over its copy, answers
  as a frame parsed under the live zone; the bare frame answers otherwise); a node in `IN` and scalar subqueries under
  both; `write_plan` hashes the node as its input; the old `reader_lineage` test is replaced by "a JSON lineage
  registers as any frame does".

## G1 to G4

- G1: `ReaderParse` carries `analyzer: Arc<Analyzer>` from the wrapping state; `parse_closed` analyses the copy
  under the live settings, then `FrameView::closed` (schema of the analysed plan). Cell `Z/ansi_true/la_utc/csv_i/eb_hour`:
  base WRONG `(1, 3, …)`, head EQUAL `(1, 19, 1704078000)`; pin `Z/eb_hour/la_utc/csv_i` green; Rust
  `under_differing_settings_a_reader_parse_reads_its_copy_under_the_live_settings`.
- G2: the rule walks with `transform_down_with_subqueries` and `holds_parse` with `apply_with_subqueries`. `subq` 6 of
  6 EQUAL on head (Fable p2) and pins `SQ/matching/*`, `SQ/differing/*` 8 of 8; Rust
  `a_reader_parse_inside_a_subquery_is_decided_under_both_settings` (IN plus scalar, both settings).
- G3: the root-shape list below; cell `G3/headerless_csv` (`.columns` `['_c0', '_c1']`, rows, a view over it) EQUAL
  on base and head, and the `EXPLAIN` pin (plain `EXPLAIN`, `explain()`, `explain("formatted")`) green on both.
- G4: `LiveOptions::of_session` in `extension.rs`; the seat holds one handle per session, bound in
  `context_with_df_54_1_rule_guards` after the context is built; every node of the session clones it, so the lock
  timeout falls back to the session's last read. `temp_views.rs` untouched.

## Root-shape analyzer rules (G3)

- `SparkProjectionDisplay` (`spark_typed.rs:89-97`, root `Projection` only): a bare reader frame's root is the node;
  the parse projection beneath aliases every output explicitly, so the rule renamed nothing there on main either;
  under a user projection the root is the user's, as on main.
- `LambdaRebind` (`lambda_rebind.rs:75`, root expressions only): the node has no expressions; a parse has no lambda.
- `OptimizerSeatConfig` (`df_guards.rs:110-130`): skips only a root statement, empty relation or `VALUES`; the node
  is checked as any plan.
- `SparkLtzTimestampCast` → `retarget_dml_store_casts` (`timestamp_ntz_cast.rs:78-100`, `Dml` over `Projection` over
  `Values`): never a reader node.
- `StructSetopNames` `source_type_at` (`struct_setop.rs:79`, a union input that is a `Projection` is read beneath its
  casts): a reader frame as a direct union input is now read by its schema (the parse's declared types); under
  coercion the input is the coercion projection, as on main. Possible move, no measured cell: a struct-typed user
  schema in a union could stop refusing.
- `CsvFold` (`csv/fold.rs:332-352`), `GeneratorRewrite` (`generator.rs:532`), the DML conform rules
  (`conform_stage.rs:50`, `fractional_division.rs:93`, `insert_gate`, `store_overflow`): generic rebuilds or DML
  inputs; the node sits below and passes as a child.
- Measured: the `EXPLAIN` diff over the reader set (below) and the reader sweep (0 lost).

## Reported

- One emulation reader moved, rightly: `csv_fix` (the native `read_csv` with `utf8_columns` and a user `CAST`) is not
  a reader parse, so its user cast now follows the frame's zone, as Spark's user cast does: measured `ucast.py`
  (the facade CSV with a user `CAST(ts AS TIMESTAMP)`): Spark and head 4 of 4, base 0 of 4. `csv_fix` against Spark's
  `csv_s` twin: base 54 of 128 cross, head 0 of 128; matching 128 of 128 on both.
- Named, unchanged: `cache_lazy` 4 WRONG (`ts`, `u`, `b`); D2; D4; D9–D11 18 RAISE; `smartCsv` 20 unchanged; D6.
- `udf` (reported): json_s and csv_i 4 WRONG in `t`, `b`, `u` on base and head (the frame is planned when the UDF
  column is built, under the creating settings); `pq` EQUAL. Fable's prediction: it belongs with
  TEMP-VIEW-CACHE-LAZY-PARSE-1 (Q8, the materialisation-timing family).
- `PREPARE` (reported): Spark refuses `PREPARE` (`PARSE_SYNTAX_ERROR`). RePark accepts it and `EXECUTE` under B
  answers the view body wholly under B, `pq` too (base and head the same), so the prepared door ignores the frame's
  settings for every reader; the same query run directly under B answers Spark's rule on head 6 of 6 (base 2 of 6).

## EXPLAIN

- `df.explain` simple, extended, codegen, cost, formatted (66 each), plain `EXPLAIN` over six SQL shapes (22
  each), `.columns` (22): identical. explain10 22 and explain_joins 7: identical after path normalisation. rp.py's
  `E` cells under matching settings: 10 of 10 identical.
- `EXPLAIN VERBOSE` (22) differs: `ReaderParse` and the `repark_reader_parse` optimizer line. Q11, disclosed.
- **The named move from the first run:** a local CSV filter plan now carries unit 0's cast shield:
  `Filter: CAST(column_1 AS Int32) > Int32(0)` -> `Filter: __repark_cast_shield__(CAST(column_1 AS Int32)) >
  Int32(0)` (logical, `partial_filters`, and `FilterExec`), in `E/{la,utc}/x_csv_s` and `xh_csv_s`, on base and
  head alike. It comes from unit 0's merged head, not from unit 1.
- Under differing settings (`E/utc/*`, fable-view2 `jf_differ`) the head shows the closed frame view, as in the first
  run.

## The first run

The first run (head `f9cc698e` on unit 0's head `aa9fd178`) measured the left-hand column of the table above; this rebuild supersedes it.

The comparison and probe scripts of this rebuild are not committed; step 0's probes under [../evidence/probes/](../evidence/probes/map.md) replay its cell sets, and the path and user-`CAST` cells are pinned in the test files.
