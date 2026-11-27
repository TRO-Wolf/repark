---
name: spark-door-scoreboard
description: >-
  Measure the whole Spark door against Spark 4.1.2 on the recorded audit
  corpus: replay a RePark package against the saved Spark answers, score every
  comparable cell by class, and compare two scoreboards cell by cell to list
  what moved onto Spark and what moved off it. Reach for it to put a measured
  number on Spark-door parity, before and after a fix campaign, to check that a
  branch moves no cell off Spark across the whole corpus, to add cases with
  live Spark answers, or to plan a fresh whole-door audit. Do not use it for
  Iceberg catalogs, AWS, performance, or a workload's real mix, and do not use
  it to classify one divergence into a fix, a pin or a registry entry; that is
  audit-repark-parity.
---

# Spark-door scoreboard

A whole-door measurement of the Spark door against Spark 4.1.2. The corpus is the 2026-11-21
outside audit's: 17,264 cell definitions in 51 lanes with their recorded Spark answers, of which
16,645 are comparable. A replay runs only the RePark half, so it needs a built package and an
interpreter, but no JVM. The harness is
[python/repark-parity/src/repark_parity/scoreboard/](../../../python/repark-parity/src/repark_parity/scoreboard/map.md);
the corpus, its provenance and the committed scoreboards are in its
[data/](../../../python/repark-parity/src/repark_parity/scoreboard/data/map.md) directory.

**The scoreboard JSON is the fact; this skill is the sequence.** Every count below that is not a
command's expected output is dated and points at a scoreboard file. Read the numbers from the file,
not from here.

## 1. Scope

**It measures** the Spark door (the PySpark facade, `F.*`, `F.expr`, the Spark SQL text door, the
DataFrame, Column, Window, types and Row, the plain-file readers and writers, the session
settings and the error taxonomy) against Spark 4.1.2 on a deliberately hard corpus. Every scalar
cell runs in several seats (SQL literal, SQL over a physical column, `F.expr` literal and column,
the typed Column API, a `WHERE` filter); both ANSI modes; the zones UTC, `America/Los_Angeles` and
`Asia/Kolkata`; daylight-saving gaps and overlaps, pre-1970 instants, the year range's ends, NaN,
signed zero, narrow integers at their limits, decimal precision and scale, nested types and stored
writes.

**It does not measure** Iceberg catalogs or tables (section 11), AWS, performance, structured
streaming, Spark Connect, the RDD API, the native doors, the ML and technical-analysis modules, or
how often any of these cells occurs in a real workload.

**The EQUAL share is not "works for a normal job".** The grid is edge-weighted: one ordinary
`SELECT a + b` stands beside eighteen typed variants at their limits in six seats and two modes.
A 54% EQUAL share means 54% of these hard cells match Spark exactly, value and schema, nullability
and decimal scale included. A normal job that avoids the edges can match far more often; a job that
reaches one wrong-value family can be wrong on every row. Read the share as a trend line between two
scoreboards, and read the moved-off list as the regression signal.

## 2. Prerequisites

- **The Spark 4.1.2 interpreter**: a Python 3.12 with `pyspark==4.1.2` and `pyarrow==25.0.1`. Only
  `record` starts Spark; `replay` uses it as the RePark half's default interpreter, as the audit
  did.
- **Java 17** for `record` only.
- **The RePark half's imports.** The facade needs pydantic, which the Spark interpreter does not
  carry. Either point `SCOREBOARD_REPARK_PYTHON` at an interpreter that has pyarrow and pydantic
  (the repository `.venv`), or keep the Spark interpreter and set `SCOREBOARD_DEPENDENCY_PATH` to a
  directory holding `pydantic`, `pydantic_core`, `annotated_types`, `typing_extensions.py` and
  `typing_inspection` (pydantic 2.13.4, pydantic-core 2.46.4: the audit's versions). The second form
  keeps pandas and numpy identical to the audit's; 15 cells import pandas.
- **The orchestrating interpreter**: any Python 3.12 with pyarrow and pydantic, with
  `PYTHONPATH=python/repark-parity/src`. Never pass `-I`: it drops `PYTHONPATH` and imports the
  wrong build.
- **Memory**: each Spark driver is `local[1]` with a 2 GiB heap; a RePark batch process stays well
  under that. The memory floor refuses a batch below the given `MemAvailable`.
- **Cores**: every engine process is pinned with `taskset` to the CPU list and runs at nice 19,
  with Rayon and Tokio at two threads each.
- **No network.** Nothing contacts a package index or a cloud service.

The settings, each a flag or an environment variable. A command that needs a missing one stops and
names its flag and variable.

| Setting | Flag | Variable | Needed by |
|---|---|---|---|
| Spark 4.1.2 interpreter | `--spark-python` | `SCOREBOARD_SPARK_PYTHON` | `record`; default RePark interpreter |
| RePark interpreter | `--repark-python` | `SCOREBOARD_REPARK_PYTHON` | `replay`, `record --with-repark` |
| Extra RePark import path | `--dependency-path` | `SCOREBOARD_DEPENDENCY_PATH` | optional |
| Java home | `--java-home` | `SCOREBOARD_JAVA_HOME`, then `JAVA_HOME` | `record` |
| CPU list | `--cpus` | `SCOREBOARD_CPUS` | every engine run |
| Spark drivers at once | `--spark-drivers` | `SCOREBOARD_SPARK_DRIVERS` | `record` |
| Memory floor, MiB | `--memory-floor-mib` | `SCOREBOARD_MEMORY_FLOOR_MIB` | every engine run |
| Output root | `--output-root` | `SCOREBOARD_OUTPUT_ROOT` | every engine run; destinations are new paths inside it |
| Package directory | `--package` | `SCOREBOARD_PACKAGE` | `replay`, `record --with-repark` |

The session used in every command below:

```bash
export PYTHONPATH=python/repark-parity/src
export SCOREBOARD_SPARK_PYTHON=<spark-4.1.2-python>
export SCOREBOARD_DEPENDENCY_PATH=<pydantic-dir>
export SCOREBOARD_JAVA_HOME=<java-17-home>
export SCOREBOARD_CPUS=<cpu-list>
export SCOREBOARD_SPARK_DRIVERS=1
export SCOREBOARD_MEMORY_FLOOR_MIB=4096
export SCOREBOARD_OUTPUT_ROOT=<scratch>/scoreboard-runs
export SCOREBOARD_PACKAGE=<package-dir>
SB=".venv/bin/python -B -m repark_parity.scoreboard"
OUT=$SCOREBOARD_OUTPUT_ROOT
DATA=python/repark-parity/src/repark_parity/scoreboard/data
```

On a shared box, run each command under the box's shared measurement lock and its Spark JVM slot
rule, and give `--cpus` only the cores you were granted.

## 3. Getting a package

The harness takes a directory that holds `repark/_native.abi3.so`, the matching Python facade under
`repark/`, and the wheel's `repark-<version>.dist-info`. A `BUILT_FROM` file holding the commit is
optional; `replay` copies it into `replay.json`.

- **A lane kit**, where one is provisioned: `build-pkg <rev> <name> [release-fast|ci|release]`
  builds the wheel in a fresh worktree and installs it into `pkgs/pkg-<name>-<sha8>/` with
  `BUILT_FROM`, `PRODUCT_TREE` and `PROFILE`.
- **The repository's own build**, from a clean worktree at the revision:

```bash
cd python/repark
uvx --python 3.12 maturin@1.14.1 build --profile release -o <wheels> -i <python>
uv pip install --python <python> --target <package-dir> --no-deps <wheels>/*.whl
git rev-parse HEAD > <package-dir>/BUILT_FROM
```

Install the wheel; never copy a new `_native*.so` into an older facade (section 9). Score two
revisions with packages of the same profile (section 9, build profile).

## 4. The sequence

**4.0 Corpus check.** `$SB check-corpus` reads every stored lane, checks each Spark answer's digest
and engine tag, and refuses a duplicate id across lanes. Expected: `{"lanes": 51, "cells": 17264}`.

**4.1 Controls.** Replay the controls lane and check it.

```bash
$SB replay $OUT/<name>-controls --lane controls
$SB controls $OUT/<name>-controls/controls
```

It fails on a lost equal control, a lost stored write, a classifier mutation in the wrong class or
a float edge folded. It lists each controls cell whose class moved from the audit's expected class;
on the audit package pass `--strict` and expect `0 expected classes moved`. On main `9df269acb`
(2026-11-27) three moved because their defects were fixed: `control-sum` (now EQUAL),
`control-extract` and `control-epoch` (now NULLABILITY-ONLY).

**4.2 Full replay, twice.** Every comparable cell, batches of 192 on a pool of `--jobs` processes:

```bash
$SB replay $OUT/<name>-run1 --jobs 6
$SB replay $OUT/<name>-run2 --jobs 6 --batch-size 97
$SB flaky $OUT/<name>-run1 $OUT/<name>-run2 --output $OUT/<name>-flaky.json
```

The second run uses another batch size, so a cell that depends on what ran before it in the same
session also shows. Re-run any cell `flaky` lists, then pass every run to `flaky` again:

```bash
python3 -c "import json; print('\n'.join(json.load(open('$OUT/<name>-flaky.json'))['flaky']))" > $OUT/ids.txt
$SB replay $OUT/<name>-rerun --cells $OUT/ids.txt
$SB flaky $OUT/<name>-run1 $OUT/<name>-run2 $OUT/<name>-rerun --output $OUT/<name>-flaky.json
```

Measured 2026-11-27 on 16 cores: a full replay of 16,645 cells takes about 40 seconds with
`--jobs 6`, and three runs of main `9df269acb` (two batch sizes) differed on 0 cells.

**4.3 Per-lane replay.** `--lane <lane>` (repeatable) replays every stored cell of a lane,
recorded exclusions included, except the `local-statements` table probes bound to different
default catalog providers. `--cells <file>` replays listed ids. Each lane directory holds
`cases.jsonl`, `spark.jsonl`, `repark.jsonl`, `repark.log` and `diff.jsonl`.

**4.4 Scoreboard.**

```bash
$SB summary $OUT/<name>-run1 --flaky $OUT/<name>-flaky.json \
  --label "<date> <what>" --revision <full-sha> --measured <YYYY-MM-DD> \
  --output $DATA/scoreboards/<date>-<sha8>.json.gz --markdown $DATA/scoreboards/<date>-<sha8>.md
```

The JSON holds `meta` (label, revision, date, Spark version, the native SHA-256), `totals`
(definitions, comparable, EQUAL, EQUAL share, recorded exclusions, excluded at run, flaky, classes),
`ansi` (classes with ANSI on and off), `lanes`, `families` (from the cell fields `rule_id`, `card`,
`record_ids` and `variant`) and `cells` (id to lane, class, ANSI). It holds no path.

**4.5 Comparison with the last committed scoreboard.**

```bash
$SB compare $DATA/scoreboards/<last>.json.gz $DATA/scoreboards/<date>-<sha8>.json.gz \
  --output $DATA/reports/<date>-<base8>-to-<head8>.json.gz \
  --markdown $DATA/reports/<date>-<base8>-to-<head8>.md
```

Per lane and by id: `moved_on` (unlike in the base, EQUAL in the head), `moved_off` (EQUAL in the
base, anything else in the head, an exclusion at run included), `changed_unlike` (a different
unlike class) and `uncounted` (a change where one side is a recorded exclusion or flaky).

**4.6 Branch check.** Build main's package and the branch's at the same profile, run 4.2 and 4.4 on
both, and compare main to branch. A branch that moves a cell off Spark names each one in its PR with
its cause.

**4.7 Filing.** Commit the new scoreboard pair and report pair under `data/`, add them to the
"Committed" line of [data/map.md](../../../python/repark-parity/src/repark_parity/scoreboard/data/map.md),
and put the report's headline and the moved-off list in the unit's card.

## 5. The classes

The classifier joins the two answers by id and digest and labels the pair. Both answered: a type or
name difference, after removing nullability, is `WRONG-TYPE` first; then a value difference is
`WRONG-VALUE`; then a schema difference that is left is `NULLABILITY-ONLY`. Examples are real cells,
in the same class in the 2026-11-21 and 2026-11-27 scoreboards.

| Class | Example cell | What it was | What usually causes it |
|---|---|---|---|
| `EQUAL` | `control-one` | `SELECT 1 AS v` | value, type, names, nullability and decimal scale all match |
| `BOTH-REFUSED` | `num.int.abs.on.UTC.sql-literal` | `abs(INT min)` under ANSI: both `ARITHMETIC_OVERFLOW`, `22003` | a correct refusal; the verdict also records `error_class_match` and `sqlstate_match` (null when one side has no code) |
| `WRONG-VALUE` | `num.float.ln.on.UTC.sql-literal` | `ln(-1.0)`: Spark NULL, RePark NaN | a function shim, a DataFusion rewrite or fold, session-zone handling, a coercion that picks another type for the arithmetic |
| `WRONG-TYPE` | `num.decimal.round.7.on.UTC.sql-literal` | `round(DECIMAL(8,3), 7)`: Spark `decimal(9,3)`, RePark `decimal(8,3)` | result-type rules (decimal precision and scale, integer widening, float versus double), column naming |
| `NULLABILITY-ONLY` | `num.verify.pi.on.sql-literal` | `pi()`: Spark non-null, RePark nullable | DataFusion's nullability derivation where Spark proves non-null (constants, `coalesce` with a literal, array elements) |
| `ACCEPTED-WHERE-SPARK-REFUSED` | `num.follow.mod_zero.on.UTC.sql-literal` | `mod(7, 0)` under ANSI: Spark `REMAINDER_BY_ZERO`, RePark NULL | an ANSI check missing in one seat, a lenient cast or parser path |
| `REFUSED-WHERE-SPARK-ANSWERS` | `num.verify.e.on.sql-literal` | `e()`: RePark `UNRESOLVED_ROUTINE` | a missing function, option or syntax, a refusal on a range Spark accepts |
| `PANIC` | none in the corpus on either date | a Rust panic surfaced as `PanicException` | an unchecked arithmetic or slice; it blocks at any rank |
| `EXCLUDED` | `dates.excluded.curdate` (recorded: "Wall-clock function excluded by owner") | a cell that measures nothing comparable | setup failure, wall clock, unseeded randomness, an observation PyArrow cannot convert, a settings read-back failure |

`FLAKY` and `RECORDED-EXCLUSION` are scoreboard labels, not classes: the first marks a cell whose
answer differed between runs on one package, the second one of the 619 frozen exclusions. Neither is
comparable.

To read the answers behind a class: `$SB inspect $OUT/<name>-run1/numbers --class WRONG-VALUE
--limit 5`, or a name or id fragment as the second argument.

## 6. Regressions

- **Reading a comparison.** Start with `totals.moved_off`. Each entry is a cell that matched Spark in
  the base and does not in the head. `moved_on` is progress; `changed_unlike` is movement between
  wrong answers (for example a refusal that became a wrong value, which is worse).
- **What a moved-off cell means.** The head answers differently from the base on a fixed input with a
  fixed Spark answer. It is a regression unless the cell is date-, order- or session-dependent
  (section 9): confirm with a second run and with the base package if it is still buildable. Then read
  the cell with `inspect` and find the change that moved it.
- **A wrong stored value is S1.** On the corpus severity scale, S1 is data deleted or a wrong value
  stored; S2 other state changes or wrong reads; S3 refusal text and over-refusals; a panic blocks at
  any rank. A `WRONG-VALUE` or `WRONG-TYPE` cell that answers a query can reach a stored write, so
  rank it S1 until a `store: true` witness (section 7) shows it does not.
- **Hand-off.** The scoreboard finds and counts. Classification into a fix, a `_divergence` pin or a
  registry entry is [audit-repark-parity](../audit-repark-parity/SKILL.md)'s, against live Spark.
  Hand it the moved-off ids and the `inspect` output.

## 7. New cases

**The case schema.** One JSON object per line.

| Field | Meaning |
|---|---|
| `id` | unique across the whole corpus; `check-corpus` and `ingest` refuse a reuse |
| `ansi` | boolean; set and read back before the cell |
| `zone` | an IANA name; set and read back before the cell |
| `seat` | the entry point: `sql-literal`, `sql-column`, `expr-literal`, `expr-column`, `typed-column`, `sql-filter`, or a lane's own seat name |
| `surface` | one API-freeze row id (`J1`, `K7`, `B1`, ...) |
| `sql` or `py` | the operation: Spark SQL text, or a trusted Python expression evaluated in the case scope |
| `names` | the public names this cell exercises independently; a SQL or `F.expr` cell does not prove the typed wrapper |
| `setup`, `once` | trusted Python statements; `once` runs once per process for a shared view. A setup failure is an exclusion, so keep the tested operation in `sql` or `py` |
| `settings` | extra Spark settings set for the cell and restored after |
| `store` | `true` writes the result to fresh Parquet, reads it back, and cold-reads every part with PyArrow into `physical_write` |
| `ordered` | `true` only when Spark promises the order; rows otherwise compare as a multiset |
| `exclude` | a reason; the cell is recorded as excluded on both engines |
| `expected`, `expect_value_equal` | controls lane only: the class or value equality the controls check demands |
| `rule_id`, `card`, `record_ids`, `variant` | optional family labels the scoreboard counts by |

The case scope holds `s` (the session), `F`, `T`, `Window`, `Row`, `Decimal`, `dt`, `pa`, `Path`,
`show_text`, `fixture(pa.table(...))` (identical physical Parquet input on both engines, so no
optimizer folds the column), `work`, `folder` and `p` (an unused local data path for this cell). The
modules in [cellpath/](../../../python/repark-parity/src/repark_parity/scoreboard/cellpath/map.md)
import by bare name. `repark_parity.scoreboard.grid.scalar_grid` writes the six scalar seats for one
expression, and `write_cells` writes a case file.

**Recording the Spark answers live.** Write the cells to a file, then record them, with the RePark
half and the diff in the same destination:

```bash
$SB record <new-cases>.jsonl $OUT/<lane>-record --with-repark
$SB inspect $OUT/<lane>-record
$SB ingest <lane> $OUT/<lane>-record/cases.jsonl $OUT/<lane>-record/spark.jsonl
```

`record` refuses an existing destination and checks the Spark version is 4.1.2. `ingest` checks
ids, digests, the engine tag and the version, refuses an id another lane holds, and refuses a stored
lane unless `--replace` is given. Then run `check-corpus` and add the lane's counts to
[data/map.md](../../../python/repark-parity/src/repark_parity/scoreboard/data/map.md).

**The digest rule.** Each answer carries the SHA-256 of its cell's sorted-key ASCII JSON line. Edit a
cell in any way, a space inside the SQL included, and its stored answer no longer joins: the diff
fails with `wrong engine or input digest`. A cell is never edited in place.

**Re-record a whole lane** (record it all, then `ingest --replace`) when Spark is upgraded, when any
cell definition in it changes, or when a `cellpath/` helper it imports changes. A whole Spark upgrade
re-records every lane and starts a new baseline; do not mix Spark versions in one corpus.

## 8. A fresh whole-door audit

Repeat the 2026-11-21 method when the corpus no longer represents the door (a Spark upgrade, a new
surface, a scoreboard that stays flat while known families close):

1. **Fix the target and the finish line.** Record the audited sha, build one package, and record its
   native SHA-256. Draft finish line: for a declared surface, every public name answers as Spark
   4.1.2 does on a measured grid or refuses loudly; with ANSI on there is no silent difference;
   ANSI-off differences rank second. The declared surface is the Spark-facing rows of
   `docs/design/v1-0-api-freeze.json`. A missing name is a coverage gap, not a defect.
2. **Prove the harness first.** Run known divergent cells from open cards and an equal control set;
   the harness must report both before any lane starts.
3. **Build grids from inventories, not memory.** Introspect the oracle's public names and signatures
   (`cellpath/inventory_names.survey`), and give every scalar cell the six seats, both ANSI modes and
   the three zones, with daylight-saving edges, pre-1970 instants, range ends, NaN, signed zero,
   narrow integers and decimal limits as edge values.
4. **Set session state per cell** on both engines (ANSI, zone, tested settings) and never rely on
   the machine's zone. Leave out anything unseeded or clock-dependent and say so in `exclude`.
5. **Compare type as well as value**, including nullability and decimal scale; keep float signs and
   NaN as one class; compare timestamps as instants and as rendered text; record a refusal's error
   class and SQLSTATE.
6. **Group differing cells into families by cause.** Each family names the seat in the code (path
   and lines at the sha, or the DataFusion crate and version) and the Spark rule it breaks, with its
   source. For each S1 family, measure one `store: true` cell. Match each family to existing cards
   and registry rows by reading their cells, not their file names.
7. **Refute.** A fresh reviewer re-runs each new S1 or S2 family's three smallest cells in new
   processes on both engines, checks that Spark's answer is the intended one, and reads the cited
   lines. Keep what survives; list what does not, with the reason.
8. **Deliver** one answer for the reader: the families that disqualify (silent, ANSI on) first, a
   proposed grouping into root-cause units, ANSI-off families, over-refusals and missing names by
   surface, what was not covered and why; then the new corpus `ingest`ed lane by lane and a new
   baseline scoreboard.

## 9. Pitfalls

- **Date-dependent cells.** Wall-clock functions are excluded by definition; two zero-argument
  `unix_timestamp` examples changed between two runs of the audit and are recorded exclusions. A cell
  that reads today's date drifts against a recorded answer without a code change: use literal
  instants, and treat a moved-off date cell as suspect until a second run on another day agrees.
- **Row order.** Rows compare as a multiset; `ordered: true` only when Spark promises the order.
  Ties under a window or a constant sort key are unspecified on both engines; 74 recorded exclusions
  are order-sensitive for this reason.
- **Flaky cells.** Re-run any cell that differs between two runs of one package and mark it flaky; it
  leaves the count. None on main `9df269acb` across three runs (2026-11-27). A batch boundary moves
  with `--batch-size`, so a second run with another size also exposes session-state leaks.
- **Spark driver memory.** Each driver is `local[1]` with a 2 GiB heap. Keep `SCOREBOARD_SPARK_DRIVERS`
  within the box's JVM budget and the memory floor above a driver's need; a starved JVM fails a batch
  rather than a cell.
- **Cores shared with other work.** Give `--cpus` only granted cores. CPU contention does not change
  answers, but some stored writes differ in `physical_write.part_files` between builds; the classifier
  does not read `physical_write`, so a part-count change is an observation, never a class change.
- **A facade that does not match its extension.** The engine prints `repark._native.__file__`, fails
  when the binary is not under the given package, and records the binary's SHA-256 in every answer and
  in `meta.native_sha256`. Build a wheel and install it; a `.so` copied into an older facade answers
  from neither revision.
- **Build profile.** The `ci` profile inherits the dev profile's Rust overflow checks; `release` and
  `release-fast` wrap. On 12 `nested-pairs` cells (`pair.array.on.timestamp.date` and its siblings) the
  audit's `ci` package refused with a caught overflow where a release build answers a wrapped
  timestamp. Compare two scoreboards built at the same profile, and score what users run (release).
- **Never `-I`.** It ignores `PYTHONPATH`, so the engine imports an installed RePark instead of the
  package, and the harness instead of this checkout.
- **Never overwrite recorded answers.** Destinations are always new; `ingest --replace` re-records a
  whole lane only (section 7). Do not edit `data/cells/` by hand.

## 10. Reporting

Publish the comparison's Markdown table: the base and head EQUAL shares of comparable cells, the
moved-on, moved-off and changed counts, and the per-lane rows. Under it, list every moved-off id with
its head class, and the largest moved-on lanes. Add the scoreboard's class table with its ANSI-on and
ANSI-off columns when a reader needs the totals. The files go in
`python/repark-parity/src/repark_parity/scoreboard/data/scoreboards/` and `.../data/reports/`; the
card of the unit that measured them carries the headline and links both.

The first report is
[2026-11-27-dac2af61-to-9df269ac.md](../../../python/repark-parity/src/repark_parity/scoreboard/data/reports/2026-11-27-dac2af61-to-9df269ac.md),
recorded on the
[SPARK-DOOR-SCOREBOARD-1 card](../../../task/roadmap/mid-term/spark-door-scoreboard-1-card-2026-11-27.md).

## 11. Iceberg

This scoreboard does not cover Iceberg: its writes are plain local Parquet, and the audit kept its
table probes out of the comparable set. For Iceberg parity, use the declared divergences and backlog
in the parity registry, [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md),
and the AWS acceptance matrix,
[python/repark/tests/parity/test_aws_acceptance_flat_matrix.py](../../../python/repark/tests/parity/test_aws_acceptance_flat_matrix.py)
beside [test_aws_acceptance.py](../../../python/repark/tests/parity/test_aws_acceptance.py). A
whole-table scoreboard for Iceberg is card ICEBERG-SCOREBOARD-1 (named, not yet written, 2026-11-27):
the same replay over local catalog tables, with snapshot, schema and file-level witnesses.
