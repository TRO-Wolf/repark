# Unit ledger — EX-25 · v1.1 example backfill, the `F.*` long tail (a)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-25 merges, or when the owner closes the
slate row.

**Unit:** EX-25 · **Date:** 2026-09-05 · **Model:** muse-spark-1.3 · **Branch:** `docs/ex-25-functions-a` · **Base:** `bc7c76cc` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-25 lane brief (45 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_functions_a.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 45 `F.*` backlog names of the lane brief, re-derived at the base `bc7c76cc`
(all 45 present on `docs/examples/backlog.txt`; C-001 records the exact list). The oracle is
live PySpark 4.1.2 (ANSI on, UTC session zone): every asserted value in every example was
measured there first, then matched on repark. The brief's "plainly supported" premise held for
20 names; the other 25 diverge or refuse and stay on the backlog with nineteen new §7 rows
(EX-FN-1..19; `F.base64` keeps its BL-17 row), pinned by twenty tests in the new
`test_examples_functions_a.py`. Five new scripts plus the `F.hours` arm in
`partition_transforms.py` carry the 20 covered names. There is no `csv_json.py`: all four
CSV/JSON names refuse (EX-FN-6, EX-FN-8, EX-FN-16), so no honest example exists for that family.

**Roster (45):** `F.add_months`, `F.approx_percentile`, `F.array_position`, `F.array_sort`,
`F.arrays_overlap`, `F.arrays_zip`, `F.base64`, `F.char`, `F.chr`, `F.current_user`, `F.decode`,
`F.elt`, `F.encode`, `F.expr`, `F.flatten`, `F.format_number`, `F.from_csv`, `F.hash`, `F.hours`,
`F.initcap`, `F.isnan`, `F.json_tuple`, `F.kurtosis`, `F.make_interval`, `F.make_timestamp`,
`F.map_zip_with`, `F.mode`, `F.monotonically_increasing_id`, `F.months_between`,
`F.percentile_approx`, `F.posexplode`, `F.posexplode_outer`, `F.raise_error`, `F.randstr`,
`F.regexp_extract`, `F.replace`, `F.schema_of_csv`, `F.schema_of_json`, `F.sentences`,
`F.session_user`, `F.sha2`, `F.skewness`, `F.spark_partition_id`, `F.split`, `F.input_file_name`.

**Grouping (5 new files + 1 arm):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `functions/array_more.py` | `F.array_position`, `F.array_sort`, `F.arrays_overlap`, `F.flatten`, `F.map_zip_with` | The served array remainder: search, order, overlap, flatten, map merge. `F.arrays_zip` and the `posexplode` pair refuse (EX-FN-1, EX-FN-2). |
| `functions/strings_more.py` | `F.char`, `F.chr`, `F.elt`, `F.initcap`, `F.regexp_extract`, `F.sha2` | The served scalar-string remainder: code points, choice, caps, extract, digest. |
| `functions/dates_more.py` | `F.add_months`, `F.make_interval` | Month shifting plus interval-built date arithmetic. |
| `functions/stats.py` | `F.percentile_approx`, `F.approx_percentile` | The alias pair agreeing on one frame. |
| `functions/session_misc.py` | `F.current_user`, `F.session_user`, `F.randstr`, `F.isnan` | Session identity (shape-checked), token lengths, NaN tests. |
| `functions/partition_transforms.py` (`F.hours` arm) | `F.hours` | The name refuses outside `partitionedBy` on both engines, so it joins the transform example with Spark-grounded hour slots. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 45-name roster above is exactly the lane brief's list, every name re-derived from `docs/examples/backlog.txt` at the base `bc7c76cc`; 20 names are covered by the six example units in the grouping table and 25 stay on the backlog with a §7 row each (EX-FN-1..19, plus BL-17 for `F.base64`); no `csv_json.py` ships because all four CSV/JSON names refuse. | The backlog grep at dispatch (all 45 present), the shipped examples, and the oracle table (45 rows, one per roster name). | **PROVEN** |
| C-002 | `functions/array_more.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured answers for `F.array_position` (found/missing/NULL), `F.array_sort` ascending with NULLs last, `F.arrays_overlap` with its NULL decisions, `F.flatten` over NULL sub-arrays, and `F.map_zip_with` merging two maps; every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its five names. | **PROVEN** |
| C-003 | `functions/strings_more.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured answers for `F.chr`/`F.char` (modulo-256, negative-empty), `F.elt` in range plus its `INVALID_ARRAY_INDEX` raise, `F.initcap` splitting on spaces only, `F.regexp_extract` by group with empty no-match, and `F.sha2` at 224/256 bits; every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its six names. | **PROVEN** |
| C-004 | `functions/dates_more.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured answers for `F.add_months` from month ends both directions and `F.make_interval` shifting a date and a timestamp; the terse string-cast arm is disclosed as EX-FN-19, not asserted; every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-005 | `functions/stats.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured median and extremes over 1..100 through both `F.percentile_approx` and `F.approx_percentile`; the ignored accuracy knob stays disclosed under FN-APPROXPCT-ACC-1, not asserted; every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-006 | `functions/session_misc.py` runs green under `python <path>` with no network and no JVM, shape-checks `F.current_user`/`F.session_user` as non-empty strings and `F.randstr` lengths plain and seeded, and asserts the Spark-measured `F.isnan` triple with NULL answering false; every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its four names. | **PROVEN** |
| C-007 | The `F.hours` arm in `functions/partition_transforms.py` runs green under `python <path>` with no network and no JVM, writes a two-row hours-partitioned table and asserts the Spark-grounded slots 473698/473702 (Spark-measured `unix_seconds` floor-divided by 3600 per the Iceberg spec); scalar `F.hours` refuses identically on both engines. | The shipped arm (executed by the `--require-execute` gate leg and standalone) and the oracle row for the name. | **PROVEN** |
| C-008 | The 20 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 20, 213 → 193, with no other `scripts/` change; the backlog delta is exactly the covered set; the gate's static half and its `--require-execute` leg both exit 0 (718 covered; 193 backlog; 190 examples on this tree). | The gate's own counts line at the base `bc7c76cc` (698/213/185, the unit's before-line) and on the shipped tree (718/193/190), plus the red-first provocation below. | **PROVEN** |
| C-009 | Every stayed name carries a §7 row naming its measured cell (EX-FN-1..19 plus BL-17 for `F.base64`), and every new row carries a pin in `python/repark/tests/test_examples_functions_a.py` (20 tests, all green); the pins codify today's refusals and wrong arms so the fix units red them on purpose. | The nineteen registry rows, the pin file's green run, and the oracle table's stayed column. | **PROVEN** |

`LOGIC_SCORE` = **9/9 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — the backlog ratchet (round 1, on this tree):** the five new example files held
outside `docs/examples/` (in gitignored scratch) plus the `F.hours` entry stripped from the
`partition_transforms.py` `COVERS` list (temporary, reverted before the next run), while
`docs/examples/backlog.txt` kept the 20 roster rows deleted and `BACKLOG_BASELINE` stood at 193
(`213 − 20`, as if the whole covered set were taught): the gate exits **1** with exactly **20
findings**, every one `public name F.<name> has no example COVERS row…` naming exactly the 20
covered names, and no other finding. Restoring the files returns the static gate to **0** (`718
covered; 193 backlog; 190 examples`).
`pins: ex-25-functions-a/C-001, C-008`

**Provocation 2 — the execute-leg control (round 1, on this tree):** one provocation injected into
`functions/stats.py` (temporary, never committed, reverted before the next run): the asserted
median `50` overwritten with `51`. Full gate `.venv/bin/python
scripts/check_example_coverage.py --require-execute`: exit **1** with exactly one execute finding,
`example …/docs/examples/functions/stats.py exited 1: … F.median values [50] != [51]` — the
control names the script and both values. Reverting restores the full gate to **0**.
`pins: ex-25-functions-a/C-005, C-008`

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-05)

The measurement instrument is two shared probe scripts (`scratch/ex25/measure.py`,
`scratch/ex25/measure2.py`, gitignored) run once per engine with identical cells, plus a
`unix_seconds` grounding pass for the hours slots; the Spark halves below are verbatim cell
output. Each shipped example was also run standalone from the lane root before any commit
(exit 0, all six). The lane venv resolves `repark` to the lane with a release native
(`repark._native.__debug_assertions__` False, built by `maturin develop --release` in-lane).
One JVM at a time beside at most one other lane's; every oracle JVM was stopped by its script
(`spark.stop()`), and no lane JVM remained afterwards. The probe's first pass caught three
usage corrections that the second pass re-measured: Spark's `array_sort` takes no `asc`
argument (the repark-only descending arm is not taught), Spark's `replace` takes `lit`/column
search (the plain-string spelling is a facade-spelling divergence, EX-FN-15), and
`schema_of_csv`/`schema_of_json` take a foldable literal, not a column.

| Name | Spark cell | repark cell | Verdict | File / row |
|---|---|---|---|---|
| `F.add_months` | +1m: 2024-02-29, 2024-03-29, 2024-01-15; −2m: 2023-11-30, 2023-12-29, 2023-10-15; NULL NULL | identical | covered | `functions/dates_more.py` |
| `F.approx_percentile` | p50 over 1..100 is 50 | identical | covered | `functions/stats.py` |
| `F.array_position` | found 2/0/0/NULL; missing 0/0/0/NULL; NULL element all NULL | identical | covered | `functions/array_more.py` |
| `F.array_sort` | ascending, NULLs last; no `asc` parameter on Spark | identical ascending | covered (ascending only) | `functions/array_more.py` |
| `F.arrays_overlap` | True, False, NULL, NULL, NULL | identical | covered | `functions/array_more.py` |
| `F.arrays_zip` | [{0:1,1:x},{0:2,1:None}]; ([],[y,z]) NULL-filled; NULL NULL | refuses R-FN-BATCH2 | stayed | EX-FN-1 |
| `F.base64` | padded (`U3Bhcms=`, `QQ==`) | unpadded (`U3Bhcms`, `QQ`) | stayed | BL-17 (existing row) |
| `F.char` | A, \x00, comma, A, A, empty, \x00, NULL | identical | covered | `functions/strings_more.py` |
| `F.chr` | A, \x00, comma, A, A, empty, \x00, NULL | identical | covered | `functions/strings_more.py` |
| `F.current_user` | `john` (login-dependent) | `repark` | covered (shape-check) | `functions/session_misc.py` |
| `F.decode` | UTF-8 round trip; US-ASCII served | refuses: no charset codec | stayed | EX-FN-3 |
| `F.elt` | a/b/c in range; 0/4 raise INVALID_ARRAY_INDEX | identical incl. the raise | covered | `functions/strings_more.py` |
| `F.encode` | utf-16 `feff00410042`; US-ASCII `4142` | refuses: no charset codec | stayed | EX-FN-3 |
| `F.expr` | `a + 1` binds to [2, 3, None] | column refs raise; literals agree | stayed | EX-FN-4 |
| `F.flatten` | [1,2,3], [None,4], NULL, NULL | identical | covered | `functions/array_more.py` |
| `F.format_number` | `12,332.12`, `0.50`, `-9,876.54` (+4dp arms) | refuses R-FN-BATCH3 | stayed | EX-FN-5 |
| `F.from_csv` | (1,hello), (2,None), NULL | refuses E1 | stayed | EX-FN-6 |
| `F.hash` | −559580957, 1765031574, 42 on NULL, −936062819 on (1,a) | refuses R-FN-BATCH1 | stayed | EX-FN-7 |
| `F.hours` | refuses outside `partitionedBy` (same error class) | identical refusal; slots 473698/473702 | covered | `functions/partition_transforms.py` |
| `F.initcap` | space-split caps incl. `O'neil`, `Ünï_9 Ab` | identical | covered | `functions/strings_more.py` |
| `F.isnan` | True, False, False (NULL false) | identical | covered | `functions/session_misc.py` |
| `F.json_tuple` | ("1","2") strings; bad/NULL JSON NULLs | refuses E1 | stayed | EX-FN-8 |
| `F.kurtosis` | −1.1517159763313605 over [1,2,2,3,4,5] | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.make_interval` | date-add 2025-03-18; ts-add 14:35:11; cast spells units out | date arms identical; cast terse; collect refuses both | covered + BACKLOG ARM | `functions/dates_more.py` + EX-FN-19 |
| `F.make_timestamp` | 2024-01-15T10:30:05; Feb 30 raises ANSI | refuses R-FN-BATCH3 | stayed | EX-FN-10 |
| `F.map_zip_with` | {1:ax, 2:b, 3:y} | identical | covered | `functions/array_more.py` |
| `F.mode` | 2 over [1,2,2,3,4,5] | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.monotonically_increasing_id` | 0..4 single partition | refuses R-FN-BATCH4 | stayed | EX-FN-12 |
| `F.months_between` | 3.0, −8.0, NULL | refuses R-FN-BATCH1 | stayed | EX-FN-11 |
| `F.percentile_approx` | 50; [1, 50, 100] for [0, 0.5, 1] | identical | covered | `functions/stats.py` |
| `F.posexplode` | (0,10), (1,20) | refuses | stayed | EX-FN-2 |
| `F.posexplode_outer` | same + (NULL,NULL) for []/NULL | refuses | stayed | EX-FN-2 |
| `F.raise_error` | raises USER_RAISED_EXCEPTION P0001 | refuses E1 at build | stayed | EX-FN-14 |
| `F.randstr` | length 10 plain and seeded | identical lengths | covered (shape-check) | `functions/session_misc.py` |
| `F.regexp_extract` | group/whole/no-match empties, NULL NULL | identical | covered | `functions/strings_more.py` |
| `F.replace` | `lit`/column search; `$1` literal | plain-str search; `$1` backslash | stayed | EX-FN-15 |
| `F.schema_of_csv` | `STRUCT<_c0: INT, _c1: STRING>` | refuses E1 | stayed | EX-FN-16 |
| `F.schema_of_json` | `STRUCT<a: BIGINT, b: STRING>` | refuses E1 | stayed | EX-FN-16 |
| `F.sentences` | nested words; `""` → `[[]]`; NULL NULL | refuses R-FN-BATCH2 | stayed | EX-FN-17 |
| `F.session_user` | `john` (login-dependent) | `repark` | covered (shape-check) | `functions/session_misc.py` |
| `F.sha2` | 224/256 hexes incl. empty-string digests | identical | covered | `functions/strings_more.py` |
| `F.skewness` | 0.3053162697580512 over [1,2,2,3,4,5] | refuses R-FN-BATCH4 | stayed | EX-FN-9 |
| `F.spark_partition_id` | 0 | refuses R-FN-BATCH4 | stayed | EX-FN-12 |
| `F.split` | [a,b,c], [a,'',c], [''], NULL, limit arm | refuses R-FN-BATCH1 | stayed | EX-FN-18 |
| `F.input_file_name` | `file:///.../letters.csv` | refuses R-FN-BATCH4 | stayed | EX-FN-13 |

## Gates (2026-09-05, on this tree)

| Command | Exit |
|---|---|
| `make check-example-coverage` | **0** (`718 covered; 193 backlog; 2 exceptions; 190 examples`; static half, system python3) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (same counts; all six example units executed) |
| `make check-python-conventions` | **0** (251 files clean) |
| `make py-lint` | **0** |
| `make py-format-check` | **0** (734 files formatted) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q` | **0** (55 passed: 35 existing + 20 new) |
| `for f in` the five new scripts + `partition_transforms.py` `; do .venv/bin/python $f; done` | **0** (all six PASS) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |

Counts line (on this tree; the base `bc7c76cc` run printed `698 covered; 213 backlog;
2 exceptions; 185 examples`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 718 covered; 193 backlog; 2 exceptions; 190 examples`

Before this unit: `698 covered; 213 backlog; 185 examples` (at `bc7c76cc`, `BACKLOG_BASELINE`
213). On this unit's tree: `718 covered; 193 backlog; 190 examples` (`BACKLOG_BASELINE` 213 →
193) — exactly the 20 covered roster names, +20 / −20 / +5 (the hours arm extends an existing
file, so examples grow by five, not six).

## Review notes (round 1, in-lane)

| Finding | Disposition |
|---|---|
| `make check-example-coverage` flagged unused `F.col` COVERS rows in `array_more.py` and `strings_more.py` (string column refs, no `F.col` call) | dropped the two rows; gate green; recorded here |
| `make py-lint` flagged 3 E501s, 1 RUF005, 2 RUF043s on the new files | shortened the raise lines, unpacked the range, raw-escaped the two dotted `match` patterns; `ruff format` reflowed the six files; all six re-run green after the reflow |
| The brief grouped `hours` into `dates_more.py` and `from_csv`/`schema_of_csv`/`schema_of_json`/`json_tuple` into `csv_json.py` | `hours` refuses outside `partitionedBy` on both engines, so it joined `partition_transforms.py` where that use is taught; all four CSV/JSON names refuse, so no `csv_json.py` ships — recorded here and in Scope |
| The brief's "45 plainly supported" premise | measured 20 supported / 25 stayed; every stayed name carries a §7 row and a pin per the brief's own divergence rule — no invention, no papering over |

## Cost

The Muse Spark (muse-spark-1.3) leg started 2026-09-05: read the contract, the slate, the
corpus (the EX-24/EX-23 ledgers, the functions map, the gate, the §7 `F.*` rows), and the facade
for the ambiguous names (`replace`, `expr`, `map_zip_with`); built the lane release native
(`maturin develop --release`, 7m14s) and installed the pinned PySpark 4.1.2 oracle into the lane
venv; measured all 45 names on both engines through two shared probe scripts plus a
`unix_seconds` grounding pass (three JVM runs, each stopped by its script); wrote the five
example files plus the hours arm, the twenty pins, the nineteen §7 rows, and this ledger, ran
both red-first provocations, then the ratchet, the maps, and the full gate list, committing in
slices. Base `bc7c76cc`.

## Disk

Pickup: `df -h` 1.1 TB free of 1.8 TB. The provocation scratch lives under the gitignored
`scratch/ex25/` (probe scripts, held-aside example copies, captured Spark/repark cell logs —
all removable at close). Lane-local weight added by this unit: `.ivy2/` (182 MB, copied from
`~/.ivy2.5.2` per the brief's ivy redirect; untracked), the PySpark 4.1.2 install in the lane
venv, and the release native build under `target/` (untracked build cache, shared with any
later lane build). No worktree created (lane-local unit). No build artifacts committed.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-25 moves only the inventory/backlog ratchet, example files, §7 rows, and pins;
it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-25-functions-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 20 covered roster names are taught by five new example files plus the hours arm, and the oracle table records the Spark cell, the repark cell, and the verdict for all 45 rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/functions/array_more.py, docs/examples/functions/strings_more.py, docs/examples/functions/dates_more.py, docs/examples/functions/stats.py, docs/examples/functions/session_misc.py, docs/examples/functions/partition_transforms.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first provocation 1 held the five files outside docs/examples with the hours COVERS entry stripped while the backlog rows stayed deleted and the baseline stood at 193; the gate exited 1 with exactly 20 findings and the backlog is an exact baseline 193.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every F.* COVERS name on the F door alias; the examples call each covered name through F.<name>, and the two unused F.col rows the gate flagged were dropped rather than papered over, so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/functions/array_more.py, docs/examples/functions/strings_more.py]
    - id: AT-4
      status: N/A
      justification: The gate and the examples are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the six local examples; they build local frames (and one memory-catalog table for hours), drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service. The Spark JVM ran only the gitignored oracle probes, never the examples.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; provocation 2 ran the execute half with a wrong-median control and it failed by name with exit 1.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citations for C-001..C-009 live in scripts/map.md beside the prior example batches, the example docstrings cite no pins (one-line form; citations live in the maps), and this ledger cites its clauses in the red-first and oracle sections.
      artifacts: [scripts/map.md, docs/examples/functions/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Oracle: live PySpark 4.1.2 (ANSI on, UTC); probes under gitignored `scratch/ex25/`
- Pins: [../../../python/repark/tests/test_examples_functions_a.py](../../../python/repark/tests/test_examples_functions_a.py) (20 tests for EX-FN-1..19)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 (EX-FN-1..19 new; BL-17, FN-APPROXPCT-ACC-1 reused)
- Siblings: [ex-24-ta-b-ledger.md](ex-24-ta-b-ledger.md), [ex-23-ta-a-ledger.md](ex-23-ta-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-25-functions-a
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-25-functions-a
  artifacts_verified:
    ledger: PASS (C-001..C-009 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review notes carry the in-lane round-1 dispositions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, F.* long tail (a) — 20 covered, 25 stayed with EX-FN-1..19
  verdict: PENDING
  rejection_route: N/A
```
