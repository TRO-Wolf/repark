# Unit ledger — EX-5 · v0.7 example backfill, `F.*` string search, padding, UTF-8 and regex

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when the `F.*` string-search /
UTF-8 / regex family PR merges, or when the owner closes the slate row.

**Unit:** EX-5 · **Date:** 2026-09-03 · **Model:** grok-4.6 (continuation of glm-5.3-flash) ·
**Branch:** `feat/ex-5-functions-strings-regex` · **Base:** `58c8104` as merged (dispatch base `d7e2c4a`)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md).
**Ruling:** owner, 2026-08-31, v0.7 example documentation; one clause per batch.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, this ledger with its `staging/map.md` row, `docs/spark-sql-iceberg-parity.md`
§7, and `python/repark/tests/` for the three silent-divergence pins. Closed:
`crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`,
`STATUS.md`, every other ledger.

## Scope

Batch roster as dispatched (33 names, every one a row of `docs/examples/backlog.txt`
at `d7e2c4a`):

`F.overlay F.position F.repeat F.replace F.reverse F.split F.split_part F.substr
F.substring F.substring_index F.translate F.soundex F.sentences F.find_in_set
F.elt F.quote F.bit_length F.octet_length F.is_valid_utf8 F.validate_utf8
F.make_valid_utf8 F.try_validate_utf8 F.regexp F.regexp_count F.regexp_extract
F.regexp_extract_all F.regexp_instr F.regexp_like F.regexp_replace F.regexp_substr
F.rlike F.like F.ilike`

**As landed: 27.** Six names stay on the backlog — see the oracle table.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Batch lands runnable local examples for the 27 roster names the live oracle confirms, in eight files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 + Iceberg 1.11.0 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those 27 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 27, 605 → 578 as merged (844 → 817 at dispatch), with no other `scripts/` change; the six names `F.split`, `F.regexp_extract`, `F.sentences`, `F.elt`, `F.validate_utf8`, `F.replace` stay backlog rows with both engines' values recorded; FN-ELT-1, FN-REGEX-POSIX-1 and FN-LIKE-ESCEND-1 are filed in the registry with pins asserting repark's current answer; the gate's static half and its `--require-execute` leg both exit 0. | Red-first (33 roster names on the dispatch backlog), the oracle table, the eight scripts green on repark and on Spark, the three pins, the counts line, the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**. Citation: `scripts/map.md`.

## Red-first

**At dispatch** (`d7e2c4a`): `BACKLOG_BASELINE = 844` and every one of the 33 roster
names is a row of `docs/examples/backlog.txt`. None has a `COVERS` entry. Removing
those 33 rows without examples would red the gate with one finding per name; this
batch closes 27 of those findings and leaves the six measured drops listed.

**As merged** (`887695c` on `origin/main`, this branch at `dc12608`): sibling EX
batches have already moved the ratchet; this unit's own delta remains 27 names
(605 → 578). The live counts line is in Gates below.

## Oracle table (live PySpark 4.1.2 + Iceberg 1.11.0, `TZ=UTC`, zulu-17)

Throwaway driver under `/tmp/oc-ex5-oracle/`. Kept names: each example file was
run unmodified on repark (exit 0) and again with `repark.functions` swapped for
`pyspark.sql.functions` on the live session (8/8 SPARK-PASS). Dropped names:
per-input cells on both engines, 2026-09-03.

| Name | Inputs | Spark | repark | Verdict | File |
|---|---|---|---|---|---|
| `F.overlay` | `s ∈ {Spark, SQL, hello world, café, "", NULL}`; `overlay(s, "XY", 2, 3)` and default length | `["SXYk","SXY","hXYo world","cXY","XY",None]` / `["SXYrk","SXY","hXYlo world","cXYé","XY",None]` | same | kept | slice.py |
| `F.substr` | same `s`; `substr(s, 2, 3)` and `substr(s, -3, 2)` | `["par","QL","ell","afé","",None]` / `["ar","SQ","rl","af","",None]` | same | kept | slice.py |
| `F.substring` | same `s`; `substring(s, 2, 3)` | `["par","QL","ell","afé","",None]` | same | kept | slice.py |
| `F.split_part` | `s ∈ {one,two,three / a,b / NULL}`; parts 1, 2, 3, −1 | `["one","a",None]` / `["two","b",None]` / `["three","",None]` / `["three","b",None]` | same | kept | split_part.py |
| `F.substring_index` | `s ∈ {www.apache.org, a.b, nodot, NULL}`; counts 2, −1, 0, 4 | `["www.apache","a.b","nodot",None]` / `["org","b","nodot",None]` / `["","","",None]` / identity | same | kept | split_part.py |
| `F.translate` | `s` as slice.py; map `Sl→76` and delete `o` | `["7park","7QL","he66o wor6d","café","",None]` / `["Spark","SQL","hell wrld","café","",None]` | same | kept | translate.py |
| `F.position` | `s` as slice.py; `position("SQL", s)`, `position("l", s)`, `position(s, "Spark SQL")` | `[0,1,0,0,0,None]` / `[0,0,3,0,0,None]` / `[1,7,0,0,1,None]` | same | kept | search.py |
| `F.find_in_set` | same `s`; sets `"a,b,SQL"` and `"a,b,c"` | `[0,3,0,0,0,None]` / `[0,0,0,0,0,None]` | same | kept | search.py |
| `F.repeat` | same `s`; counts 2, 0, −1 | twice concatenates; 0 and −1 yield `""` (NULL stays NULL) | same | kept | words.py |
| `F.reverse` | same `s` | `["krapS","LQS","dlrow olleh","éfac","",None]` | same | kept | words.py |
| `F.soundex` | same `s` | `["S162","S400","H464","C100","",None]` | same | kept | words.py |
| `F.quote` | `s ∈ {Spark, a\`b, "", NULL}` | `["'Spark'","'a\`b'","''",None]` | same | kept | words.py |
| `F.bit_length` | `b ∈ {abc, \\xff, Café-bytes, a\\xffb, "", NULL}` | `[24,8,40,24,0,None]` | same | kept | utf8.py |
| `F.octet_length` | same `b` | `[3,1,5,3,0,None]` | same | kept | utf8.py |
| `F.is_valid_utf8` | same `b` | `[True,False,True,False,True,None]` | same | kept | utf8.py |
| `F.make_valid_utf8` | same `b` | `["abc","\ufffd","Café","a\ufffdb","",None]` | same | kept | utf8.py |
| `F.try_validate_utf8` | same `b` | `["abc",None,"Café",None,"",None]` | same | kept | utf8.py |
| `F.regexp` | `s ∈ {Spark SQL, aaa, abc123, NULL}`; pattern `S.*k` | `[True,False,False,None]` | same | kept | regex.py |
| `F.rlike` | same `s`; `^S` and `\\d` | `[True,False,False,None]` / `[False,False,True,None]` | same | kept | regex.py |
| `F.regexp_like` | same `s`; `S.*k` | `[True,False,False,None]` | same | kept | regex.py |
| `F.regexp_count` | same `s`; `a` and `[0-9]` | `[1,3,1,None]` / `[0,0,3,None]` | same | kept | regex.py |
| `F.regexp_replace` | same `s`; `[a-z]→*` and `\\w+→N` | `["S**** SQL","***","***123",None]` / `["N N","N","N",None]` | same | kept | regex.py |
| `F.regexp_substr` | same `s`; `[a-z]+` and `[0-9]+` | `["park","aaa","abc",None]` / `[None,None,"123",None]` | same | kept | regex.py |
| `F.regexp_instr` | same `s`; `a` and `z` | `[3,1,1,None]` / `[0,0,0,None]` | same | kept | regex.py |
| `F.regexp_extract_all` | lit `a1b2c3` groups 1 and 2; `aaa`/`z` group 0 | `[["a","b","c"]]` / `[["1","2","3"]]` / `[[]]` | same | kept | regex.py |
| `F.like` | `s ∈ {Spark SQL, aaa, abc123, NULL}` plus escape rows `100%` / `a_b` | `%`/`_` match as asserted; `100\\%` and `a\\_b` escape | same | kept | like.py |
| `F.ilike` | same frames; patterns `s%`, `%sql`, `100\\%` | case-folded match as asserted | same | kept | like.py |
| `F.replace` | `s ∈ {Spark SQL 42, abc123, NULL}`; `replace(col, lit S, lit Z)` vs bare `"S","Z"` | col+col: `["Zpark ZQL 42","abc123",None]`; bare: `AnalysisException UNRESOLVED_COLUMN` on `Z` | col+col: `TypeError` (Column not str); bare: `["Zpark ZQL 42","abc123",None]` | dropped | — |
| `F.split` | same `s`; `split(s, " ")` | `[["Spark","SQL","42"],["abc123"],None]` | `UnsupportedOperationException` `functions.split is not supported yet (R-FN-BATCH1)` | dropped | — |
| `F.sentences` | lit `"Hi there. Good Morgen? Yes!"` | `[[["Hi","there"],["Good","Morgen"],["Yes"]]]` | `UnsupportedOperationException` `functions.sentences is not supported yet (R-FN-BATCH2)` | dropped | — |
| `F.regexp_extract` | `s` as replace; pattern `([a-z]+)([0-9])` groups 1, 2; `zzz` group 0 | g1 `["","abc",None]`; g2 `["","1",None]`; nomatch `["","",None]` | `UnsupportedOperationException` `functions.regexp_extract is not supported yet (R-FN-BATCH1)` | dropped | — |
| `F.elt` | `elt(n, "a", "b")` for `n ∈ {1,2,3,0}` | n=1 `"a"`; n=2 `"b"`; n=3 and n=0 raise `INVALID_ARRAY_INDEX` | n=1 `"a"`; n=2 `"b"`; n=3 and n=0 return `None` (silent vs Spark raise) | dropped | — |
| `F.validate_utf8` | `b ∈ {abc, \\xff, a\\xffb, NULL}` | valid `"abc"`; invalid `IllegalArgumentException INVALID_UTF8_STRING`; NULL `None` | valid `"abc"`; invalid `PySparkException INVALID_UTF8_STRING` (different Python surface); NULL `None` | dropped | — |

`F.elt` is the silent-value drop: out-of-range index is NULL on repark and a hard
error on Spark. Round 2 filed it as `FN-ELT-1` with a pin (Registry rows filed below).
for a later parity unit.

## Gates

Recorded 2026-09-03 on this tree after the files landed.

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python docs/examples/functions/{slice,split_part,translate,search,words,utf8,regex,like}.py` | **0** each |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples python/repark/tests` | **0** |
| `uv run --no-sync ruff format --check docs/examples python/repark/tests` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_fn_elt_out_of_range.py python/repark/tests/test_fn_regex_posix_class.py python/repark/tests/test_fn_like_escape_end.py -q` | **0** (6 passed) |
| pin mutations (scratch copies): elt raise / regex `[1,0,4]` / like `[True]` | **1 red of 1** each |

Counts line, both legs identical, as merged on this tree:

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 333 covered; 578 backlog; 2 exceptions; 83 examples`

At dispatch (`d7e2c4a`) the same delta read `94 covered; 817 backlog; 23 examples` (was `67 covered; 844 backlog; 15 examples`: +27 names, +8 files).

## Cost

The dollar figure and wall-clock span below are the Grok continuation / remediation
leg only (the GLM first-pass is not billed here).

| Item | Value |
|---|---|
| Wall-clock start (first land) | 2026-09-03 11:44:16 UTC |
| Wall-clock end (first land) | 2026-09-03 11:51:39 UTC |
| Remediation start | 2026-09-03 12:17:53 UTC |
| Model | grok-4.6 (continuation of glm-5.3-flash) |
| Grok cost | $0.60 (continuation / remediation leg only) |
| Disk at start | 592G free on `/` (67% used) |
| Oracle | PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, `TZ=UTC`, zulu-17 |

## Registry rows filed

`FN-ELT-1`, `FN-REGEX-POSIX-1` and `FN-LIKE-ESCEND-1` (BACKLOG) in
`docs/spark-sql-iceberg-parity.md` §7, each with its pin
(`test_fn_elt_out_of_range.py`, `test_fn_regex_posix_class.py`,
`test_fn_like_escape_end.py`) and `pins:` citations in
`python/repark/tests/map.md` and `scripts/map.md`. Their current wrong values
are pinned so the fix reds on purpose.

`F.replace` (bare-string arm: repark treats a bare `str` as a literal, Spark
resolves it as a column name and refuses `UNRESOLVED_COLUMN`) is wrapper
semantics: disclosed in this ledger, §8 drop-in class — no pin.

Recorded, not built (prose-bullet class, no pin), live 2026-09-03:

- `F.overlay(F.lit("Spark"), F.lit("XY"), -1)` — Spark `'XYSpark'`; repark
  refuses `negative substring length not allowed`.
- `F.position(F.lit("SQL"), F.lit("Spark SQL"), F.lit(4))` — Spark `7`; repark
  `TypeError: position() takes from 1 to 2 positional arguments but 3 were given`
  (no three-argument start form).
- `F.split_part(F.lit("a,b,c"), F.lit(","), F.lit(0))` — both error, different
  classes: Spark `SparkRuntimeException [INVALID_INDEX_OF_ZERO]` SQLSTATE 22003;
  repark `PySparkException: split_part: the index 0 is invalid`.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-5-functions-strings-b-regex
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The 33-name roster was checked against the base backlog; 27 names have COVERS plus value assertions that hold on both engines; the six drops keep both measured values in this ledger.
      artifacts: [docs/examples/functions/slice.py, docs/examples/functions/regex.py, docs/examples/backlog.txt, scripts/check_example_coverage.py]
    - id: AT-2
      status: ATTACKED
      evidence: Every kept name is exercised on NULL plus the empty string or empty bytes; overlay default-length, substring negative position, like backslash-escape, and utf8 invalid sequences are in the files.
      artifacts: [docs/examples/functions/slice.py, docs/examples/functions/like.py, docs/examples/functions/utf8.py, docs/examples/functions/words.py]
    - id: AT-3
      status: ATTACKED
      evidence: Engine refusals and mismatched error surfaces were measured per input and dropped rather than asserted; F.elt out-of-range is NULL on repark and INVALID_ARRAY_INDEX on Spark.
      artifacts: [task/ledgers/staging/ex-5-functions-strings-b-regex-ledger.md]
    - id: AT-4
      status: N/A
      justification: Local single-session example scripts; no shared mutable state and no concurrent writers.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, or network. Examples use a local ReparkSession master local[1] only.
      artifacts: [docs/examples/functions/slice.py, docs/examples/functions/like.py]
    - id: AT-6
      status: ATTACKED
      evidence: Asserted cells are Spark's values at the file's exact inputs; a Spark-disagreeing cell was not written. Backlog ratchet is down-only, 844 to 817 at dispatch and 605 to 578 as merged.
      artifacts: [docs/examples/backlog.txt, scripts/check_example_coverage.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: Documentation examples over a six-row frame; no system-breaking resource path.
    - id: AT-8
      status: ATTACKED
      evidence: F.replace is a silent facade-spelling class (bare str is a literal here, a column name on PySpark, UNRESOLVED_COLUMN) disclosed in the ledger, §8 drop-in class, no pin; F.split / F.sentences / F.regexp_extract are disclosed engine gaps; F.validate_utf8 raises the same error class with a different Python type.
      artifacts: [task/ledgers/staging/ex-5-functions-strings-b-regex-ledger.md]
    - id: AT-9
      status: N/A
      justification: Example scripts exit nonzero on a mismatch; there is no service to observe.
    - id: AT-10
      status: ATTACKED
      evidence: The coverage gate execute leg runs every example; swapping the eight files onto live Spark is 8/8 pass; FN-ELT-1 / FN-REGEX-POSIX-1 / FN-LIKE-ESCEND-1 pins assert repark's current answer so a fix reds them.
      artifacts: [scripts/check_example_coverage.py, python/repark/tests/test_fn_elt_out_of_range.py, python/repark/tests/test_fn_regex_posix_class.py, python/repark/tests/test_fn_like_escape_end.py]
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)
