# Revalidation ledger — PR-245 · SQP-1 integration with exact source-size ratchets

**Date:** 2026-08-26 · **Branch:** `feat/sqp-1-spark-string-literals` · **PR:** #245 ·
**SEPMO path:** STANDARD · **Risk tier:** standard

**Retired:** this ledger moved to `../completed/` in PR #245's final revalidation commit.

**Scope boundary:** this unit revalidates SQP-1 after the current `main` integration and clears the
exact source-size ratchets without slack. It does not change dependencies, the owned Iceberg fork,
CI workflow policy, or the frozen completed SQP-1 ledger.

## Frozen proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The public Spark SQL door preserves and re-measures single-quoted escapes, raw prefixes, adjacent literals, comments, and exact error positions | Focused pins plus a pinned-live PySpark transcript for each class | **PROVEN** |
| C-002 | `CAST` and `TRY_CAST AS BINARY` preserve Spark-legal values and Arrow types; illegal source types preserve the named error contract | Legal value/type pins and illegal-source error pins | **PROVEN** |
| C-003 | The ANSI-door controls remain unchanged; facade DataFrame and SQL paths remain equal in value and Arrow type | Entry-point control pins | **PROVEN** |
| C-004 | Current `main`'s exact source-size gates integrate without increasing an exception: each touched oversized Python file is line-neutral or smaller than its `main` baseline, and every remaining exception records its exact final count | Main-baseline comparison, exact table identity, and source-size gates | **PROVEN** |
| C-005 | Every new or changed Rust source stays at or below 1,000 lines and keeps one cohesive responsibility | Exact line census and Rust source-size gate | **PROVEN** |
| C-006 | The enumerated shipped Python SQL-literal helper calls match the pinned inventory; the conventions gate rejects direct `replace` calls whose constant arguments evaluate to one quote and two quotes | AST inventory pin plus red/green bypass provocation | **PROVEN** |
| C-007 | Changed code comments and documentation comments contain only durable reasons or invariants; a local block uses at most two lines where feasible and contains no generated-model footer or implementation narrative | Diff review and text scan | **PROVEN** |
| C-008 | The completed SQP-1 ledger and its original pin family remain byte-intact; revalidation uses a separate pin family | Blob-identity pin and distinct test-name pin | **PROVEN** |
| C-009 | Maps, ledger navigation, and current-main integration remain truthful and diff-clean | Map, ledger, and diff-scope gates | **PROVEN** |
| C-010 | Focused, workspace, facade, and pinned-live parity evidence covers each affected Spark-visible class | Command records with exact inputs, outputs, and exit codes | **PROVEN** |

VERDICT: PASS iff OPEN=0 and REJECTED=0. LOGIC_SCORE = 10/10.

## Finite verification domain

- Spark literals: standard escapes, unknown escapes, quote escapes, raw prefixes, adjacency,
  comments between literals, and line/column errors.
- Binary casts: string, null, binary, and `TRY_CAST` legal paths; numeric, decimal, boolean, and
  date illegal paths; value and Arrow type on legal paths.
- Controls: ANSI literal behavior; facade DataFrame and facade SQL equality.
- Ratchets: the six main-baseline growth failures named in C-004, both stale shrink rows, every
  changed Rust source, and every Python SQL-embed site guarded by C-006.

## Proportionality rubric

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-pr-245-revalidation
  pr_unit: pr-245-revalidation
  criteria:
    blast_radius: FAIL (parser front door, Rust cast planning, facade embeds, and repository gates)
    reversibility: PASS (source and test changes revert without data migration)
    size: FAIL (more than five files and more than 150 changed lines)
    novelty: PASS (no dependency, external call, or architectural pattern)
    sensitivity: PASS (no security, money, catalog commit, or data-write behavior changes)
    clarity: PASS (ten frozen clauses; zero OPEN or REJECTED)
  path: STANDARD
  recorded_by: Orchestrator
```

## Actor build record

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-start
  agent: Actor
  action: revalidate SQP-1 against current main and clear every exact source-size ratchet without slack
  charter_trace: C-001..C-010
  preconditions:
    - charter frozen: SATISFIED (the Orchestrator handed ten exact clauses on 2026-08-26)
    - merge conflicts resolved: SATISFIED (git reports staged integration changes and no unmerged paths)
    - original record protected: SATISFIED (completed SQP-1 ledger is outside the writable unit scope)
    - gate failures enumerated: SATISFIED (six growth rows and two stale shrink rows are named in C-004)
  success_condition: all ten clauses have load-bearing pins and every recorded focused, parity, facade, workspace, map, ledger, and size command exits zero
  step_risks:
    - compaction changes facade behavior: HANDLED(existing SQP-1 pins stay intact and revalidation adds public-door controls)
    - a baseline update grants slack: HANDLED(each row must equal the final splitlines count)
    - facade SQL bypasses the canonical helper: HANDLED(the fail-closed convention rule gets a mutation probe)
    - current-main changes are mistaken for unit changes: HANDLED(diff review uses both main...HEAD and the staged merge)
  contingencies:
    - a behavior pin fails: EXECUTABLE(reproduce and correct only the affected SQP-1 path)
    - a source-size row differs: EXECUTABLE(compact the named file or ratchet to its exact lower count)
    - a broad gate finds unrelated environment drift: EXECUTABLE(compare the same gate with current main and record the evidence)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

## Actor evidence

### Exact source-size integration

`python3 scripts/check_lib_py.py` initially exited 1 with the eight current-main ratchet findings.
The final exact counts are:

| Path | Main baseline | Final |
|---|---:|---:|
| `python/repark/src/repark/spark/dataframe/plan_collapse.py` | 1422 | 1418 |
| `python/repark/src/repark/spark/dataframe/writer_readwriter.py` | 1406 | 1406 |
| `python/repark/src/repark/spark/functions.py` | 2033 | 2030 |
| `python/repark/src/repark/spark/ml/feature/_transformers.py` | 2763 | 2762 |
| `python/repark/src/repark/spark/session/_funcs.py` | 8390 | 8387 |
| `python/repark/tests/test_functions_gt2.py` | 1050 | 1050 |
| `python/repark-parity/bench/tpcds/runner.py` | 1263 | 1263 |
| `python/repark-parity/bench/tpch/runner.py` | 1780 | 1780 |

The changed Rust production files are 482 lines (`spark_literals.rs`), 683 lines
(`spark_ast.rs`), and 454 lines (`router.rs`). `check_lib_py.py` and
`check_rust_file_size.py` both exit 0 with exact baselines.

Mutation: one blank line made `plan_collapse.py` 1419 lines. The Python size guard exited 1 and
named its exact 1418 baseline. Removing the line restored exit 0.

### Pinned live PySpark oracle

Command, with stdout and stderr kept separate:

```text
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  .venv/bin/python /tmp/pr245_oracle.py \
  > /tmp/pr245-oracle.stdout 2> /tmp/pr245-oracle.stderr
exit 0
```

The script creates `SparkSession.builder.master("local[1]")` and collects these exact inputs:

```text
banner: Spark 4.1.2; timezone America/New_York; ansi=true; escapedStringLiterals=false
SELECT '\d', '\\d', 'a\nb'
  -> ["d", "\\d", "a\nb"]; schema string,string,string
SELECT r'\d', 'ab' 'cd'
  -> ["\\d", "abcd"]; schema string,string
SELECT /* lead */ '\d' AS value -- tail
  -> ["d"]; schema string
SELECT CAST('abc' AS BINARY), TRY_CAST('abc' AS BINARY), CAST(NULL AS BINARY),
       hex(CAST('\t' AS BINARY))
  -> [616263, 616263, NULL, "09"]; schema binary,binary,binary,string
CAST(CAST(1 AS INT) AS BINARY)
  -> DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION
TRY_CAST(CAST(1 AS INT) AS BINARY), CAST(DECIMAL AS BINARY),
CAST(BOOLEAN AS BINARY), CAST(DATE AS BINARY)
  -> DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION
SELECT 1 AS ok,\n'a\' AS broken
  -> PARSE_SYNTAX_ERROR at line 2, pos 0
```

Classification: every measured row matches the SQP-1 contract. No product bug, disposed
divergence, stale claim, or unestablished conclusion remains in this affected surface.

### Revalidation pins and mutation evidence

- `test_pr_245_revalidation.py` covers C-001/C-002/C-003/C-010 through the public Spark SQL,
  ANSI, and facade DataFrame doors. It collects values and Arrow types.
- `test_pr_245_revalidation_record.py` covers C-004..C-009 with exact line counts, Rust ceilings,
  a bypass detector, six frozen SHA-256 values, comment markers, and map rows.
- Removing the Spark-front-door canonicalizer made
  `unescape_covers_the_spark_escape_domain` exit 101 on `SELECT '\''`. Restoring it returned all
  11 literal tests to green.
- Removing the binary-cast rewrite made `cast_string_to_binary_plans_and_round_trips` exit 101
  with `Unsupported SQL type BINARY`. Restoring it returned all five binary tests to green.
- A temporary `scripts/pr245_guard_mutation.py` containing raw quote doubling made
  `check_python_conventions.py` exit 1 at its exact line. Removing it restored exit 0.
- Adding one line to a frozen-file hash input or changing one expected digest makes
  `test_pr245_original_sqp_record_and_pin_family_are_byte_frozen` fail. The six frozen hashes
  match and `git diff --quiet HEAD -- <frozen family>` exits 0.

Focused green evidence after every mutation was restored:

```text
cargo test -p repark-spark spark_string_literals --lib: 11 passed, exit 0
cargo test -p repark-spark cast_binary --lib: 5 passed, exit 0
cargo test -p repark-sql --test ansi_door_string_literals: 1 passed, exit 0
pytest revalidation + original SQP runtime/static files: 25 passed, exit 0
check_lib_py.py: 395 files clean, exit 0
check_python_conventions.py: 183 files clean, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_ledger_grammar.py: 7 live ledgers, 69 clauses, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers, 555 links, exit 0
```

Broad and parity evidence:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
make py-test-facade: 3,736 passed, 71 skipped, 44 warnings, exit 0
PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q: 419 passed, exit 0
```

The first sandboxed `make verify` invocation exited 2 because `uv` could not lock the shared
cache. The same command ran with approved cache access and exited 0. The first parity-record run
also failed as intended when the CAP-1 census pin exposed four stale pre-merge baselines. The pin
now records 1418, 2030, 2762, and 8387; the unchanged rerun exits 0.

`make parity-live PARITY_LIVE_JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` was interrupted to bound the
broad run. It reached 3,829 executed tests in 281.04 seconds and exited 130. Its three reported
failures are interruption effects: the Spark gateway received the signal during two
`applyInPandas` collections, and the signal handler then affected one cache test. The focused live
oracle above exited 0 before this run and covers every behavior changed by this unit.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-final
  agent: Actor
  action: verify the integrated SQP-1 behavior, exact ratchets, canonical SQL helper, and frozen records
  charter_trace: C-001..C-010
  preconditions:
    - all mutations restored: SATISFIED
    - exact exception table equals live files: SATISFIED
    - original SQP-1 family byte-identical: SATISFIED
    - live Spark oracle measured: SATISFIED
  success_condition: all focused, parity, facade, workspace, size, map, and ledger gates exit zero
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PASS
  escalation: "—"
```

```yaml
ACTOR_BUILD_SUMMARY:
  id: ABS-pr245-revalidation
  clauses:
    C-001: public Spark SQL literal, prefix, adjacency, comment, and position pins plus live oracle
    C-002: binary legal value/type and illegal error-condition pins plus live oracle
    C-003: ANSI and facade DataFrame/SQL equality controls
    C-004: exact final Python baselines and line-neutral-or-smaller main comparison
    C-005: changed Rust sources remain cohesive and below 1,000 lines
    C-006: canonical helper routing plus fail-closed mutation probe
    C-007: durable compact comment scan
    C-008: six byte-frozen original artifacts plus separate revalidation pins
    C-009: map, ledger, source-size, and diff checks
    C-010: focused, workspace, facade, parity-record, and live-oracle evidence
  verdict: PASS
```

Disk before artifact work and before broad validation: 652 GiB free on `/dev/nvme1n1p1`. The
worktree uses the repository target and uv caches. No cache, worktree, or uncommitted user file was
deleted. The task-owned oracle script and logs remain under `/tmp` until handoff.

This staging record contains no review attestation.

## Actor remediation plan — 2026-08-26

- [x] Reproduce the shrinking-escape downstream location error and the `chr(39)` guard bypass.
- [x] Preserve a canonical-to-original character map and translate only downstream SQL parser
  locations; keep tokenizer errors and the borrowed path unchanged.
- [x] Replace the SQL quote-doubling text matcher with an AST rule over enumerable constant quote
  arguments and pin the shipped SQL-embed inventory.
- [x] Compact the router front-door comment and replace the universal comment record with scoped
  assertions.
- [x] Re-measure live Spark, run mutation RED/GREEN proofs, update maps and exact line baselines,
  then run focused and broad gates.

## Actor remediation evidence — 2026-08-26

### Original-source parser locations

`canonicalize` still returns `Cow<str>`. Owned rewrites now retain one original `Location` for
each canonical character and for EOF. On an error, the router rebuilds this map and changes only a
downstream `ParserError` location. It leaves non-SQL errors, tokenizer errors, malformed location
messages, the borrowed path, and errors after metadata or time-travel text rewrites unchanged.

Public-door RED before the repair:

```text
.venv/bin/python -m pytest \
  python/repark/tests/test_pr_245_revalidation.py::test_pr245_downstream_error_uses_original_sql_position -q
2 failed, 1 passed, exit 1
SELECT '\u0061' + ): expected Column 19, got Column 14
SELECT '\u0061' AS ok,\n  '\u0062' + ): expected Column 14, got Column 9
```

After the repair, `test_pr_245_revalidation.py` has 6 passing tests. The three position rows are
`Line 1, Column 19`, `Line 2, Column 7`, and `Line 2, Column 14`. The original tokenizer-origin
pin remains `Line 2, Column 1`. The focused original and remediation facade families have 18
passing tests.

Live command, with streams separated:

```text
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  .venv/bin/python /tmp/pr245_oracle.py \
  > /tmp/pr245-oracle-remediation.stdout 2> /tmp/pr245-oracle-remediation.stderr
exit 0
banner: Spark 4.1.2; timezone America/New_York
SELECT '\u0061' + ) -> PARSE_SYNTAX_ERROR, line 1 pos 18
SELECT '\u0061' AS ok,\n  1 + ) -> PARSE_SYNTAX_ERROR, line 2 pos 6
SELECT '\u0061' AS ok,\n  '\u0062' + ) -> PARSE_SYNTAX_ERROR, line 2 pos 13
```

Spark positions are zero-based. The public RePark message uses one-based `Column`, so the pins use
19, 7, and 14.

### Enumerable Python guard and scoped comment record

The novel unit RED returned `[]` for
`value.replace(chr(39), chr(39) * 2)`. The AST evaluator now handles string constants,
`chr(<integer>)`, constant addition, and repetition bounded to two. The repaired unit returns
`[1]`. A real gate provocation added `scripts/pr245_guard_mutation.py` with that expression:

```text
python3 scripts/check_python_conventions.py
ERROR: scripts/pr245_guard_mutation.py:3 directly calls replace with constant one-quote and
two-quote arguments.
python-conventions: FAIL — 1 violation(s) across 184 files
exit 1
```

Deleting the task-owned provocation restored 183 clean files and exit 0. The record pin inventories
14 shipped files and every call to `sql_string_literal`, `_sql_string_literal`, or
`escape_sql_single_quotes` under the shipped facade and benchmark trees. It does not claim to
recognize every possible semantic SQL construction.

The router call-site comment is exactly two lines. It records the front-door exactly-once reason
and original-location invariant. The record test asserts only that local block; it makes no
universal claim about comments in other files.

Final changed-file counts are 582 (`spark_literals.rs`), 472 (`router.rs`), 317
(`check_python_conventions.py`), 113 (`test_pr_245_revalidation.py`), and 156
(`test_pr_245_revalidation_record.py`). All remain below 1,000, so no exception baseline changed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-remediation
  agent: Actor
  action: preserve original parser locations and close the enumerable Python guard bypass
  charter_trace: C-001, C-006, C-007, C-010
  preconditions:
    - novel public-door location RED reproduced: SATISFIED
    - novel chr(39) bypass RED reproduced: SATISFIED
    - original SQP-1 files remain frozen: SATISFIED
  success_condition: public positions match live Spark, guard provocation bites, scoped pins and all gates pass
  step_risks:
    - tokenizer errors are remapped: HANDLED(translation accepts only ParserError::ParserError)
    - secondary SQL rewrites use the wrong map: HANDLED(translation runs only when metadata and time-travel return borrowed text)
    - constant evaluation executes code or allocates without bound: HANDLED(AST whitelist and repetition ceiling two)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PASS
  escalation: "—"
```

Focused gates before broad validation:

```text
cargo clippy --locked -p repark-spark --lib -- -D warnings -A clippy::disallowed_methods: exit 0
cargo test -p repark-spark spark_string_literals --lib: 11 passed, exit 0
pytest original + remediation facade files: 18 passed, exit 0
pytest remediation record + frozen record + CAP census: 34 passed, exit 0
check_lib_py.py: 395 files clean, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_python_conventions.py: 183 files clean, exit 0
check_ledger_grammar.py: 7 live ledgers clean, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers and 555 links clean, exit 0
```

Broad remediation gates:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
git diff --check: exit 0
frozen SQP-1 family git diff --quiet: exit 0
```

The first sandboxed `make ci` reached the docstring gate and exited 2 only because `uv` could not
create its shared-cache lock file. The identical approved-cache run exited 0. Final disk headroom
is 652 GiB free on `/dev/nvme1n1p1` at 63% used. The task kept the shared build and dependency
caches and the `/tmp/pr245-oracle*` evidence files; it deleted only its temporary guard provocation.

## Actor second remediation plan — 2026-08-26

- [x] Reproduce newline-expansion, mixed-region, EOF, and arithmetic-`chr` failures through their
  public or mechanical entry points.
- [x] Preserve exact original location progress through expanded replacement text and bridge the
  downstream typed EOF parser error to original EOF.
- [x] Add a bounded, non-executing integer-expression evaluator for the enumerated `chr` forms and
  pin its rejection boundaries plus receiver-blind behavior.
- [x] Re-measure the three diagnostic positions against live PySpark 4.1.2 and run RED/GREEN
  regression proofs.
- [x] Update maps and evidence, run focused and broad gates, then stage every final byte.

## Actor second remediation evidence — 2026-08-26

### Downstream diagnostic translation

The public RED added expansion, mixed expansion plus shrink, and unlocated EOF rows to
`test_pr245_downstream_error_uses_original_sql_position`:

```text
pytest ...::test_pr245_downstream_error_uses_original_sql_position -q
3 failed, 3 passed, exit 1
SELECT '\n' AS shifted, ): got Line 2, Column 15; expected Line 1, Column 25
SELECT '\u0027' AS expanded, '\u0061' AS shrunk, ): got Column 41; expected 50
SELECT '\u0061' +: no location; expected Line 1, Column 18
```

The source map now traverses canonical line changes and retains exact locations for copied source.
The error translator walks direct SQL, context, diagnostic, collection, and shared wrappers. It
also translates diagnostic spans. A multiply referenced shared parser error first exited 101 at
canonical Column 14; the clone-safe wrapper translation now returns original Column 19. Typed
tokenizer errors and unlocated non-EOF parser errors remain unchanged. The EOF bridge applies only
to `ParserError::ParserError` text ending exactly in `found: EOF`; it appends mapped original EOF.

The repaired Rust translation family has 4 passing tests. The public position family has 6 passing
rows, and the full public revalidation file has 9 passing tests. Translation remains disabled after
metadata or time-travel changes the SQL text because those secondary rewrites have no composed map.

Pinned live command, with streams separate:

```text
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  .venv/bin/python /tmp/pr245_oracle.py \
  > /tmp/pr245-oracle-second-remediation.stdout \
  2> /tmp/pr245-oracle-second-remediation.stderr
exit 0
banner: Spark 4.1.2; timezone America/New_York
SELECT '\n' AS shifted, ) -> PARSE_SYNTAX_ERROR, line 1 pos 24
SELECT '\u0027' AS expanded, '\u0061' AS shrunk, ) -> line 1 pos 49
SELECT '\u0061' + -> PARSE_SYNTAX_ERROR, line 1 pos 17
```

Spark positions are zero-based. RePark's public message is one-based, so the pins use Columns 25,
50, and 18. The first sandboxed oracle attempt exited 1 because loopback bind was prohibited; the
identical loopback-enabled run above exited 0.

### Bounded receiver-blind AST guard

The integer evaluator accepts literals, unary `+`/`-`, and binary `+`/`-` to depth four. Every
intermediate absolute value is at most `0x10FFFF`. The string evaluator retains constant strings,
bounded concatenation, `chr`, and repetition counts zero through two. It executes no AST node.
The replace rule deliberately ignores the receiver and accepts exactly two positional arguments.

The reported arithmetic class uses `chr(0x28 - 1)`, which evaluates to quote code point 39. Its
record pin was RED (`[]`) before the evaluator and GREEN (`[1]`) after it. `chr(0x20 - 1)` evaluates
to 31, so the false-positive boundary pins it as a miss. Other misses cover booleans, invalid or
malformed `chr`, names, calls, division, excessive depth, repetition by three, and three-argument
`replace`. The 14-file shipped helper-call inventory remains exact.

Real gate provocation:

```text
scripts/pr245_arithmetic_guard_mutation.py:3
escaped = value.replace(chr(0x28 - 1), chr(0x28 - 1) * 2)
python3 scripts/check_python_conventions.py
python-conventions: FAIL — 1 violation(s) across 184 files
exit 1
```

Deleting the task-owned provocation restored 183 clean files and exit 0.

Final source counts are 787 (`spark_literals.rs`), 472 (`router.rs`), 333
(`check_python_conventions.py`), 116 (`test_pr_245_revalidation.py`), and 179
(`test_pr_245_revalidation_record.py`). All are below 1,000; no exception baseline changed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-second-remediation
  agent: Actor
  action: translate expanded, mixed, wrapped, and EOF parser diagnostics and close the bounded arithmetic chr bypass
  charter_trace: C-001, C-006, C-010
  preconditions:
    - three public diagnostic REDs reproduced: SATISFIED (3 failed and 3 passed before repair)
    - arithmetic quote-code bypass reproduced: SATISFIED (detector returned an empty hit list)
    - pinned PySpark oracle available: SATISFIED (Spark 4.1.2 banner and exact positions recorded)
    - original SQP-1 family remains frozen: SATISFIED (six-file git diff is empty)
  success_condition: all six public position rows match live Spark, every parser wrapper pin passes, the guard bites and its rejection boundary stays clean, and focused plus broad gates exit zero
  step_risks:
    - expansion creates a false source line: HANDLED(canonical traversal maps copied error tokens to original locations)
    - one wrapper hides the parser error: HANDLED(direct, context, diagnostic, collection, unique and multiply shared pins)
    - EOF text overmatches another error: HANDLED(typed parser variant plus exact terminal found-EOF contract)
    - constant evaluation executes or allocates without bound: HANDLED(AST whitelist, depth/value limits, repetition ceiling two)
    - receiver restriction creates a miss: HANDLED(receiver-blind rule and explicit unrelated-receiver hit pin)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

Focused evidence:

```text
cargo clippy --locked -p repark-spark --lib -- -D warnings -A clippy::disallowed_methods: exit 0
cargo test -p repark-spark location_translation_tests --lib: 4 passed, exit 0
cargo test -p repark-spark spark_string_literals --lib: 11 passed, exit 0
pytest public revalidation + frozen SQP facade: 21 passed, exit 0
pytest static revalidation + frozen SQP record: 12 passed, exit 0
check_lib_py.py: 395 files clean, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_python_conventions.py: 183 files clean, exit 0
check_ledger_grammar.py: 7 live ledgers clean, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers and 555 links clean, exit 0
```

Broad evidence before this evidence append:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
```

The first corrected `make ci` run found only Ruff formatting in the new static boundary test; its
exact formatter diff was applied and the full rerun exited 0. The first `make verify` run reached
the docstring gate before sandboxed uv cache locking exited 2; the identical cache-enabled rerun
exited 0. Disk was 651 GiB free at both broad-gate boundaries. The task retains shared caches and
the task-owned oracle script plus separate stdout/stderr evidence under `/tmp`.

## Actor third remediation plan — 2026-08-26

- [ ] Reproduce aliased mixed-collection translation loss and invalid diagnostic-span deletion in
  Rust tests.
- [ ] Make shared error translation compositional across every wrapper while preserving unsupported
  siblings and unmappable spans unchanged.
- [ ] Replace recursive text evaluation with a depth-, node-, and output-budgeted iterative walk.
- [ ] Pin deep constructed concatenation, nested repetition, allocation bounds, and the existing
  arithmetic, boolean, receiver, malformed-call, and arity behavior.
- [ ] Run focused stress probes, source/convention/map/ledger gates, broad gates, and stage all bytes.

## Actor third remediation execution — 2026-08-27

F-pr245-005 was reproduced by two Rust REDs. An aliased
`Shared(Collection([parser, NotImplemented]))` kept canonical Column 14, and an out-of-range note
span changed from `Some(Span(99,99))` to `None`; the focused command exited 101 with 2 failures.
Shared-tree cloning now retains the string-backed non-parser variants that can occur beside a
parser error. The mixed collection translates its parser child to original Column 19, preserves
the unsupported sibling's exact rendered value, and leaves the retained Arc at canonical Column
14. Main, note, and help spans map when valid and retain their original `Some(Span)` when invalid.

F-pr245-006 replaces recursive text evaluation with an explicit stack. Depth 16, 64 visited nodes,
and two output characters bound the whitelist. Concatenation and repetition calculate output
length before allocation; integer expressions retain their independent depth-four and Unicode
range bounds. A parsed 2,000-term concatenation and a constructed 30-level repetition both return
a safe miss. Raising only the text depth and node budgets made the stress pin RED (`"'" is None`),
and restoring the bounds made it GREEN in 0.19 seconds with 33,280 KiB maximum RSS.

Final third-cycle source counts are 882 (`spark_literals.rs`), 360
(`check_python_conventions.py`), and 224 (`test_pr_245_revalidation_record.py`). Each remains below
the 1,000-line default, so no source-size exception changed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-third-remediation
  agent: Actor
  action: make aliased diagnostic translation child-compositional and bound constant text evaluation before allocation
  charter_trace: C-001, C-006, C-007, C-010
  preconditions:
    - aliased mixed-collection RED reproduced: SATISFIED (canonical Column 14 remained; exit 101)
    - invalid span deletion RED reproduced: SATISFIED (Some Span became None; exit 101)
    - deep evaluator stress input is syntactically parseable: SATISFIED (ast.parse produced the 2,000-term call)
    - original SQP-1 family remains frozen: SATISFIED (no frozen-file edit in this cycle)
  success_condition: each cloneable aliased collection child retains its own meaning, no diagnostic span is deleted, bounded text forms detect quotes, and deep forms miss without recursion or large allocation
  step_risks:
    - retained Arc changes through translation: HANDLED(retained clone stays at canonical Column 14)
    - unsupported sibling blocks parser translation: HANDLED(NotImplemented sibling and parser child pinned separately)
    - invalid span disappears: HANDLED(main, note, and help valid/out-of-range pins)
    - concat or repetition allocates before refusal: HANDLED(length check precedes both operators)
    - constructed AST executes arbitrary code: HANDLED(AST whitelist calls only built-in chr after bounded integer evaluation)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

Focused evidence before broad gates:

```text
cargo test -p repark-spark location_translation_tests -- --nocapture: 6 passed, exit 0
cargo test -p repark-spark spark_string_literals --lib: 11 passed, exit 0
pytest PR-245 static plus public revalidation: 18 passed, exit 0
pytest constructed-tree stress: 1 passed in 0.04s; elapsed 0.19s; max RSS 33,280 KiB
check_python_conventions.py: 183 files clean, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_lib_py.py: 395 files clean, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers and 555 links clean, exit 0
check_ledger_grammar.py: 7 live ledgers clean, exit 0
```

All five third-remediation plan items completed. Broad evidence:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
```

The first two sandboxed `make verify` attempts stopped at the docstring gate with exit 2 because
uv could not write its home cache and tool locks. The identical cache-enabled run passed. The
broad-gate disk check reported 651 GiB free. No cleanup removed shared caches or build artifacts;
the incremental target remains for the open merge's remaining validation.

## Actor fourth remediation plan — 2026-08-27

- [ ] Pin DataFusion's actual Spark passthrough parser boundary as a direct typed SQL error.
- [ ] Remove synthetic wrapper cloning and translate only the reachable direct parser error;
  preserve every other error tree unchanged.
- [ ] Reproduce a real 10,000-term valid Python file raising a parser resource error.
- [ ] Catch parser resource failures as one controlled file diagnostic while retaining normal valid
  and syntax-error behavior.
- [ ] Update maps and Actor evidence, run focused and broad gates, and stage every final byte.

## Actor fourth remediation execution — 2026-08-27

### Reachable diagnostic contract

C-001 and C-010 error-position evidence quantifies over the Spark passthrough parser's reachable
typed boundary: DataFusion `SessionState::sql_to_statement` converts `ParserError` directly to
`DataFusionError::SQL`. The router calls this boundary before planning. Planning, execution,
diagnostic, collection, and shared trees occur outside that parser conversion and are not location
translation inputs. The earlier synthetic wrapper evidence is superseded by this boundary pin.

The production translator now matches only direct `DataFusionError::SQL(ParserError::ParserError)`.
It leaves tokenizer errors and every non-direct tree unchanged. The retained mixed shared-tree pin
checks Arc pointer identity, so preservation does not depend on cloning or stringifying any error.
This removes the clone framework and its unreachable heterogeneous-enum contract.

The narrowing pin was RED because the mixed shared tree returned a new Arc; the focused Rust run
exited 101 with 1 failure. The DataFusion boundary pin was already GREEN and proved the narrowing
precondition. After repair, all five location tests pass, including expansion/mixed mapping, direct
EOF mapping, direct boundary shape, tokenizer stability, and shared-tree identity.

### Python parser-resource gate

`check_file` now catches `RecursionError`, `MemoryError`, and `OverflowError` from `ast.parse` and
returns one controlled diagnostic. `SyntaxError` retains its existing diagnostic, and normal valid
files remain clean. No source-length ceiling was added because a byte or token count would reject
valid generated Python without proving AST risk; the typed parser exceptions are the exact boundary.

The permanent real-file pin writes a valid 10,000-term addition expression. Before repair it raised
`RecursionError` with a pytest traceback and exited 1. After repair, direct and full-gate paths each
return one diagnostic without a traceback. Memory and parser-overflow branches use the same real
file boundary with the parser failure injected before any AST consumer runs.

Temporary full-gate provocation:

```text
.venv/bin/python scripts/check_python_conventions.py
ERROR: scripts/pr245_parser_resource_probe.py exceeds Python parser resource limits (RecursionError) — refuse to pass closed
python-conventions: FAIL — 1 violation(s) across 184 files
exit 1
```

The task-owned probe was deleted with `apply_patch`; the clean gate reports 183 files and exit 0.
Final fourth-cycle counts are 706 (`spark_literals.rs`), 365
(`check_python_conventions.py`), and 273 (`test_pr_245_revalidation_record.py`). No source-size
exception changed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-fourth-remediation
  agent: Actor
  action: narrow location translation to the reachable typed parser boundary and fail closed on Python parser resource exhaustion
  charter_trace: C-001, C-005, C-006, C-007, C-009, C-010
  preconditions:
    - parser-bearing shape proved at production boundary: SATISFIED (SessionState sql_to_statement returns direct SQL ParserError)
    - unsupported shared trees are outside translation input: SATISFIED (router order plus pointer-identity pin)
    - real valid resource input reproduced: SATISFIED (10,000-term file raised RecursionError before repair)
    - original SQP-1 family remains frozen: SATISFIED (frozen hash pin passes)
  success_condition: reachable direct parser diagnostics map to original SQL, unrelated trees remain identical, and every named ast.parse resource failure becomes one controlled nonzero gate result
  step_risks:
    - narrowing drops a real wrapped parser error: HANDLED(actual SessionState boundary pin precedes planning)
    - unrelated errors are cloned or stringified: HANDLED(non-SQL match arm returns the owned value unchanged)
    - resource failure leaks a traceback: HANDLED(full main-path stderr is exactly two controlled lines)
    - broad complexity ceiling rejects valid generated code: AVOIDED(no heuristic ceiling added)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

Focused evidence before broad gates:

```text
cargo test -p repark-spark location_translation_tests --lib: 5 passed, exit 0
cargo test -p repark-spark spark_string_literals --lib: 11 passed, exit 0
pytest static plus public PR-245 revalidation: 19 passed, exit 0
check_python_conventions.py: 183 files clean, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_lib_py.py: 395 files clean, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers and 555 links clean, exit 0
check_ledger_grammar.py: 7 live ledgers clean, exit 0
```

Fourth-remediation broad evidence:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
```

Disk remained at 651 GiB free. The task retains the shared incremental target for the open merge;
no cache or worktree was deleted.

## Critic final disposition — 2026-08-27

The fifth independent CCC pass converged with no open S1-or-higher finding.

- `F-pr245-001` — **REMEDIATED**. Public expansion, mixed-region, multiline, EOF, no-change, and
  tokenizer controls match the original Spark SQL coordinates.
- `F-pr245-002` — **REMEDIATED**. The formatter-stable direct quote-doubling bypass is detected.
- `F-pr245-003` — **REMEDIATED**. The router comment contains two local durable invariants.
- `F-pr245-004` — **REMEDIATED**. Bounded arithmetic quote expressions detect code point 39;
  code point 31 remains a deliberate miss.
- `F-pr245-005` — **REMEDIATED**. The production contract is narrowed to the reachable direct
  `DataFusionError::SQL(ParserError::ParserError)` boundary. A boundary pin calls DataFusion
  `SessionState::sql_to_statement` and fails if that shape gains a wrapper. Planning, execution,
  collection, diagnostic, and shared errors remain untouched; the shared-tree pin holds Arc
  pointer identity.
- `F-pr245-006` — **REMEDIATED**. Iterative constant-text evaluation bounds depth, visited nodes,
  and output length before allocation.
- `F-pr245-007` — **REMEDIATED**. `ast.parse` resource failures become one controlled file
  diagnostic and a nonzero gate result. `KeyboardInterrupt` and `SystemExit` remain uncaught.

Fresh public/live input:

```text
SELECT hex(CAST('\000\001\032' AS BINARY)), length('x\r\ny'),
       r'\U0001F642', '\q' /* outer /* inner */ tail */ '\t'
RePark -> 00011A, 4, \U0001F642, q<TAB>; Arrow string, int32, string, string
Spark 4.1.2 -> identical values and types
```

The metadata/time-travel secondary-rewrite path remains deliberately outside source-location
translation because it has no composed map. This disclosed conservative residual predates the
final direct-parser contract and is not a branch defect. The interrupted full parity-live run is
also an execution residual, not branch evidence; focused live Spark probes and the broad gates pass.

Phase verdicts: Critic-1 quality **PASS**; Critic-2 safety/security **PASS**; Critic-3 pure logic
**PASS**; claims-versus-tree **PASS**. Final verdict: **CONVERGED**.

```yaml
STALE_COVERAGE_ATTESTATION:
  pr_unit: pr-245-revalidation
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-010 were checked against staged bytes and fresh public/live behavior.
      artifacts: [public PR-245 pins, Spark 4.1.2 transcript]
    - id: AT-2
      status: ATTACKED
      evidence: Expansion, shrinkage, mixed regions, EOF, malformed input, null, Unicode, raw, adjacency, and exact evaluator limits were exercised.
      artifacts: [test_pr245_downstream_error_uses_original_sql_position, test_pr245_sql_embed_guard_bounds_constructed_constant_trees]
    - id: AT-3
      status: ATTACKED
      evidence: Direct parser, tokenizer, unlocated, unrelated error-tree, SyntaxError, RecursionError, MemoryError, and OverflowError paths were checked.
      artifacts: [location_translation_tests, test_pr245_python_guard_controls_parser_resource_failures]
    - id: AT-4
      status: ATTACKED
      evidence: Cow ownership, router ordering, and shared Arc identity were verified.
      artifacts: [unsupported_shared_error_tree_stays_identical, router.rs]
    - id: AT-5
      status: ATTACKED
      evidence: SQL payloads remained literal data and the AST evaluator executes no arbitrary node.
      artifacts: [fresh public/live input, Python guard rejection controls]
    - id: AT-6
      status: ATTACKED
      evidence: Literal, binary, ANSI, Arrow type, frozen hash, and exact-ratchet compatibility passed.
      artifacts: [test_pr_245_revalidation.py, test_pr245_original_sqp_record_and_pin_family_are_byte_frozen]
    - id: AT-7
      status: ATTACKED
      evidence: Text evaluation is bounded before allocation and parser resource failures fail closed.
      artifacts: [check_python_conventions.py, constructed-tree and real-file stress pins]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion SessionState parser shape, Spark diagnostics, CAST contracts, and helper inventory were checked.
      artifacts: [spark_passthrough_parser_boundary_returns_direct_sql_error, test_pr245_shipped_sql_literal_helper_call_inventory_is_exact]
    - id: AT-9
      status: ATTACKED
      evidence: Caller-source parser positions and controlled convention-gate diagnostics remain actionable.
      artifacts: [public position pins, real main-path stderr pin]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation-derived regressions, boundary liveness, frozen pins, and fresh uncommitted probes cover each affected claim.
      artifacts: [five remediation cycles, focused and broad command records]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-5, AT-6, AT-7, AT-8, AT-9, AT-10]
  complete: true
  converged: true
```

Final Critic commands: location translation 5 passed; PR-245 Python 19 passed; frozen hashes and
exact ratchets 2 passed; source, convention, grammar, map, ledger, and `git diff --check main`
gates exited 0; `make verify` exited 0. Disk was 651 GiB free before and after validation.

## Actor delivery-return plan — 2026-08-27

- [ ] Reproduce the two preflight failures alone and after the earlier direct-parser rows.
- [ ] Prove DataFusion's exact typed error variants for direct and native expected-token failures.
- [ ] Translate only reachable `SQL` and `Diagnostic(SQL)` parser errors without cloning unrelated
  error trees.
- [ ] Base metadata/time-travel residual detection on changed SQL bytes, not `Cow` ownership.
- [ ] Add the ordered public regression, update maps/evidence, run focused and broad gates, and
  fully stage the completed ledger with re-review required.

## Actor delivery-return execution — 2026-08-27

**RE-REVIEW REQUIRED.** The preceding coverage attestation and convergence verdict describe bytes
before this delivery-return repair. This Actor section does not amend or renew that attestation.

The exact completed-tree PR file reproduced 2 failures and 7 passes. Both failing inputs also fail
without a contaminating predecessor, so session, catalog, and extension state are not the cause.
The differentiator is DataFusion's typed parser branch: ordinary sqlparser failures return direct
`DataFusionError::SQL`, while DFParser's native expected-token helper attaches a diagnostic and
returns `DataFusionError::Diagnostic(_, SQL)`. The two expansion inputs reach the latter branch.

The translator now handles exactly the reachable `SQL` and `Diagnostic(SQL)` variants. It maps the
diagnostic main/note/help spans when valid, retains an unmappable span unchanged, and translates the
inner parser message. Every other error tree remains owned and unchanged; no clone or stringify
framework returned. A Rust boundary pin runs the direct input and both diagnostic inputs through
the real `SessionState::sql_to_statement` boundary.

The router's secondary-rewrite residual now compares SQL bytes at both metadata and time-travel
boundaries. A distinct owned buffer with unchanged bytes keeps the original source map; changed
bytes disable it. The pure router pin covers both outcomes. Thus the disclosed residual applies
only after a real secondary SQL rewrite, not because a `Cow` happens to be owned.

The public ordered regression uses one session for direct SQL, newline expansion, and mixed
expansion/shrink errors. It was RED with canonical Line 2 Column 15 and Column 41 before repair.
After the native rebuild, the ordered subset passes all 7 rows and the full PR file passes 10.

Final delivery-return counts are 759 (`spark_literals.rs`), 476 (`router.rs`), 88
(`router/tests.rs`), and 133 (`test_pr_245_revalidation.py`). Every Rust source remains below
1,000 lines and no exception baseline changed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-delivery-return
  agent: Actor
  action: translate the reachable diagnostic parser branch and base secondary-rewrite residuals on byte changes
  charter_trace: C-001, C-005, C-007, C-009, C-010
  preconditions:
    - exact preflight failures reproduced: SATISFIED (2 failed, 7 passed)
    - predecessor contamination isolated: SATISFIED (both rows fail alone; input-specific typed branch found)
    - direct and diagnostic boundary shapes proved: SATISFIED (real SessionState parser pin)
    - original expectations retained: SATISFIED (Line 1 Columns 25 and 50 unchanged)
  success_condition: all public location rows map to caller SQL after direct-parser predecessors, unrelated trees remain unchanged, and only byte-changing secondary rewrites suppress translation
  step_risks:
    - diagnostic wrapper is mistaken for an unrelated later error: HANDLED(real DFParser expected-token construction and boundary pin)
    - span mapping deletes an invalid span: HANDLED(fallback retains the original span)
    - owned unchanged SQL loses its map: HANDLED(distinct-owned-equal-bytes router pin)
    - real metadata or time-travel rewrite gets a false map: HANDLED(changed-byte branch returns None)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED_TO_REVIEW
  escalation: "—"
```

Delivery-return focused and facade evidence:

```text
pytest test_pr_245_revalidation.py before repair: 2 failed, 7 passed, exit 1
cargo test -p repark-spark location_translation_tests --lib: 5 passed, exit 0
cargo test -p repark-spark router::tests:: --lib: 5 passed, exit 0
pytest exact predecessor/location subset: 7 passed, exit 0
pytest complete test_pr_245_revalidation.py: 10 passed, exit 0
make py-test-facade: 3743 passed, 71 skipped, 44 warnings, exit 0
check_rust_file_size.py: 277 files clean, exit 0
check_lib_py.py: 395 files clean, exit 0
check_python_conventions.py: 183 files clean, exit 0
make check-map-sync: 150 maps clean, exit 0
make check-ledgers: 146 ledgers and 555 links clean, exit 0
check_ledger_grammar.py: 7 live ledgers clean, exit 0
```

Delivery-return broad evidence:

```text
make ci: exit 0
make verify: exit 0 (all workspace tests and doctests passed)
```

Disk remained at 651 GiB free. The native editable build and shared incremental target remain for
the pending re-review; no cache, worktree, or uncommitted evidence was deleted.

## Critic delivery-return disposition — 2026-08-27

This section **supersedes the stale Critic disposition and coverage attestation above**. It reviews
the delivery-return bytes recorded in `SLR-pr245-actor-delivery-return`.

The T9 C-001/C-010 defect is **REMEDIATED**. DataFusion's real
`SessionState::sql_to_statement` boundary returns both reachable forms: direct
`DataFusionError::SQL(ParserError)` and expected-token
`DataFusionError::Diagnostic(_, SQL(ParserError))`. The translator maps both forms. It maps valid
diagnostic main, note, and help spans and preserves invalid spans unchanged. Owned planning and
execution errors retain their values. Shared and collection trees retain their original identity.

Source-map retention now compares SQL bytes at both secondary-rewrite boundaries. An owned buffer
with equal bytes keeps the map. A real metadata or time-travel byte change suppresses the map.
Therefore, the remaining secondary-rewrite residual applies only when SQL text actually changes.

The two exact former preflight failures pass alone (`2 passed`) and after the direct-parser
predecessor (`1 passed`, three ordered inputs). All ten public PR tests pass. A fresh reachable
`Diagnostic(SQL)` input combines newline expansion, Unicode shrinkage, and quote shrinkage before
the error:

```text
SELECT '\n' AS expanded, '\u0061' AS shrunk, '\u0027' AS quote, )
RePark: Line 1, Column 65
Spark 4.1.2: line 1, position 64
```

No open S1-or-higher finding remains. Critic-1 quality **PASS**; Critic-2 safety/security **PASS**;
Critic-3 pure logic **PASS**; claims-versus-tree **PASS**. Final delivery-return verdict:
**CONVERGED**.

```yaml
STALE_COVERAGE_ATTESTATION:
  pr_unit: pr-245-revalidation-delivery-return
  supersedes: prior Critic coverage attestation in this ledger
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 and C-010 were rechecked against both reachable DataFusion parser variants and all ten public tests.
      artifacts: [spark_passthrough_parser_boundary_returns_only_reachable_parser_variants, test_pr_245_revalidation.py]
    - id: AT-2
      status: ATTACKED
      evidence: Direct, diagnostic, expansion, shrinkage, mixed-region, multiline, EOF, ordered-predecessor, and malformed boundaries were exercised.
      artifacts: [location_translation_tests, test_pr245_expanding_error_positions_survive_prior_direct_parser_error]
    - id: AT-3
      status: ATTACKED
      evidence: Valid and invalid diagnostic spans, tokenizer errors, unlocated errors, planning errors, execution errors, shared trees, and collections were attacked.
      artifacts: [translate_parser_error, translate_diagnostic, unsupported_shared_error_tree_stays_identical]
    - id: AT-4
      status: ATTACKED
      evidence: Session ordering, Cow ownership, owned-equal buffers, actual byte changes, and Arc identity were checked.
      artifacts: [original_sql_for_locations, source_locations_depend_on_rewrite_bytes_not_buffer_ownership]
    - id: AT-5
      status: ATTACKED
      evidence: Canonicalized payloads remained data, arbitrary AST nodes were not executed, and unrelated error trees were not cloned or stringified.
      artifacts: [fresh public/live diagnostic probe, Python convention controls]
    - id: AT-6
      status: ATTACKED
      evidence: Spark values, binary and ANSI controls, Arrow types, frozen hashes, and exact source ratchets remain compatible.
      artifacts: [public PR tests, frozen-hash and ratchet pins]
    - id: AT-7
      status: ATTACKED
      evidence: Constant evaluation and parser construction remain bounded or fail closed; no new system-breaking resource path was found.
      artifacts: [test_pr245_python_guard_controls_parser_resource_failures, make verify]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 parser construction, diagnostic wrapping, Spark position conventions, and public error contracts were verified.
      artifacts: [SessionState::sql_to_statement boundary pin, Spark 4.1.2 transcript]
    - id: AT-9
      status: ATTACKED
      evidence: Original caller coordinates survive both parser variants, while invalid diagnostic spans and unrelated errors remain observable unchanged.
      artifacts: [translate_diagnostic, public ordered regression]
    - id: AT-10
      status: ATTACKED
      evidence: The two former preflight failures were isolated and ordered, all public and facade tests passed, and a fresh Diagnostic(SQL) differential matched Spark.
      artifacts: [2 isolated tests, ordered regression, 3743-test facade run, fresh live probe]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-5, AT-6, AT-7, AT-8, AT-9, AT-10]
  complete: true
  converged: true
```

Final delivery-return Critic commands: isolated failures 2 passed; ordered regression 1 passed;
public PR file 10 passed; location translation 5 passed; Spark-literal pins 11 passed; router pins
5 passed; facade 3,743 passed with 71 skips and 44 warnings; frozen hashes and exact ratchets 2
passed; source, convention, grammar, map, ledger, and `git diff --check main` gates exited 0;
`make verify` exited 0. Disk was 651 GiB free before and after validation.

## Actor post-push C-009 delivery return — 2026-08-27

**RE-REVIEW REQUIRED.** The replacement Critic attestation above predates this navigation-test
repair. This Actor note does not amend or renew that attestation.

Delivery moved this ledger from `staging/` to `completed/`, but the navigation pin still required
the staging map to name it. The pin now requires the completed map entry, retains both test-home
map checks, and proves the staging map no longer names the delivered ledger. Production code and
the frozen SQP-1 family are unchanged.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-post-push-c009
  agent: Actor
  action: align the navigation pin with the ledger's completed lifecycle state
  charter_trace: C-008, C-009
  preconditions:
    - ledger lifecycle home is completed: SATISFIED (completed file and map entry exist)
    - staging navigation no longer names the ledger: SATISFIED
    - both test-home maps remain required: SATISFIED
  success_condition: the navigation pin describes the delivered tree without weakening test-family coverage
  step_risks:
    - a stale staging reference becomes accepted: HANDLED(explicit absence assertion)
    - a test-home map check is lost: HANDLED(both original test-map rows retained)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED_TO_REVIEW
  escalation: "—"
```

Post-push C-009 evidence:

```text
pytest exact navigation test: 1 passed, exit 0
make py-test: 423 passed, exit 0
make ci: exit 0
```

## Critic post-push C-009 disposition — 2026-08-27

This section supersedes the preceding active attestation. C-009 is **REMEDIATED** and the
post-push tree is **CONVERGED** with no open S1-or-higher finding.

The only functional byte change since the merge/revalidation commit is the lifecycle record test.
It retains both test-home map pins and requires `task/ledgers/completed/map.md` to name this ledger.
It separately rejects the stale filename in `task/ledgers/staging/map.md`. The touched parity-test map has one
required lockstep line that describes these navigation assertions. No production byte changed.

```yaml
STALE_COVERAGE_ATTESTATION:
  pr_unit: pr-245-revalidation-post-push-c009
  supersedes: delivery-return Critic coverage attestation in this ledger
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-009 was checked against the completed lifecycle state and exact three-file staged diff.
      artifacts: [test_pr245_navigation_names_the_separate_revalidation_family, parity-test map, completed ledger]
    - id: AT-2
      status: ATTACKED
      evidence: Positive completed-map presence and negative stale staging-map presence were both exercised.
      artifacts: [exact navigation test]
    - id: AT-3
      status: ATTACKED
      evidence: A missing completed link or retained staging link fails the assertion rather than passing silently.
      artifacts: [required map dictionary, explicit staging absence assertion]
    - id: AT-4
      status: N/A
      justification: The change is a read-only record assertion with no mutable or concurrent state.
    - id: AT-5
      status: N/A
      justification: The change adds no input, privilege, serialization, path-write, or execution surface.
    - id: AT-6
      status: ATTACKED
      evidence: Both test-home map pins remain required; the touched parity-test map truthfully describes the new navigation assertions.
      artifacts: [python/repark/tests/map.md, python/repark-parity/tests/map.md]
    - id: AT-7
      status: N/A
      justification: Four fixed file reads add no system-breaking resource behavior.
    - id: AT-8
      status: ATTACKED
      evidence: The assertion follows the repository ledger lifecycle contract after Delivery moved the file.
      artifacts: [completed map, staging map, ledger lifecycle gate]
    - id: AT-9
      status: ATTACKED
      evidence: Navigation now identifies the ledger's actual completed home and rejects the obsolete home.
      artifacts: [C-009 exact test, map-sync and ledger lifecycle gates]
    - id: AT-10
      status: ATTACKED
      evidence: The exact test and all 423 record-suite tests pass; the negative assertion catches stale staging navigation.
      artifacts: [1 exact test, make py-test]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-9, AT-10]
  complete: true
  converged: true
```

Post-push Critic commands: exact navigation test 1 passed; `make py-test` 423 passed; ledger
lifecycle, ledger grammar, map sync, and the diff check exited 0. The follow-up contains this
record test, its required parity-test map update, and this completed ledger.
Disk remained at 651 GiB free.

## Actor second post-push delivery return — 2026-08-27

**RE-REVIEW REQUIRED.** The post-push Critic attestation above predates this standalone-harness
repair. This Actor section does not amend or renew that attestation.

The parity CI environment intentionally excludes the RePark product package. TPC-H and TPC-DS
imported the product `_idents` module at runner import time, so clean `make py-test` failed before
collecting the record test. The dependency-free `repark_parity.sql` module now owns Generic SQL
quote-only escaping for parity code. Both runners and both datagens use it; no raw call-site
quote-doubling was added. The convention guard sanctions exactly the product and parity helper
files, and its shipped helper-call inventory remains exact.

Other parity-bench `repark` imports execute RePark workloads. They do not provide Generic SQL
escaping, so they remain outside this standalone import seam.

The clean subprocess pin blocks `repark` in `sys.modules`, imports both runners, and checks that
quotes double while a backslash remains unchanged. TPC-DS and TPC-H runners remain exactly 1,263
and 1,780 lines, so their source-size baselines stay line-neutral.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-pr245-actor-second-post-push
  agent: Actor
  action: restore dependency-free parity runner imports through one parity-local quote helper
  charter_trace: C-003, C-004, C-006, C-009, C-010
  preconditions:
    - clean no-project import failure reproduced: SATISFIED (ModuleNotFoundError for repark)
    - parity SQL use is quote-only Generic SQL: SATISFIED (DuckDB COPY and read_parquet paths)
    - all four parity call sites inventoried: SATISFIED (two runners and two datagens)
    - oversized runners remain line-neutral: SATISFIED (1,263 and 1,780)
  success_condition: parity CI imports both runners without the product package and all SQL path embeds use a sanctioned helper
  step_risks:
    - parity helper changes non-quote bytes: HANDLED(clean subprocess value pin)
    - a product dependency remains at module import: HANDLED(repark-blocked dual-runner import)
    - the guard rejects its own helper or permits another home: HANDLED(exact two-file guard pin)
    - helper inventory silently loses a call site: HANDLED(exact shipped-call inventory)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED_TO_REVIEW
  escalation: "—"
```

Second post-push evidence:

```text
clean no-project dual-runner import before repair: ModuleNotFoundError: repark, exit 1
focused product-blocked import, guard, and inventory pins: 3 passed, exit 0
clean no-project dual-runner import after repair: exit 0
clean CI-dependency parity suite from /tmp: 424 passed, exit 0
make py-test: 424 passed, exit 0
make ci: exit 0
make verify: exit 0 after one sandbox-only uv cache-lock retry
```

Disk remained at 651 GiB free. The shared incremental target and uv cache remain for review; no
cache, worktree, or uncommitted evidence was deleted.

## Critic second post-push parity-isolation disposition — 2026-08-27

This section supersedes the post-push C-009 Critic attestation. The parity-isolation defect is
**REMEDIATED**. No open S1-or-higher finding remains.

`repark_parity.sql.escape_sql_single_quotes` is the smallest helper that satisfies the Generic SQL
body contract. It doubles each single quote, adds no surrounding quotes, and leaves every other
character unchanged. Both TPC runners and both datagens import this standalone helper. Their
record types import without the RePark product package. The Spark-door helper remains in
`repark.spark._idents` and keeps its separate product contract.

The bench import census found 40 product-package imports. Thirty-nine are inside functions that
execute product workloads. The only module-level import is in the MW-7 measurement driver itself;
it is not a shared record or parity-harness dependency. A clean environment contained neither a
RePark import spec nor RePark package metadata and ran all 424 parity tests.

Fresh quote pressure covered empty text, one and three quotes, repeated backslashes, and a Unicode
path. The novel input `雪\\server\\o''clock/路径` produced
`雪\\server\\o''''clock/路径`. Backslashes and non-quote Unicode characters were byte-stable.
The product-blocked negative control failed with `ModuleNotFoundError` (exit 1), while the dual
runner pin passed (exit 0). Thus, the isolation pin fails if a product helper import returns.

Critic-1 quality **PASS**; Critic-2 safety/security **PASS**; Critic-3 pure logic **PASS**;
Critic-4 claims-versus-tree **PASS**. Final verdict: **CCC-CONVERGED**.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: pr-245-revalidation-second-post-push
  supersedes: post-push C-009 Critic coverage attestation in this ledger
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The exact staged helper, four imports, guard whitelist, and call inventory were checked.
      artifacts: [repark_parity/sql.py, four TPC call sites, exact helper inventory]
    - id: AT-2
      status: ATTACKED
      evidence: Empty, quote, repeated-quote, backslash, Unicode, and path values were executed.
      artifacts: [clean isolated helper probe, product-blocked runner pin]
    - id: AT-3
      status: ATTACKED
      evidence: Missing-product imports fail loudly; the standalone imports complete without fallback.
      artifacts: [negative ModuleNotFoundError control, clean dual-runner import]
    - id: AT-4
      status: N/A
      justification: The pure text helper and import relocation add no mutable or concurrent state.
    - id: AT-5
      status: ATTACKED
      evidence: Quote doubling prevents SQL delimiter termination and preserves backslashes as data.
      artifacts: [four quoted SQL embeddings, hostile quote and path probes]
    - id: AT-6
      status: ATTACKED
      evidence: The product Spark helper stays separate; TPC source ratchets and frozen pins remain exact.
      artifacts: [two helper homes, 1263-line TPC-DS runner, 1780-line TPC-H runner]
    - id: AT-7
      status: ATTACKED
      evidence: The helper is one bounded string replacement with no recursion, parser, or execution hook.
      artifacts: [escape_sql_single_quotes, make ci, make verify]
    - id: AT-8
      status: ATTACKED
      evidence: A no-product environment imported both runner graphs and ran the full parity suite.
      artifacts: [clean Python environment, 424 parity tests]
    - id: AT-9
      status: ATTACKED
      evidence: Maps identify the standalone helper and import seam; guard diagnostics name both homes.
      artifacts: [four touched maps, convention guard output, ledger and map gates]
    - id: AT-10
      status: ATTACKED
      evidence: Focused pins, clean suite, CI, and verify pass; the negative control proves the pin bites.
      artifacts: [11 focused tests, 424 clean tests, make ci, make verify]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-5, AT-6, AT-7, AT-8, AT-9, AT-10]
  complete: true
  converged: true
```

Second post-push Critic commands: clean dual-runner test 1 passed; focused PR file 11 passed;
clean no-product parity suite 424 passed; Python conventions, source-size, maps, ledgers, grammar,
and diff checks exited 0; `make ci` exited 0. The first sandboxed `make verify` attempt exited 2
at uv cache locking. The permitted replay exited 0 with all workspace tests and doctests passing.
Disk was 651 GiB free before and after broad validation.
