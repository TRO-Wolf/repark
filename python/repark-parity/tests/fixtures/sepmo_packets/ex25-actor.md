# SEPMO compact worker packet

packet_version: 1

## Stable prefix

These rules never change between units. A packet that omits any of them is invalid.

### Comment ban
No code comments anywhere: Python `#` beyond `# noqa`; Rust `//` `///` `//!`; TOML/YAML/shell `#`; docstrings one line where `check-docstring-presence` demands. The only forced exception is one `/// # Errors` line clippy demands on a `pub fn` returning `Result`. On a new fork file the ASF header plus one `///` where the fork's `deny(missing_docs)` forces it on a new `pub` item. Never delete a pre-existing comment. Reasons live in `map.md`s (`pins: <unit>/C-NNN`) and the ledger. Self-check before every commit: `git diff --cached | grep -nE '^\+.*(//|#)' -- '*.rs' '*.py' '*.toml' | grep -v '#\[' | grep -v noqa` prints nothing beyond the forced lines.

### Identity and trailer
Identity `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`. The LAST line of every commit message is the adapter Authored-By trailer named in the packet identity; no other trailer.

### Prohibitions
Never push, never `gh`, never `aws`, never `--no-verify`, never touch `.github/`. Never edit `$HOME/.claude/`. Wrapper patches live under `docs/sepmo/telemetry/wrapper-patches/`. No home paths in the tree. Never change dependencies or `Cargo.lock` unless the packet names a sanctioned writer (`make bump-fork-pin`).

### Cargo cap
Three cargo builders is the box cap. One cargo invocation at a time, in one lane at a time, unless the packet names a different bound.

### Release module
Numbers and native-module checks need `maturin develop --release`. Prove `__debug_assertions__` is False and the path is under the lane. Record load; keep the box quiet.

### Live-oracle provisioning
Live PySpark is the oracle when a unit asserts Spark values. Provision with `--extra record` as `make parity-live` does. One JVM beside at most one other. Wait for an empty box (60 s poll, up to 15 min). Redirect ivy into the lane (`cp -a ~/.ivy2.5.2 <lane>/.ivy2` + `PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=<lane>/.ivy2 pyspark-shell"`). `JAVA_HOME` is Java 17. ANSI on; UTC session zone unless the case is about zones. Kill what you start.

### Size ceilings
Size ceilings only ratchet down. A new Python file stays under 1000 lines. Every ratchet in `scripts/check_lib_py.py` is mirrored in `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` in the same commit. Do not raise a gate baseline without owner approval. Do not change what any gate requires.

### Map lockstep
`map.md` in every directory, updated in the same change. A new directory gets a new `map.md`. No amends.

### Frozen ledgers
A ledger in `completed/` or `archive/` is frozen. Do not amend it.
## Dynamic

### Identity
- unit: ex25
- role: actor
- attempt: 1
- adapter: muse
- packet_format_version: 1
- task_reference: EX-25 — v1.1 example backfill, the `F.*` long tail (a): the 45 plainly supported names (Muse Spark 1.3, standard pattern)
- authored_by_trailer: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>

### Source identity
- repository: repark
- base_revision: bc7c76cc
- working_diff_identity: (empty)
- untracked_inputs: (none)
- brief_hash: bc1020bae4be851f5b822bf322e934afa505a96b2e38935a4487362617c43804

### Authority
Contract: AGENTS.md. Binding: SEPMO v2.3. Constraints: the stable prefix above. Sources: AGENTS.md, docs/testing.md, .agents/skills/sepmo/binding-manifest.md, .agents/skills/sepmo/unit-runbook.md.

### Scope
**Objective.** EX-25 — v1.1 example backfill, the `F.*` long tail (a): the 45 plainly supported names (Muse Spark 1.3, standard pattern)

**Requirement ids.** ex25, roster

**Acceptance.**
`add_months approx_percentile array_position array_sort arrays_overlap arrays_zip base64 char chr current_user decode elt encode expr flatten format_number from_csv hash hours initcap isnan json_tuple kurtosis make_interval make_timestamp map_zip_with mode monotonically_increasing_id months_between percentile_approx posexplode posexplode_outer raise_error randstr regexp_extract replace schema_of_csv schema_of_json sentences session_user sha2 skewness spark_partition_id split input_file_name`. Out of this batch (EX-26): the `kll_*`, `hll_*`, `theta_*`, `st_*`, variant, `from_xml`/`schema_of_xml`, UDF/pandas_udf and `java_method`/`reflect` names.

One script per small family under `docs/examples/functions/` (group as the map already does: e.g. `array_more.py` for array_position/array_sort/arrays_overlap/arrays_zip/flatten/posexplode/posexplode_outer/map_zip_with; `strings_more.py` for base64/char/chr/decode/encode/elt/initcap/replace/split/regexp_extract/format_number/sentences/sha2/hash; `dates_more.py` for add_months/months_between/make_timestamp/make_interval/hours; `stats.py` for kurtosis/skewness/mode/percentile_approx/approx_percentile; `session_misc.py` for current_user/session_user/spark_partition_id/monotonically_increasing_id/input_file_name/randstr/raise_error/isnan/expr; `csv_json.py` for from_csv/schema_of_csv/schema_of_json/json_tuple). Each example runs on repark and asserts the Spark-measured answers (shape-check only where Spark's answer is non-deterministic: `randstr`, `monotonically_increasing_id`, `spark_partition_id`, `current_user`); a name whose repark answer diverges from Spark is NOT papered over — it stays on the backlog with a §7 row (form: the existing `F.*` rows) naming the measured cell, and the example covers the rest of its family. `make check-example-coverage` green with `BACKLOG_BASELINE` lowered by exactly the names covered; backlog delta = exactly those names. Ledger: EX-24 form, `Model: muse-spark-1.3`, one clause per script plus the roster clause and the backlog-delta clause, attestation block; `staging/map.md` row; `docs/examples/functions/map.md` rows.

**Exclusions.** .github/, crates/, python/repark/src/, STATUS.md, briefs/next-sequence.md, scripts/

### Implementation context
- relevant_files: repark-lanes/briefs/mklane-ex25.log, .agents/skills/engineering-method/SKILL.md, docs/testing.md, briefs/example-backfill.md, docs/examples/map.md, docs/examples/functions/map.md, scripts/check_example_coverage.py, task/ledgers/staging/ex-24-ta-b-ledger.md, repark-lanes/briefs/live-cell-rules.md, docs/spark-sql-iceberg-parity.md, docs/examples/backlog.txt, briefs/next-sequence.md, task/ledgers/staging/ex-25-functions-a-ledger.md, staging/map.md, scripts/ledger_lifecycle.py
- callers: (none)
- interfaces: (none)
- dependency_decisions: (none)
- known_traps: (none)

### Verification
**Commands.**
- make check-example-coverage
- make check-python-conventions
- make py-lint
- make py-format-check
- .venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q
- for f in docs/examples/functions/<new files>; do .venv/bin/python $f; done
- make check-map-sync
- make check-ledger-grammar
- make check-ledgers
- make check-docs-compaction
- python3 scripts/ledger_lifecycle.py check --base origin/main
- typos .

**Behavioral cases.**
(none)

**Oracle requirements.** live PySpark 4.1.2
**Evidence destinations.** task/ledgers/staging/, handback.json, unit ledger

### Permissions and resources
- authorized_actions: Read the named sources, Edit only the writable paths in this packet, Run the listed verification commands, Writable: docs/examples/functions/, docs/examples/backlog.txt, BACKLOG_BASELINE, docs/spark-sql-iceberg-parity.md, python/repark/tests/test_examples_*.py, map.md, task/ledgers/staging/ex-25-functions-a-ledger.md, staging/map.md, Commit with the bound identity and trailer
- ownership_boundaries: .github/, crates/, python/repark/src/, STATUS.md, briefs/next-sequence.md, scripts/
- resource_limits: Three cargo builders is the box cap, One Spark JVM beside at most one other
- escalation_conditions: Ambiguity that changes the outcome is a HALT, A red gate is not worked around

### Handoff
- expected_output_fields: status, commits, gates, notes, questions, covered, stayed
- unresolved_decisions: (none)
- dependency_consumers: orchestrator launch wrapper

Use this trailer as the last line of every commit: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>
