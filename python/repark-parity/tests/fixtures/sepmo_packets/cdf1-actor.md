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
- unit: cdf1
- role: actor
- attempt: 1
- adapter: muse
- packet_format_version: 1
- task_reference: PERF-FACADE-CDF-1 — `createDataFrame(list of tuples)` stops normalizing every cell in Python five times (Muse Spark 1.3, standard pattern; perf analysis slate item 3, candidate 2)
- authored_by_trailer: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>

### Source identity
- repository: repark
- base_revision: e8344e8e
- working_diff_identity: (empty)
- untracked_inputs: (none)
- brief_hash: 7df947bc10d3a98eeffd363e47a07f066c5195dccf30d1517bde23958603f7c6

### Authority
Contract: AGENTS.md. Binding: SEPMO v2.3. Constraints: the stable prefix above. Sources: AGENTS.md, docs/testing.md, .agents/skills/sepmo/binding-manifest.md, .agents/skills/sepmo/unit-runbook.md.

### Scope
**Objective.** PERF-FACADE-CDF-1 — `createDataFrame(list of tuples)` stops normalizing every cell in Python five times (Muse Spark 1.3, standard pattern; perf analysis slate item 3, candidate 2)

**Requirement ids.** cdf1

**Acceptance.**
`createDataFrame(list of tuples)` at 1e5 rows costs ~1,717 ms (analysis) / ~803 ms (facade runner, `create/100000/tuples_count`) against `createDataFrame(pandas)` far lower, because inference normalizes every cell in Python across five passes (`_normalize_create_dataframe_cell`, `_prepare_nested_cell`, the long/double merge refusal walk, the per-row schema check, the Arrow conversion). Deliverable: column-wise inference and conversion — one pass that types each column from its values (Spark's merge rules: long/double refusal, int widths, decimal precision/scale, string/bytes, date/timestamp with tz, bool, None) and builds Arrow arrays column by column (`pyarrow.array` with the resolved type, or the existing per-cell normalizer only for nested columns: struct/array/map/Row cells), preserving every current answer: the VALUES-path rules, the refusal messages and their exact text, `verifySchema`, explicit-schema paths (`schema=StructType`, DDL string, name list), `Row` inputs, dict inputs, mixed None columns, empty input, the TY-*/DEC-* pinned inference answers. Target: `create/100000/tuples_count` ≤ 100 ms (analysis' target) — report what you reach and, if the target is missed, the isolated cost that remains and where it sits, honestly.

**Exclusions.** .github/, STATUS.md, briefs/next-sequence.md

### Implementation context
- relevant_files: repark-lanes/briefs/mklane-cdf1.log, .agents/skills/engineering-method/SKILL.md, docs/testing.md, .agents/skills/sepmo/unit-runbook.md, repark-lanes/briefs/live-cell-rules.md, docs/perf/map.md, docs/perf/facade-boundary-baseline.md, docs/perf/engine-iceberg-analysis-2026-09-04.md, session/create_dataframe_rows.py, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/test_perf_facade_cdf_1.py, task/ledgers/staging/perf-facade-cdf-1-ledger.md, task/ledgers/staging/map.md, python/repark/tests/map.md, briefs/next-sequence.md, python/repark/tests/test_parity_live.py, scripts/ledger_lifecycle.py
- callers: (none)
- interfaces: (none)
- dependency_decisions: never change dependencies
- known_traps: HALT with evidence rather than inventing a missing decision, Commit early; lanes live under $HOME/repark-lanes/lanes/

### Verification
**Commands.**
- make ci
- make check-python-conventions
- PYTHONPATH=$HOME/repark-lanes/lanes/oc-cdf1/python/repark/src .venv/bin/python -m pytest python/repark/tests -q --timeout 900 -x
- .venv/bin/python -m pytest python/repark-parity/tests -q
- REPARK_PARITY_LIVE=1 PYTHONPATH=$HOME/repark-lanes/lanes/oc-cdf1/python/repark/src .venv/bin/python -m pytest python/repark/tests/test_parity_live.py python/repark/tests/test_perf_facade_cdf_1.py -q
- make check-map-sync
- make check-ledger-grammar
- make check-ledgers
- make check-docs-compaction
- python3 scripts/ledger_lifecycle.py check --base origin/main
- typos .

**Behavioral cases.**
`python/repark/tests/test_perf_facade_cdf_1.py` (≤ 1000 lines): equality pins of the new path against the old path (keep the old inference callable under a private name for the pin, as PERF-FACADE-1 did for the converter) on a wide value matrix — every scalar Python type, None in every column and whole-None columns, mixed int/float (the long/double refusal), ints beyond int64, Decimal at several scales, str/bytes/bytearray, date/datetime naive and aware, bool, nested tuple/list/dict/Row cells, ragged rows (refusal), 0 rows, 1 row, 1e4 rows — asserting type-and-value equality of the resulting DataFrame's schema and `collect()`, and the exact refusal messages; plus a live leg comparing schema and rows against PySpark 4.1.2 `createDataFrame` for the scalar matrix. Red under a deliberately wrong inference (e.g. ints typed as double) before the implementation; mutation score after (drop the long/double refusal → which pins red; wrong decimal scale → which; skip the None mask → which; treat a nested column as scalar → which).

**Oracle requirements.** live PySpark 4.1.2
**Evidence destinations.** task/ledgers/staging/, handback.json, unit ledger

### Permissions and resources
- authorized_actions: Read the named sources, Edit only the writable paths in this packet, Run the listed verification commands, Commit with the bound identity and trailer
- ownership_boundaries: .github/, STATUS.md, briefs/next-sequence.md
- resource_limits: Three cargo builders is the box cap, One Spark JVM beside at most one other
- escalation_conditions: Ambiguity that changes the outcome is a HALT, A red gate is not worked around

### Handoff
- expected_output_fields: status, commits, gates, notes, questions, numbers
- unresolved_decisions: (none)
- dependency_consumers: orchestrator launch wrapper

Use this trailer as the last line of every commit: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>
