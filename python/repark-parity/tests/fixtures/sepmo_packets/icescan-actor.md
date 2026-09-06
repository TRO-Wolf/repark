# SEPMO compact worker packet

packet_version: 1

## Stable prefix

These rules never change between units. A packet that omits any of them is invalid.

### Comment ban
No code comments anywhere (Python `#` beyond `# noqa`; docstrings one line where `check-docstring-presence` demands). Reasons live in `map.md`s and the ledger.

### Identity and trailer
Identity `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`. The LAST line of every commit message is the adapter Authored-By trailer named in the packet identity; no other trailer.

### Prohibitions
Never push, never `gh`, never `aws`, never `--no-verify`, never touch `.github/`. Never edit `$HOME/.claude/`. Wrapper patches live under `docs/sepmo/telemetry/wrapper-patches/`. No home paths in the tree.

### Cargo cap
Three cargo builders is the box cap. One cargo invocation at a time, in one lane at a time, unless the packet names a different bound.

### Live-oracle provisioning
Live PySpark is the oracle when a unit asserts Spark values. One JVM beside at most one other. Redirect ivy into the lane. Kill what you start.

### Size ceilings
Size ceilings only ratchet down. A new Python file stays under 1000 lines. Do not raise a gate baseline without owner approval. Do not change what any gate requires.

### Map lockstep
`map.md` in every directory, updated in the same change. A new directory gets a new `map.md`.

### Frozen ledgers
A ledger in `completed/` or `archive/` is frozen. Do not amend it.
## Dynamic

### Identity
- unit: icescan
- role: actor
- attempt: 1
- adapter: muse
- packet_format_version: 1
- task_reference: PERF-ICE-SCAN-1 — Iceberg `count(*)` stops decoding every column, and small tables scan in parallel (Muse Spark 1.3, standard pattern; perf analysis slate items 4 and 5)
- authored_by_trailer: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>

### Source identity
- repository: repark
- base_revision: 8f40ce46
- working_diff_identity: (empty)
- untracked_inputs: (none)
- brief_hash: 384bd41e665925796866d68d869af2586f55ee2d244d775c2ba438ec2a7e89ca

### Authority
Contract: AGENTS.md. Binding: SEPMO v2.3. Constraints: the stable prefix above. Sources: AGENTS.md, docs/testing.md, .agents/skills/sepmo/binding-manifest.md, .agents/skills/sepmo/unit-runbook.md.

### Scope
**Objective.** PERF-ICE-SCAN-1 — Iceberg `count(*)` stops decoding every column, and small tables scan in parallel (Muse Spark 1.3, standard pattern; perf analysis slate items 4 and 5)

**Requirement ids.** icescan

**Acceptance.**
Fork: commits on `$HOME/repark-lanes/lanes/icescan-fork` with tests; a short note under the fork's `docs/` only if the fork's contract demands it. RePark: `task/ledgers/staging/perf-ice-scan-1-ledger.md` (house form, `Model: muse-spark-1.3`, `risk_tier: standard`, the fork SHA it was measured against, a clause that says the pin bump is the orchestrator's RP-12 step), registry rows `PERF-ICE-COUNTSTAR-1` and `PERF-ICE-SCANPART-1` filed with before/after and status FIXED-PENDING-PIN (read how `PERF-DVCLOSE-STMT-1` recorded its fork dependency and copy that form), `docs/perf/iceberg-scan-baseline.md` with the tables and commands, maps in lockstep, pins under `python/repark/tests/test_perf_ice_scan_1.py` that SKIP with a named reason until the fork pin carries F-27 (detect the fork capability at runtime, e.g. via a probe of the folded plan), so `main` stays green before the bump. `STATUS.md` and `briefs/next-sequence.md` untouched.

**Exclusions.** .github/

### Implementation context
- relevant_files: repark-lanes/briefs/mklane-icescan.log, .agents/skills/engineering-method/SKILL.md, docs/testing.md, .agents/skills/sepmo/unit-runbook.md, repark-lanes/briefs/live-cell-rules.md, docs/perf/map.md, docs/perf/dynamic-flatten-baseline.md, docs/fork-sync.md, docs/perf/engine-iceberg-analysis-2026-09-04.md, crates/repark-iceberg/map.md, table/mod.rs, crates/iceberg/src/arrow/reader.rs, crates/integrations/datafusion/src/physical_plan/scan.rs, scan/partition_work.rs, scan/mod.rs, task/ledgers/staging/perf-ice-scan-1-ledger.md, docs/perf/iceberg-scan-baseline.md, python/repark/tests/test_perf_ice_scan_1.py, briefs/next-sequence.md, scripts/ledger_lifecycle.py
- callers: (none)
- interfaces: (none)
- dependency_decisions: Do not change Cargo.toml, Cargo.lock, or other dependency files
- known_traps: HALT with evidence rather than inventing a missing decision, Commit early; lanes live under $HOME/repark-lanes/lanes/

### Verification
**Commands.**
- RePark lane (with the temporary patch reverted): `make ci
- make verify
- make check-python-conventions
- make rust-panic-ban
- .venv/bin/python -m pytest python/repark/tests -q --timeout 900 -x
- make check-map-sync
- make check-ledger-grammar
- make check-ledgers
- make check-docs-compaction
- python3 scripts/ledger_lifecycle.py check --base origin/main
- typos .` and `git diff origin/main -- Cargo.toml Cargo.lock` empty. Fork lane: `cargo fmt --all --check
- cargo clippy --all-targets -- -D warnings
- cargo test -p iceberg -p iceberg-datafusion`. Hand back `handback.json` at `$HOME/repark-lanes/lanes/oc-icescan`: `{"status":"done|halt","repark_commits":[...],"fork_commits":[...],"fork_lane":"$HOME/repark-lanes/lanes/icescan-fork","numbers":{...},"gates":"...","notes":"...","questions":[...]}` and end with the before/after table and the mutation score (revert the empty-projection change → which pins red; revert the statistics fold → which; revert the split override → which). HALT with evidence if the DV path cannot stay correct under zero-column batches.

**Behavioral cases.**
(none)

**Oracle requirements.** (none)
**Evidence destinations.** task/ledgers/staging/, handback.json, unit ledger

### Permissions and resources
- authorized_actions: Read the named sources, Edit only the writable paths in this packet, Run the listed verification commands, Commit with the bound identity and trailer
- ownership_boundaries: .github/
- resource_limits: Three cargo builders is the box cap, One Spark JVM beside at most one other
- escalation_conditions: Ambiguity that changes the outcome is a HALT, A red gate is not worked around

### Handoff
- expected_output_fields: status, commits, gates, notes, questions
- unresolved_decisions: (none)
- dependency_consumers: orchestrator launch wrapper

Use this trailer as the last line of every commit: Authored-By: Muse Spark (muse-spark-1.3) <noreply@meta.ai>
