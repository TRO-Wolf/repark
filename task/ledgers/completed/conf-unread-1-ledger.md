# Unit ledger — CONF-UNREAD-1 · the four accepted-but-unread keys are wired or refused

**Unit:** CONF-UNREAD-1 step 1 (tier I) + step 2 (tier M) · **Date:** 2026-09-11 · **Branch:** `feat/conf-unread-1` · **Base:** REVIEW-FIX-8 tip (`a4eab150`)
**Model:** step 1 Muse Spark (muse-spark-1.3-contributor); step 2 Devin SWE-2 (swe-2-high)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The PROFILES-1 probe
([profiles-1-passthrough-probe-2026-09-09.md](../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md))
left four `datafusion.*` rows `ACCEPTED BUT UNREAD`: set through `.config()`,
echoed by `conf_dump()`, never read by the engine on the probed subjects. A
user who sets one believes it took effect. RF-4 narrows this card to exactly
four keys; the three `repark.*` keys and two Iceberg properties are
REVIEW-FIX-8's and are not touched here.

**Step 2 (tier M, 2026-09-11):** the docs round — the probe document's tables
gained the `after CONF-UNREAD-1` column and §5's counts were re-derived,
`docs/guide/session-and-conf.md` gained the forwarding/refusal paragraph, and
this ledger closed and moved to `../completed/` in the unit's last commit.

**Not in this unit:** any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, any live-Spark step.

**Retired:** moved to `../completed/` in the unit's last commit (step 2).

## PROPOSITION LEDGER — CONF-UNREAD-1 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1 wire branch: `datafusion.execution.parquet.enable_page_index=false` reaches the scan source options the same way the eleven passing keys are wired. | `conf_unread_enable_page_index_reaches_scan_source_options` | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:784` (`default = true`). Session flow: `apply_datafusion_config_keys` sets it before context build (`session.rs`), `SessionStateBuilder` snapshots `TableOptions::default_from_session_config(config.options())` (`datafusion-54.1.0/src/execution/session_state.rs:1599`), `combine_with_session_config` clones the whole `config.execution.parquet` (`datafusion-common-54.1.0/src/config.rs:2316-2318`); the read path builds listing options from `copied_config` + `copied_table_options` (`datafusion-54.1.0/src/execution/context/parquet.rs:73`) and the opener gates page pruning on `prepared.enable_page_index` (`datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:867,1120`). Pin (characterization: green on base and after — the key was already wired; the control session reading `true` proves the pin bites): control reads `true`, set `false` reaches both `SessionConfig` and session table options. No plan/file signal on the probe's subjects: pyarrow-default files carry no page index, so the filtered single-file scan plan is byte-identical to baseline. |
| C-002 | D-1 wire branch: `datafusion.execution.parquet.bloom_filter_on_read=false` reaches the scan source options the same way the eleven passing keys are wired. | `conf_unread_bloom_filter_on_read_reaches_scan_source_options` | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:851` (`default = true`). Same session flow as C-001; the source carries `table_parquet_options.global.bloom_filter_on_read` (`datafusion-datasource-parquet-54.1.0/src/source.rs:432-459`) into `enable_bloom_filter` (`source.rs:604`). Pin (characterization, green base and after; control reads `true`): set `false` reaches both structures. No plan/file signal on the probe's subjects: the probe files carry no bloom filters (`bloom_filter_lengths` all null in the baseline write facts), so the scan plan is byte-identical. |
| C-003 | D-1 refuse branch: `datafusion.execution.coalesce_batches` refuses loud at build and at runtime `conf.set`, naming the key and the reason. | `conf_unread_coalesce_batches_build_refuses_naming_key`, `conf_unread_coalesce_batches_runtime_set_refuses` | **PROVEN** | The option exists (`datafusion-common-54.1.0/src/config.rs:504`, `default = true`) but no 54.1.0 engine path reads it: the exhaustive consumer search finds only the definition, the `SessionConfig` accessor (`datafusion-execution-54.1.0/src/config.rs:398-406`) and proto serialization; the `CoalesceBatches` physical-optimizer rule present in 52.5.0 has no counterpart in `datafusion-physical-optimizer-54.1.0`, and the built session's physical-optimizer rule list carries no coalesce rule. Refusal: `DEAD_DATAFUSION_54_1_KEYS` + `dead_datafusion_54_1_refusal` (`session/df_guards.rs`), enforced at the build sweep (`session.rs`, `apply_datafusion_config_keys`) and at the runtime-SET intercept (`session/spill.rs`, `maybe_apply_runtime_set`), so both SQL doors refuse. Message: `repark config error: unsupported DataFusion session config 'datafusion.execution.coalesce_batches' = 'false': DataFusion 54.1.0 defines the option but no engine path reads it, so the value cannot take effect`. Red first (§Red first): both pins failed on base (`unwrap_err` on `Ok`). Green after: 5/5 in the new module. Probe: build `refused`, runtime `conf.set` `refused`; probe2 records both coalesce cases as refusals, exit 0. |
| C-004 | D-1 wire branch: `datafusion.execution.parquet.write_batch_size=1000` reaches the writer and moves the written file. | `conf_unread_write_batch_size_reaches_writer_options`, `test_probe_output_holds_the_table_inputs` (write-facts leg) | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:867-868` (`default = 1024`). Write flow: the COPY path builds its format from `state.default_table_options()` (`datafusion-datasource-parquet-54.1.0/src/file_format.rs:96-113`), which is session-derived (C-001 flow), and `into_writer_properties_builder` applies `set_write_batch_size` (`datafusion-common-54.1.0/src/file_options/parquet_writer.rs:215,254`). Rust pin (characterization, green base and after; control reads `1024`): set `1000` reaches both structures. File-visible signal, measured: the probe's new `["write"]` subject lands first-part-file bytes `575692` against baseline `575742` (`part_files` and `row_groups` equal, intra-run); scratch determinism check: two runs per value agree byte for byte while `1024`/`1000`/`100000` disagree. Red first (§Red first): the committed probe assertion failed on the base probe (`measured` had no `write` leg). |
| C-005 | D-2: the probe re-run carries every row as `PASSES THROUGH`, `VALIDATED` or `REFUSED`; `ACCEPTED BUT UNREAD` is zero among the four. | `test_probe_output_holds_the_table_inputs` (+ the unchanged byte-identity and poison pins) | **PROVEN** | Green probe re-run, exit 0, empty stderr: 20 keys — 19 `accepted` (read-back equals set value, runtime `conf.set` accepted) and `datafusion.execution.coalesce_batches` `refused` at build and at runtime; 9/9 validation probes refused. The four card rows: one `REFUSED` (C-003), three wired with stated engine evidence (C-001 file-conditional scan-source options, C-002 file-conditional scan-source options, C-004 file-bytes signal) — none unread. `test_profiles1_probe_rerun.py`: 3 passed. The probe-table `after CONF-UNREAD-1` column itself is step 2's. |
| C-006 | Step 2 (D-2 docs round): the probe document's §2/§3 tables carry the `after CONF-UNREAD-1` column — each of the four keys reads `PASSES THROUGH` or `REFUSED` with one line of step-1 evidence — and §5's counts are re-derived to agree with the table (`ACCEPTED BUT UNREAD` zero). | `test_probe_output_holds_the_table_inputs` (holds the states the column records); `scripts/check_docs_links.py` | **PROVEN** | `docs/perf/profiles-1-passthrough-probe-2026-09-09.md`: both tables carry the column — `enable_page_index`, `bloom_filter_on_read`, `write_batch_size` read `PASSES THROUGH` with the step-1 engine evidence, `coalesce_batches` reads `REFUSED` with the verbatim message; the REVIEW-FIX-8 rows are untouched. §5: `PASSES THROUGH 16, VALIDATED 3, ACCEPTED BUT UNREAD 0, REFUSED 1, NOT MEASURED 0` = 20, derived from the column. §1's global-results paragraph and §4's refusal preamble re-stated for the refused key. `check_docs_links.py` exit 0. Docs clause: no new pin authored — the column records the states `test_probe_output_holds_the_table_inputs` (green since step 1) asserts. |
| C-007 | Step 2: `docs/guide/session-and-conf.md` names the `datafusion.*` keys RePark forwards, states `datafusion.execution.coalesce_batches` is refused on DataFusion 54.1.0 and why, says a refused key fails at `.config()` / `SET` rather than being silently accepted, and quotes the refusal message exactly as the code emits it. | verbatim diff of the guide's quote against the facade-emitted message; `conf_unread_coalesce_batches_*` pins | **PROVEN** | `docs/guide/session-and-conf.md` "How `conf.get` / `conf.set` behave": the new paragraph names the measured forwarding set (four optimizer keys, two execution keys, three parquet scan options, four parquet write options), the two repark-owned `datafusion.runtime.*` pseudo-keys, and the `coalesce_batches` refusal (defined in `ConfigOptions`, no engine path reads it). The quoted message was captured from the built module in this clone — `.config()` raises `IllegalArgumentException: repark config error: unsupported DataFusion session config 'datafusion.execution.coalesce_batches' = 'false': DataFusion 54.1.0 defines the option but no engine path reads it, so the value cannot take effect`; `conf.set` wraps the same text in `[INVALID_CONF_VALUE.REQUIREMENT]`; the guide shows the `.config()` form verbatim. `check_docs_links.py` exit 0. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

New pins fail on the base tree, then pass after the wiring/refusals:

```text
cargo test -p repark-core --lib session::tests::conf_unread (base):
test session::tests::conf_unread::conf_unread_coalesce_batches_build_refuses_naming_key ... FAILED
test session::tests::conf_unread::conf_unread_coalesce_batches_runtime_set_refuses ... FAILED
test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 300 filtered out
(panics: called `Result::unwrap_err()` on an `Ok` value — the base tree accepts the dead key)

python/repark/tests/test_profiles1_probe_rerun.py::test_probe_output_holds_the_table_inputs (base):
E  AssertionError: assert 'accepted' == 'refused'
```

The three wire-branch Rust pins pass on base and after (the keys were already
wired by the generic sweep); each carries a control leg (default session reads
the DataFusion default) so none passes vacuously. Green after: `5 passed` (new
Rust module), `3 passed` (probe rerun file).

## Step 2 (tier M) — the docs round, 2026-09-11

No engine change and no new pin: the two added clauses are documents proven
against step 1's committed pins. The probe document
([profiles-1-passthrough-probe-2026-09-09.md](../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md))
gained the `after CONF-UNREAD-1` column on both tables (`—` on every row this
unit does not own), §5's counts were re-derived from it, and §1/§4's prose was
re-stated for the refused key. `docs/guide/session-and-conf.md` gained the
`datafusion.*` paragraph: the measured forwarding set, the two repark-owned
`datafusion.runtime.*` pseudo-keys, and the `coalesce_batches` refusal with the
message quoted verbatim as the facade emits it (captured on the built module in
this clone — `.config()` raises `IllegalArgumentException`, `conf.set` wraps the
same text in `[INVALID_CONF_VALUE.REQUIREMENT]`). Maps in lockstep:
`docs/perf/map.md`, `docs/guide/map.md`, `python/repark/tests/map.md`,
`crates/repark-core/src/session/tests/map.md`.

Step-2 gates: `python3 scripts/check_docs_links.py` exit 0;
`python3 scripts/check_ledger_grammar.py` exit 0; `make verify` exit 0.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: conf-unread-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: One clause per card decision (C-001..C-004 per key plus C-005 for D-2); each clause names its pin and the pin is red-first above (refusals) or control-legged (wirings). No clause rests on a paraphrase. Step 2: C-006/C-007 are the docs clauses — the probe column records the states the green rerun pin asserts, and the guide's refusal quote was diffed verbatim against the facade-emitted message in this clone.
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, python/repark/tests/test_profiles1_probe_rerun.py, docs/perf/profiles-1-passthrough-probe-2026-09-09.md, docs/guide/session-and-conf.md]
    - id: AT-2
      status: ATTACKED
      evidence: Invalid values still refuse at build for every datafusion.* key (9/9 validation probes green); the dead key refuses for every value spelling the sweep can carry, at build and at runtime SET; the probe's 200k-row subjects plus the write-facts leg bound the wired keys.
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths are the pins themselves (build Err naming key and reason; runtime SET Err through the SQL intercept); probe and probe2 record refusals instead of crashing (both exit 0, empty stderr). Step 2: the guide and the probe column quote the refusal verbatim rather than paraphrasing it, and the doc states the conf.set wrapper shape too.
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, docs/perf/profiles-1-probe/profiles1_probe.py, docs/perf/profiles-1-probe/profiles1_probe2.py, docs/guide/session-and-conf.md]
    - id: AT-4
      status: N/A
      justification: No locks, no shared mutable state, no ordering assumptions; the refusal checks are pure key comparisons before any engine contact. Step 2 touches markdown only.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; the runtime-SET refusal reuses the existing SQL-assignment parser (no new interpolation — key and value travel the established paths). Step 2 edits only markdown under docs/, task/, and the map files.
      artifacts: [crates/repark-core/src/session/df_guards.rs, crates/repark-core/src/session/spill.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Only one key changes behavior (coalesce_batches: accept to refuse); the three wired keys behave exactly as on base (their pins pass both trees), and no other datafusion.* key is touched — the sweep applies the denylist before the generic set. Step 2 changes no behavior at all: the column records states the probe already emits and the REVIEW-FIX-8 rows are byte-preserved.
      artifacts: [crates/repark-core/src/session.rs, crates/repark-core/src/session/tests/conf_unread.rs, docs/perf/profiles-1-passthrough-probe-2026-09-09.md]
    - id: AT-7
      status: N/A
      justification: No data path, loop, or allocation changes; the added work is one key comparison per builder key and per runtime SET. Step 2 touches markdown only.
    - id: AT-8
      status: ATTACKED
      evidence: No public signature change; no upstream behavior presumed — every D-1 branch cites the DataFusion 54.1.0 source line or the exhaustive no-reader search; the refusal reuses the repo's `repark config error` shape. File sizes: session.rs stays under the default ceiling, new test module far under it, no baseline row touched. Step 2: no code file edited; the guide's claims (forwarded set, pseudo-keys, refusal) cite the same 54.1.0 lines the clauses carry.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; the refusal message itself names the key and the reason, which is the diagnosable surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every clause is cited from the session-tests map and the Python-tests map; each added branch (build refusal, runtime refusal) has a pin whose flip changes the asserted output, and each wiring pin carries a control leg that fails without the builder key. Step 2: C-006 is cited from python/repark/tests/map.md and docs/perf/map.md (the rerun pin holds the states the column records); C-007 from crates/repark-core/src/session/tests/map.md and docs/guide/map.md (the refusal pins hold the message's load-bearing tokens; the verbatim check is the captured diff in C-007).
      artifacts: [crates/repark-core/src/session/tests/map.md, python/repark/tests/map.md, docs/perf/map.md, docs/guide/map.md]
```
