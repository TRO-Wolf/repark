# Unit ledger — CONF-UNREAD-1 step 1 · the four accepted-but-unread keys are wired or refused

**Unit:** CONF-UNREAD-1 step 1 (tier I) · **Date:** 2026-09-11 · **Branch:** `feat/conf-unread-1` · **Base:** REVIEW-FIX-8 tip (`a4eab150`)
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The PROFILES-1 probe
([profiles-1-passthrough-probe-2026-09-09.md](../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md))
left four `datafusion.*` rows `ACCEPTED BUT UNREAD`: set through `.config()`,
echoed by `conf_dump()`, never read by the engine on the probed subjects. A
user who sets one believes it took effect. RF-4 narrows this card to exactly
four keys; the three `repark.*` keys and two Iceberg properties are
REVIEW-FIX-8's and are not touched here.

**Not in this step:** step 2 (guide paragraph, probe-table `after CONF-UNREAD-1`
column, ledger close), any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, any live-Spark step.

**Retires:** this ledger stays in `staging/` for step 2 (tier M) and moves to
`../completed/` in the unit's last commit.

## PROPOSITION LEDGER — CONF-UNREAD-1 step 1 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1 wire branch: `datafusion.execution.parquet.enable_page_index=false` reaches the scan source options the same way the eleven passing keys are wired. | `conf_unread_enable_page_index_reaches_scan_source_options` | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:784` (`default = true`). Session flow: `apply_datafusion_config_keys` sets it before context build (`session.rs`), `SessionStateBuilder` snapshots `TableOptions::default_from_session_config(config.options())` (`datafusion-54.1.0/src/execution/session_state.rs:1599`), `combine_with_session_config` clones the whole `config.execution.parquet` (`datafusion-common-54.1.0/src/config.rs:2316-2318`); the read path builds listing options from `copied_config` + `copied_table_options` (`datafusion-54.1.0/src/execution/context/parquet.rs:73`) and the opener gates page pruning on `prepared.enable_page_index` (`datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:867,1120`). Pin (characterization: green on base and after — the key was already wired; the control session reading `true` proves the pin bites): control reads `true`, set `false` reaches both `SessionConfig` and session table options. No plan/file signal on the probe's subjects: pyarrow-default files carry no page index, so the filtered single-file scan plan is byte-identical to baseline. |
| C-002 | D-1 wire branch: `datafusion.execution.parquet.bloom_filter_on_read=false` reaches the scan source options the same way the eleven passing keys are wired. | `conf_unread_bloom_filter_on_read_reaches_scan_source_options` | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:851` (`default = true`). Same session flow as C-001; the source carries `table_parquet_options.global.bloom_filter_on_read` (`datafusion-datasource-parquet-54.1.0/src/source.rs:432-459`) into `enable_bloom_filter` (`source.rs:604`). Pin (characterization, green base and after; control reads `true`): set `false` reaches both structures. No plan/file signal on the probe's subjects: the probe files carry no bloom filters (`bloom_filter_lengths` all null in the baseline write facts), so the scan plan is byte-identical. |
| C-003 | D-1 refuse branch: `datafusion.execution.coalesce_batches` refuses loud at build and at runtime `conf.set`, naming the key and the reason. | `conf_unread_coalesce_batches_build_refuses_naming_key`, `conf_unread_coalesce_batches_runtime_set_refuses` | **PROVEN** | The option exists (`datafusion-common-54.1.0/src/config.rs:504`, `default = true`) but no 54.1.0 engine path reads it: the exhaustive consumer search finds only the definition, the `SessionConfig` accessor (`datafusion-execution-54.1.0/src/config.rs:398-406`) and proto serialization; the `CoalesceBatches` physical-optimizer rule present in 52.5.0 has no counterpart in `datafusion-physical-optimizer-54.1.0`, and the built session's physical-optimizer rule list carries no coalesce rule. Refusal: `DEAD_DATAFUSION_54_1_KEYS` + `dead_datafusion_54_1_refusal` (`session/df_guards.rs`), enforced at the build sweep (`session.rs`, `apply_datafusion_config_keys`) and at the runtime-SET intercept (`session/spill.rs`, `maybe_apply_runtime_set`), so both SQL doors refuse. Message: `repark config error: unsupported DataFusion session config 'datafusion.execution.coalesce_batches' = 'false': DataFusion 54.1.0 defines the option but no engine path reads it, so the value cannot take effect`. Red first (§Red first): both pins failed on base (`unwrap_err` on `Ok`). Green after: 5/5 in the new module. Probe: build `refused`, runtime `conf.set` `refused`; probe2 records both coalesce cases as refusals, exit 0. |
| C-004 | D-1 wire branch: `datafusion.execution.parquet.write_batch_size=1000` reaches the writer and moves the written file. | `conf_unread_write_batch_size_reaches_writer_options`, `test_probe_output_holds_the_table_inputs` (write-facts leg) | **PROVEN** | ConfigOptions path `datafusion-common-54.1.0/src/config.rs:867-868` (`default = 1024`). Write flow: the COPY path builds its format from `state.default_table_options()` (`datafusion-datasource-parquet-54.1.0/src/file_format.rs:96-113`), which is session-derived (C-001 flow), and `into_writer_properties_builder` applies `set_write_batch_size` (`datafusion-common-54.1.0/src/file_options/parquet_writer.rs:215,254`). Rust pin (characterization, green base and after; control reads `1024`): set `1000` reaches both structures. File-visible signal, measured: the probe's new `["write"]` subject lands first-part-file bytes `575692` against baseline `575742` (`part_files` and `row_groups` equal, intra-run); scratch determinism check: two runs per value agree byte for byte while `1024`/`1000`/`100000` disagree. Red first (§Red first): the committed probe assertion failed on the base probe (`measured` had no `write` leg). |
| C-005 | D-2: the probe re-run carries every row as `PASSES THROUGH`, `VALIDATED` or `REFUSED`; `ACCEPTED BUT UNREAD` is zero among the four. | `test_probe_output_holds_the_table_inputs` (+ the unchanged byte-identity and poison pins) | **PROVEN** | Green probe re-run, exit 0, empty stderr: 20 keys — 19 `accepted` (read-back equals set value, runtime `conf.set` accepted) and `datafusion.execution.coalesce_batches` `refused` at build and at runtime; 9/9 validation probes refused. The four card rows: one `REFUSED` (C-003), three wired with stated engine evidence (C-001 file-conditional scan-source options, C-002 file-conditional scan-source options, C-004 file-bytes signal) — none unread. `test_profiles1_probe_rerun.py`: 3 passed. The probe-table `after CONF-UNREAD-1` column itself is step 2's. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

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

```yaml
COVERAGE_ATTESTATION:
  pr_unit: conf-unread-1-step-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: One clause per card decision (C-001..C-004 per key plus C-005 for D-2); each clause names its pin and the pin is red-first above (refusals) or control-legged (wirings). No clause rests on a paraphrase.
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-2
      status: ATTACKED
      evidence: Invalid values still refuse at build for every datafusion.* key (9/9 validation probes green); the dead key refuses for every value spelling the sweep can carry, at build and at runtime SET; the probe's 200k-row subjects plus the write-facts leg bound the wired keys.
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths are the pins themselves (build Err naming key and reason; runtime SET Err through the SQL intercept); probe and probe2 record refusals instead of crashing (both exit 0, empty stderr).
      artifacts: [crates/repark-core/src/session/tests/conf_unread.rs, docs/perf/profiles-1-probe/profiles1_probe.py, docs/perf/profiles-1-probe/profiles1_probe2.py]
    - id: AT-4
      status: N/A
      justification: No locks, no shared mutable state, no ordering assumptions; the refusal checks are pure key comparisons before any engine contact.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; the runtime-SET refusal reuses the existing SQL-assignment parser (no new interpolation — key and value travel the established paths).
      artifacts: [crates/repark-core/src/session/df_guards.rs, crates/repark-core/src/session/spill.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Only one key changes behavior (coalesce_batches: accept to refuse); the three wired keys behave exactly as on base (their pins pass both trees), and no other datafusion.* key is touched — the sweep applies the denylist before the generic set.
      artifacts: [crates/repark-core/src/session.rs, crates/repark-core/src/session/tests/conf_unread.rs]
    - id: AT-7
      status: N/A
      justification: No data path, loop, or allocation changes; the added work is one key comparison per builder key and per runtime SET.
    - id: AT-8
      status: ATTACKED
      evidence: No public signature change; no upstream behavior presumed — every D-1 branch cites the DataFusion 54.1.0 source line or the exhaustive no-reader search; the refusal reuses the repo's `repark config error` shape. File sizes: session.rs stays under the default ceiling, new test module far under it, no baseline row touched.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; the refusal message itself names the key and the reason, which is the diagnosable surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every clause is cited from the session-tests map and the Python-tests map; each added branch (build refusal, runtime refusal) has a pin whose flip changes the asserted output, and each wiring pin carries a control leg that fails without the builder key.
      artifacts: [crates/repark-core/src/session/tests/map.md, python/repark/tests/map.md]
```
