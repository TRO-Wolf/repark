# Unit ledger — SEPMO-E0E1 · telemetry inventory and usage collector

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when SEPMO-E0E1 merges, or when the owner
closes the slate row.

**Unit:** SEPMO-E0E1 · **Date:** 2026-09-06 · **Model:** grok-4.6 · **Branch:**
`sepmo/e0-e1-usage-collector` · **Base:** `origin/main`
**Brief:** `sepmo-efficiency-brief.md` in the lane (copy of
`task/roadmap/epic-term/sepmo-efficiency-implementation-brief-2026-09-04.md`, PR #376),
work packages E-0 and E-1. **Ruling:** owner, 2026-09-04, efficiency brief §13.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/sepmo/`, `docs/map.md`, `scripts/sepmo_usage.py`, `scripts/map.md`,
`python/repark-parity/tests/test_sepmo_usage.py`,
`python/repark-parity/tests/fixtures/sepmo_usage/`, `python/repark-parity/tests/map.md`,
`Makefile` (`sepmo-usage` target only), this ledger and `staging/map.md`. Closed: `crates/`,
`python/repark/src/`, `.github/`, `STATUS.md`, `briefs/next-sequence.md`, `Cargo.lock`,
`$HOME/.claude/`.

## Scope

E-0 inventories the four worker adapters this fleet actually uses (Muse Spark, opencode/kilo
GLM, Grok, Claude sub-agents): command form, run-dir layout, and which usage fields exist
when you read real run directories rather than assume. E-1 is a local collector that emits
one nullable JSON record per run dir, with `missing_reason` for absent data, and an index
table matching the inventory. Compact packets (E-2) and verification-policy changes (E-5/E-6)
are out of scope. Wrapper files under `$HOME/.claude/` are not edited; patches are proposed
under `docs/sepmo/telemetry/wrapper-patches/`.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The inventory names each of Muse, opencode/kilo, Grok, and Claude with the exact command form, run-dir layout, and measured availability of input tokens, output tokens, cached input, cost, wall time, steps, tool calls, exit, model id, and effort; unavailable fields are stated per adapter. | [docs/sepmo/telemetry/inventory.md](../../../docs/sepmo/telemetry/inventory.md) §1–§2; `test_inventory_covers_four_adapters_and_pilot_strata`. Round 2: Muse tokens from the session store; cost still absent; Grok live keys confirmed. | **PROVEN** |
| C-002 | A frozen baseline table covers the 2026-09-05/06 Muse run dirs (29 rows) with per-lane stamp, exit, wall seconds, steps, tool calls, hand-back, commits, and session-store token columns (uncached in / out / cached / reasoning). Cost stays null. | inventory.md §3; the three-row raw-evidence table; `test_inventory_covers_four_adapters_and_pilot_strata`. Round 2 re-verdict: round 1 claimed Muse emits no tokens; that was false. | **PROVEN** |
| C-003 | Brief §12 task strata are mapped onto the lanes that actually ran (perf unit, fix unit, EX batch, critic round, remediation round). Muse `role` is null on the record; §4 is a human classification. Lane `ex26` is named as a lane, not as unit EX-26. | inventory.md §4; the same inventory test. Round 2 re-verdict: collector no longer stamps `worker` from the build-lane prompt. | **PROVEN** |
| C-004 | `sepmo_usage.py collect <run-dir>` emits one JSON record whose fields match `usage-record.schema.json`; every payload field is nullable; `missing_reason` names why a field is absent; `units` states the unit of each numeric field. | schema file (includes `tokens_cache_write`, `truncated`); collector; `test_schema_file_lists_every_record_field` and the adapter fixture tests. Round 2: Grok fixture rebuilt on live keys; Muse session-store fixture. | **PROVEN** |
| C-005 | `sepmo_usage.py index <dir>` writes the table the inventory shows (or `--jsonl` records). | `test_index_writes_inventory_table_shape`; `test_cli_collect_and_index_round_trip`. | **PROVEN** |
| C-006 | Validation fails loudly on malformed input; the collector opens no network path. | `test_malformed_grok_json_fails_loudly`; `test_malformed_muse_jsonl_fails_loudly`; `test_truncated_muse_jsonl_fails_when_exit_record_absent`; `test_missing_cmd_txt_fails_loudly`; `test_remote_url_is_rejected_before_resolve`. Round 2: remote check runs on the raw path; truncated JSONL with no exit record is UsageError. | **PROVEN** |
| C-007 | Checked-in fixtures cover each adapter shape, missing-data behaviour (null, not zero), and unit correctness. | `python/repark-parity/tests/fixtures/sepmo_usage/`; the fixture tests in `test_sepmo_usage.py`. Round 2: sanitized Muse session excerpt; Grok live-key `out.json`; truncated-tail JSONL. | **PROVEN** |
| C-008 | The collector's numbers for at least three real Muse run dirs agree with a second pass over the raw files, including session-store token totals. | inventory.md §3 raw-evidence table; `test_reconciles_three_live_muse_run_dirs_when_present` (skips only when `/tmp/muse-worker` is absent). | **PROVEN** |

`LOGIC_SCORE` = **8/8 `PROVEN`**.

## Self Logic Review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-e0e1-build
  agent: Actor
  action: Land the E-0 inventory and the E-1 collector with tests, maps, and this ledger
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  preconditions:
    - adapters and run dirs inspected by reading real files: SATISFIED (Muse JSONL + session store, OpenCode sqlite, Grok completed out.json, Claude unavailable)
    - no wrapper edits under $HOME/.claude: SATISFIED (patches proposed only; Muse patch narrowed to cost/wall)
    - no network in the collector: SATISFIED (_reject_remote on the raw path; local Path IO only)
  success_condition: make ci green; pytest test_sepmo_usage.py green; every PROVEN clause pinned
  step_risks:
    - inventing Muse tokens from tool original_output_tokens: HANDLED(session-store per-turn usage only)
    - writing zeros for missing Grok/Claude fields: HANDLED(empty-out and claude fixtures; per-field missing_reason)
    - leaking home paths into the tree: HANDLED(fixtures sanitized; inventory uses $HOME and /tmp)
  contingencies:
    - live Muse dirs absent in CI: EXECUTABLE(skip the live test; fixture tests still pin counting)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — truncated Grok JSON:** `fixtures/sepmo_usage/malformed/bad-json/out.json`
is an unclosed object. `collect` raises `UsageError` / CLI exit 1.
`pins: sepmo-e0-e1/C-006`

**Provocation 2 — Muse JSONL that is not JSON:** three non-JSON lines. `collect` raises
`UsageError` matching `malformed JSONL`.
`pins: sepmo-e0-e1/C-006`

**Provocation 3 — empty Grok out.json:** steps, tokens, and cost stay `null` (not `0`).
`pins: sepmo-e0-e1/C-004, C-007`

**Provocation 4 — truncated Muse JSONL tail:** last line cut mid-string, no
`run.terminal.completed`. `collect` raises `UsageError` matching `truncated JSONL
with no exit record`.
`pins: sepmo-e0-e1/C-006`

**Provocation 5 — remote URL:** `collect_run(Path("https://example.invalid/run"))`
raises before `Path.resolve` can turn it into a local path.
`pins: sepmo-e0-e1/C-006`

## Reconciled sample (C-008)

Measured 2026-09-06 against `/tmp/muse-worker/<lane>/<stamp>/out.jsonl` and file mtimes:

| Lane | Stamp | Raw tool.result | Raw unique task.started | exit file | wall_s (mtimes) |
|---|---|---:|---:|---|---:|
| ex25 | 20260905T214117Z | 163 | 408 | `0` | 3849.9 |
| cdf1 | 20260905T113405Z | 325 | 883 | `0` | 13214.7 |
| icescanfr2 | 20260905T225134Z | 105 | 221 | `143` | 1279.8 |

Round 2: the same three rows plus session-store token totals (uncached in / out /
cached). `cost_usd` stays null. Muse `runs.tsv` tools column equals `tool.result`
on these three rows.

| Lane | Stamp | tokens_in | tokens_out | tokens_cached | tokens_reasoning |
|---|---|---:|---:|---:|---:|
| ex25 | 20260905T214117Z | 269827 | 95642 | 24171581 | 38182 |
| cdf1 | 20260905T113405Z | 689231 | 194648 | 75010034 | 102920 |
| icescanfr2 | 20260905T225134Z | 161084 | 40183 | 8014581 | 23012 |

## Out of scope observed

- Compact packets (E-2), verification-policy changes (E-5/E-6), wrapper files under
  `$HOME/.claude/`.
- No `/tmp/oc-worker/` run dirs from 2026-09-05/06; OpenCode tokens live in sqlite, documented
  not opened.
- Round 1 recorded Grok `out.json` empty at first inventory; the completed object is now
  in the Grok section of the inventory (live keys confirmed).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sepmo-e0-e1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Inventory enumerates four adapters and every requested usage field; fixtures cover each adapter shape.
      artifacts: [docs/sepmo/telemetry/inventory.md, python/repark-parity/tests/fixtures/sepmo_usage/, python/repark-parity/tests/test_sepmo_usage.py]
    - id: AT-2
      status: ATTACKED
      evidence: Malformed JSON and JSONL fail; missing cmd.txt fails; empty Grok out.json does not become zero.
      artifacts: [python/repark-parity/tests/test_sepmo_usage.py]
    - id: AT-3
      status: ATTACKED
      evidence: Schema requires nullable fields plus missing_reason; collector rejects remote URLs on the raw path.
      artifacts: [docs/sepmo/telemetry/usage-record.schema.json, scripts/sepmo_usage.py]
    - id: AT-4
      status: N/A
      justification: Collector is a single-process local file reader; no shared mutable engine state.
    - id: AT-5
      status: ATTACKED
      evidence: No network; remote URL paths are refused before resolve; OpenCode sqlite is not opened so workspace paths and credentials stay out of the record.
      artifacts: [scripts/sepmo_usage.py]
    - id: AT-6
      status: N/A
      justification: No product execution surface and no auth change.
    - id: AT-7
      status: ATTACKED
      evidence: Majority-failed JSONL and truncated JSON are UsageError; CLI exits 1.
      artifacts: [python/repark-parity/tests/test_sepmo_usage.py]
    - id: AT-8
      status: N/A
      justification: Docs-and-scripts unit; make ci stays native-build-free; no crate change.
    - id: AT-9
      status: N/A
      justification: No new log pipeline; missing data is a field on the record, not a metric sink.
    - id: AT-10
      status: ATTACKED
      evidence: pins citations live in the test module, scripts/map.md, docs maps, and this ledger.
      artifacts: [python/repark-parity/tests/test_sepmo_usage.py, scripts/map.md, docs/sepmo/telemetry/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Brief: lane `sepmo-efficiency-brief.md` (PR #376 copy)
- Inventory: [../../../docs/sepmo/telemetry/inventory.md](../../../docs/sepmo/telemetry/inventory.md)
- Collector: [../../../scripts/sepmo_usage.py](../../../scripts/sepmo_usage.py)
- Schema: [../../../docs/sepmo/telemetry/usage-record.schema.json](../../../docs/sepmo/telemetry/usage-record.schema.json)
- Pins: [../../../python/repark-parity/tests/test_sepmo_usage.py](../../../python/repark-parity/tests/test_sepmo_usage.py)

## Round 2 — critic remediations (2026-09-06)

Critic FAIL: 2 S1, 3 S2, 2 S3. All seven findings addressed in this round.

| id | sev | disposition |
|---|---|---|
| F1 | S1 | Muse tokens from `$HOME/.local/share/muse/sessions/…/session.jsonl` via `runs.tsv` column 6 (`stream.id` fallback). Per-turn `model_completed.usage` summed; snapshot `tokenUsage.cumulative` cross-checked. Cost stays null with that reason. Inventory §2/§3, C-001/C-002, wrapper patch, and a sanitized session fixture updated. |
| F2 | S1 | `USAGE_KEYS["tokens_cached"]` includes `cache_read_input_tokens`. `tokens_cache_write` added. `modelUsage` is read. Grok missing-reason is per-field. This unit's live numbers are in inventory §2. |
| F3 | S2 | Grok `with-tokens` fixture rebuilt from the live `out.json` (session id and text replaced). |
| F4 | S2 | `_reject_remote` runs on the raw argument before `Path.resolve`. Red test `test_remote_url_is_rejected_before_resolve`. |
| F5 | S2 | Any unparsed JSONL line sets `truncated` and `missing_reason.jsonl`. Truncated Muse JSONL with no `run.terminal.completed` is `UsageError`. Fixture `malformed/truncated-tail`. |
| F6 | S3 | Muse `role` is null unless argv names it. Inventory §4 is a human lane classification. |
| F7 | S3 | §4 names lane `ex26`, not unit EX-26. |
