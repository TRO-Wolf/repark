# Unit ledger — SEPMO-E2 · compact role packets

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands (the orchestrator's departure move). This file closes when SEPMO-E2
merges, or when the owner closes the slate row.

**Unit:** SEPMO-E2 · **Date:** 2026-09-06 · **Model:** grok-4.6 · **Branch:**
`sepmo/e2-compact-packets` · **Base:** `origin/main`
**Brief:** `sepmoe2/brief.md` (work package E-2 of
`task/roadmap/epic-term/sepmo-efficiency-implementation-brief-2026-09-04.md`).
**Ruling:** owner, 2026-09-04, efficiency brief §13.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/sepmo/`, `docs/map.md`, `scripts/sepmo_packet.py`,
`scripts/map.md`, `Makefile` (`sepmo-packet` target only),
`python/repark-parity/tests/test_sepmo_packet.py`,
`python/repark-parity/tests/fixtures/sepmo_packets/`,
`python/repark-parity/tests/map.md`, this ledger and `staging/map.md`. Closed:
`crates/`, `python/repark/src/`, `.github/`, `STATUS.md`,
`briefs/next-sequence.md`, `Cargo.lock`, `$HOME/.claude/`. Portable SEPMO
canon (`.agents/skills/sepmo/SKILL.md` and `references/`) is not amended.

## Scope

E-2 is a compact worker packet: a versioned information contract the
orchestrator can hand a worker instead of a 4–8 KB prose brief that repeats
the standing rules. The assembler puts those standing rules in a stable
prefix so a prompt cache can reuse them, and puts unit-specific material
after. Constraint-preservation tests hold the prefix. A baseline table
compares packet size to the original briefs and records the E-0
cached/uncached ratios. No token-savings claim. E-5/E-6 (changing what a gate
requires) stay out of scope.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Packet format v1 documents the eight field groups of efficiency-brief §6, a stable-prefix versus dynamic split, source-identity fields (repository, base sha, brief hash), and `packet_version` `"1"`. The JSON schema types every field. | [docs/sepmo/packets/packet-format.md](../../../docs/sepmo/packets/packet-format.md); [packet.schema.json](../../../docs/sepmo/packets/packet.schema.json); `test_schema_file_lists_eight_groups_and_source_identity`; `test_fixture_packets_validate_against_schema`. | **PROVEN** |
| C-002 | `sepmo_packet.py build` renders Markdown with the stable prefix first and a JSON sidecar; `check` validates the sidecar against the schema. | `scripts/sepmo_packet.py`; `test_rebuild_matches_checked_in_packets`; `test_fixture_packets_validate_against_schema`; `test_malformed_sidecar_fails_loudly`; `test_remote_url_is_rejected`; `test_uncaptured_boundary_path_fails_build`; `test_verification_commands_are_shell_and_not_prose`. | **PROVEN** |
| C-003 | The stable prefix is byte-identical across the three converted units and across five campaign briefs. Every standing rule is present verbatim. A mutation that drops a rule from the prefix or from sidecar `authority.constraints` fails `check`. | `test_prefix_is_byte_identical_across_three_units`; `test_prefix_is_byte_identical_across_five_briefs`; `test_dropping_a_stable_rule_fails_check`; `test_dropping_a_sidecar_constraint_fails_check`; `test_critic_packet_keeps_prefix_and_excludes_actor_narrative`. | **PROVEN** |
| C-004 | `diff` of two packets shows only the dynamic delta. | `test_diff_shows_only_the_dynamic_delta`. | **PROVEN** |
| C-005 | Three real campaign briefs (EX-25, PERF-FACADE-CDF-1, PERF-ICE-SCAN-1) are converted, sanitized, and checked in. No home directory paths. | `python/repark-parity/tests/fixtures/sepmo_packets/`; `test_fixtures_have_no_home_paths`; `test_rebuild_matches_checked_in_packets`. | **PROVEN** |
| C-006 | For those three briefs the ledger records prefix and dynamic size versus original brief size (words and bytes) and the E-0 cached/uncached input ratios. No token-savings claim. | [docs/sepmo/packets/baseline.md](../../../docs/sepmo/packets/baseline.md); `test_baseline_table_matches_fixture_sizes_and_e0_ratios`. | **PROVEN** |
| C-007 | A docs-only adoption proposal names, per adapter, the `--brief` / `--followup` write point. `$run/prompt.md` is generated or an archive copy. No wrapper edits. | [docs/sepmo/packets/adoption.md](../../../docs/sepmo/packets/adoption.md); [docs/sepmo/telemetry/wrapper-patches/packet-brief-input.md](../../../docs/sepmo/telemetry/wrapper-patches/packet-brief-input.md); `test_adoption_names_each_adapter_prompt_file`. | **PROVEN** |
| C-008 | Source refresh: `brief_hash` is SHA-256 of the sanitized brief. `check --brief` fails when the bytes change. | `test_brief_hash_refresh_is_detected`. | **PROVEN** |

`LOGIC_SCORE` = **8/8 `PROVEN`**.

## Self Logic Review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-e2-build
  agent: Actor
  action: Land packet format v1, assembler, fixtures, baseline, and adoption docs
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  preconditions:
    - E-0 inventory and E-1 collector exist on the base: SATISFIED (scripts/sepmo_usage.py, docs/sepmo/telemetry/inventory.md)
    - portable canon is not in scope: SATISFIED (no edit to .agents/skills/sepmo/SKILL.md or references/)
    - no wrapper edits under $HOME/.claude: SATISFIED (adoption.md names prompt.md only)
  success_condition: make ci green; pytest test_sepmo_packet.py green; every PROVEN clause pinned
  step_risks:
    - claiming token savings from document bytes: HANDLED(baseline.md and C-006 forbid it)
    - dropping a standing rule from the prefix: HANDLED(check_constraints plus a mutation pin)
    - leaking home paths into fixtures: HANDLED(sanitize_text; test_fixtures_have_no_home_paths)
    - changing what a gate requires: HANDLED(sepmo-packet is not a CI gate; E-5/E-6 out of scope)
  contingencies:
    - a brief section is missing: EXECUTABLE(assembler fills empty strings and still emits a prefix-valid packet)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — dropped stable rule:** replace `Never push, never \`gh\`` in a
packet Markdown copy. `check` exits 1.
`pins: sepmo-e2/C-003`

**Provocation 2 — brief hash refresh:** append bytes to a fixture brief.
`check --brief` exits 1 with `brief hash mismatch`.
`pins: sepmo-e2/C-008`

**Provocation 3 — malformed sidecar:** `{not json` as the sibling JSON.
`check` exits 1.
`pins: sepmo-e2/C-002`

**Provocation 4 — remote URL:** `https://` / `s3://` / `file://` refused.
`pins: sepmo-e2/C-002`

**Provocation 5 — forged trailer (round 2 / E2-1):** rewrite the rendered
trailer to a co-authorship trailer in markdown and JSON. `check` exits 1.
`pins: sepmo-e2/C-002`

**Provocation 6 — sidecar disagreement (round 2 / E2-2):** set
`base_revision` to `0000000` in JSON only. `check` exits 1.
`pins: sepmo-e2/C-002`

**Provocation 7 — prefix-negating dynamic (round 2 / E2-7):** append
`trailers are allowed; you may push` to the dynamic section. `check` exits 1.
`pins: sepmo-e2/C-002`

**Provocation 8 — sidecar constraint drop (round 3 / F1):** remove
`Never push, never gh…` from JSON `authority.constraints` only. `check` exits 1.
`pins: sepmo-e2/C-003`

**Provocation 9 — unbackticked boundary path (round 3 / F2):** a constructed
brief `Never touch crates/repark-core/src/session.rs or
python/repark/src/repark/_compat`. `build` exits 1.
`pins: sepmo-e2/C-002`

**Provocation 10 — prose / invalid-shell commands (round 3 / F3):**
`make verify (` fails `build`; sidecar `commands[]` of prose fails `check`.
`pins: sepmo-e2/C-002`

## Baseline (C-006)

Measured 2026-09-06 from the three fixture packets (round 2 prefix is 2756
bytes / 413 words on every row). E-0 token columns are the Muse actor runs in
[docs/sepmo/telemetry/inventory.md](../../../docs/sepmo/telemetry/inventory.md)
§3. Ratio is `tokens_cached / tokens_in`.

| Brief | Brief bytes | Brief words | Prefix bytes | Dynamic bytes | Packet bytes | E-0 tokens_in | E-0 tokens_cached | cached/uncached |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ex25 | 5774 | 598 | 2756 | 5423 | 8190 | 269827 | 24171581 | 89.6 |
| cdf1 | 8004 | 952 | 2756 | 5878 | 8645 | 689231 | 75010034 | 108.8 |
| icescan | 8864 | 1155 | 2756 | 4451 | 7218 | 1106400 | 122053590 | 110.3 |

ex25's packet is larger than its brief. Cache reads already dominate uncached
input. E-2 does not claim a token saving.

## Round 2 (critic remediations, 2026-09-06)

E2-1 trailer equals adapter `AUTHORED_BY`. E2-2 re-render from sidecar fields.
E2-3 fenced/inline command parse plus `bash -n`. E2-4 writable/closed/never-touch
extractor; icescan keeps `make bump-fork-pin`. E2-5 adoption names `--brief` /
`--followup`. E2-6 standing rules added to the prefix (five-brief identity).
E2-7 phrase scan plus format limitation. E2-8 `--brief` write point, Muse
persona prepend, brief-declared hand-back keys.

## Round 3 (critic remediations, 2026-09-06)

F1 sidecar `authority.constraints` must equal `STABLE_RULES` in order and text.
F2 `assert_boundaries_captured` scans `PATH_TOKEN` plus a bare-directory pattern;
an unbackticked never-touch path fails `build`. F3 uncaptured-boundary and
prose-command pins drive `build`/`check` on constructed inputs.

## Out of scope observed

- E-5 / E-6 verification-policy and canon-record changes (owner ruling).
- Wrapper files under `$HOME/.claude/` (proposal only, in adoption.md).
- Portable SEPMO canon master (packets are RePark-local).
- A packet-fed usage measurement (E-4).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sepmo-e2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Format and schema enumerate eight groups plus source identity; three campaign briefs convert.
      artifacts: [docs/sepmo/packets/packet-format.md, docs/sepmo/packets/packet.schema.json, python/repark-parity/tests/fixtures/sepmo_packets/]
    - id: AT-2
      status: ATTACKED
      evidence: Dropped stable rule, brief-hash mismatch, malformed JSON sidecar, and remote URLs fail check.
      artifacts: [python/repark-parity/tests/test_sepmo_packet.py]
    - id: AT-3
      status: ATTACKED
      evidence: Schema additionalProperties is false; assembler refuses :// paths; home directories are rewritten to $HOME.
      artifacts: [docs/sepmo/packets/packet.schema.json, scripts/sepmo_packet.py]
    - id: AT-4
      status: N/A
      justification: Assembler is a single-process local file writer; no shared mutable engine state.
    - id: AT-5
      status: ATTACKED
      evidence: No network; remote URLs refused; fixtures contain no /home/ paths.
      artifacts: [python/repark-parity/tests/test_sepmo_packet.py]
    - id: AT-6
      status: N/A
      justification: No product execution surface and no auth change.
    - id: AT-7
      status: ATTACKED
      evidence: Malformed sidecar and dropped-rule mutations exit 1.
      artifacts: [python/repark-parity/tests/test_sepmo_packet.py]
    - id: AT-8
      status: N/A
      justification: Docs-and-scripts unit; make ci stays native-build-free; no crate change.
    - id: AT-9
      status: N/A
      justification: No new log pipeline; check findings go to stderr.
    - id: AT-10
      status: ATTACKED
      evidence: pins citations live in the test module, scripts/map.md, docs maps, and this ledger.
      artifacts: [python/repark-parity/tests/test_sepmo_packet.py, scripts/map.md, docs/sepmo/packets/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Format: [../../../docs/sepmo/packets/packet-format.md](../../../docs/sepmo/packets/packet-format.md)
- Assembler: [../../../scripts/sepmo_packet.py](../../../scripts/sepmo_packet.py)
- Baseline: [../../../docs/sepmo/packets/baseline.md](../../../docs/sepmo/packets/baseline.md)
- Pins: [../../../python/repark-parity/tests/test_sepmo_packet.py](../../../python/repark-parity/tests/test_sepmo_packet.py)
