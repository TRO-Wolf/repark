# SEPMO compact worker packet format v1

**Opened:** 2026-09-06. **Class:** campaign. **State:** live for the SEPMO
efficiency pilot.

**Retires:** freeze and archive this file when E-7 records the pilot outcome, or
when a successor format supersedes it. Link the successor from the archived
record.

This is packet format **v1**. The assembler is
[`scripts/sepmo_packet.py`](../../../scripts/sepmo_packet.py). The machine schema
is [`packet.schema.json`](packet.schema.json). Portable SEPMO canon is not
amended here; packets are a RePark-local rendering of the current contract.

A packet is two artifacts with the same stem: Markdown for the worker prompt,
and a JSON sidecar for `check` / `diff`. The Markdown puts the **stable prefix
first** and the **dynamic section after**, so a prompt cache can reuse the
prefix across units. Changing timestamps, run ids, or status prose must not
precede the prefix.

`packet_version` is `"1"`. A later version is a new format, not an in-place
edit of v1 packets.

## Stable prefix versus dynamic section

| Section | What it carries | Change rule |
|---|---|---|
| Stable prefix | Rules that never change between units | Byte-identical across units. `check` compares it to the assembler constant. |
| Dynamic | Unit, role, adapter, source identity, roster, deliverable, gates, hand-back | May change per unit. `diff` reports only this section. |

The stable prefix states these constraints, verbatim:

- comment ban for Python `#`, Rust `//` `///` `//!`, TOML/YAML/shell `#`, with
  the forced `/// # Errors` exception and the fork ASF / `deny(missing_docs)`
  exception; never delete a pre-existing comment; `pins: <unit>/C-NNN`; the
  pre-commit `git diff --cached | grep` self-check
- identity / Authored-By trailer binding
- push / `gh` / `aws` / `--no-verify` / `.github/` prohibitions
- no edits to `$HOME/.claude/`; wrapper patches under
  `docs/sepmo/telemetry/wrapper-patches/`
- no home paths in the tree
- never change dependencies or `Cargo.lock` unless the packet names a
  sanctioned writer (`make bump-fork-pin`)
- cargo cap
- release module (`maturin develop --release`, `__debug_assertions__` False,
  quiet box)
- live-oracle provisioning (`--extra record` as `make parity-live`, ivy
  redirect, `PYSPARK_SUBMIT_ARGS`, `JAVA_HOME`, ANSI/UTC, JVM wait)
- size ceilings (ratchet down; new Python under 1000 lines; cap-table mirror
  in `test_cap_1_source_file_line_cap.py`)
- do not change what any gate requires
- `map.md` lockstep; no amends
- frozen ledgers in `completed/` and `archive/`

The adapter-specific Authored-By trailer lives in the dynamic identity block so
the prefix stays identical across adapters. The prefix tells the worker to use
the trailer named there.

`check` does not prove that the dynamic section agrees with the prefix. A
worker could still write a dynamic sentence that contradicts a stable rule.
`check` reds only the phrase scan for prefix-negating sentences
(`trailers are allowed`, `you may push`, `comments are fine`, `--no-verify`
without `never`). It also requires the rendered trailer to equal the adapter
`AUTHORED_BY` entry and refuses any other trailer form. It re-renders the
dynamic section from the JSON sidecar fields and requires that text (and the
prefix hash) to match the markdown. Sidecar `authority.constraints` must equal
the assembler `STABLE_RULES` tuple in order and text; dropping a rule from the
JSON list fails `check` even when the markdown prefix is unchanged.

## Source identity and refresh

| Field | Type | Meaning |
|---|---|---|
| `repository` | string | Product repository name (`repark`) |
| `base_revision` | string | Base git sha (7–40 hex) |
| `working_diff_identity` | string | Working-tree identity at assembly; empty when unknown |
| `untracked_inputs` | string[] | Untracked inputs the unit may read |
| `brief_hash` | string | SHA-256 of the sanitized brief bytes used at assembly |

`check --brief <md>` recomputes the hash and fails on mismatch. A matching hash
detects that the brief file changed. It does not prove that a summary preserved
meaning. Constraint-preservation tests hold the stable rules. The worker still
reads the cited sources when an excerpt is not enough.

## Eight field groups

Field groups follow the efficiency brief §6. Types below match
[`packet.schema.json`](packet.schema.json).

### 1. Identity

| Field | Type | Description |
|---|---|---|
| `unit` | string | Unit id (lane or ledger key) |
| `role` | `actor` \| `critic` | Packet role |
| `attempt` | integer ≥ 1 | Attempt number |
| `packet_format_version` | `"1"` | Same as `packet_version` |
| `task_reference` | string | Brief title or task name |
| `adapter` | `muse` \| `grok` \| `glm` \| `opus` | Worker adapter |

### 2. Source identity

See the table above. Required on every packet.

### 3. Authority

| Field | Type | Description |
|---|---|---|
| `contract` | string | Engineering contract (`AGENTS.md`) |
| `binding_version` | string | Bound SEPMO spine version |
| `source_references` | string[] | Paths the worker may open for the full rule |
| `constraints` | string[] | Verbatim stable-prefix rules. `check` requires `list(constraints) == list(STABLE_RULES)` in order and text. |

Markdown does not restate the constraint sentences. It points at the prefix.

### 4. Scope

| Field | Type | Description |
|---|---|---|
| `objective` | string | What the unit must produce |
| `requirement_ids` | string[] | Unit / clause identifiers |
| `acceptance_criteria` | string | Deliverable and acceptance text |
| `exclusions` | string[] | Explicit out of scope |

A critic packet adds the Actor Self Logic Review and Actor narrative to
exclusions (efficiency brief F-5).

### 5. Implementation context

| Field | Type | Description |
|---|---|---|
| `relevant_files` | string[] | Paths named in the brief |
| `callers` | string[] | Callers the worker must inspect |
| `interfaces` | string[] | Interfaces in play |
| `dependency_decisions` | string[] | Dependency bounds already decided |
| `known_traps` | string[] | HALT conditions and durability notes |

### 6. Verification

| Field | Type | Description |
|---|---|---|
| `commands` | string[] | Required gate commands |
| `behavioral_cases` | string | Red-first / behavioral cases |
| `oracle_requirements` | string[] | Live oracle needs |
| `evidence_destinations` | string[] | Ledger, pins, hand-back |

### 7. Permissions and resources

| Field | Type | Description |
|---|---|---|
| `authorized_actions` | string[] | What this role may do |
| `ownership_boundaries` | string[] | Paths that stay closed |
| `resource_limits` | string[] | Cargo cap, JVM, disk |
| `escalation_conditions` | string[] | When to HALT |

### 8. Handoff

| Field | Type | Description |
|---|---|---|
| `expected_output_fields` | string[] | Hand-back JSON keys |
| `unresolved_decisions` | string[] | Open questions |
| `dependency_consumers` | string[] | Who reads the hand-back |

Actor keys include `status`, `commits`, `gates`, `notes`. Critic keys include
`findings`, `coverage_attestation`, `dispositions`, `evidence`. Brief-declared
hand-back keys (for example ex25 `covered` / `stayed`) are kept.

## Assembler commands

```
python3 scripts/sepmo_packet.py build --unit <id> --role actor|critic \
    --base <sha> --brief <md> [--adapter muse|grok|glm|opus] \
    [--attempt N] [--working-diff <id>] --out-dir <dir>
python3 scripts/sepmo_packet.py check <packet.md|packet.json> [--brief <md>]
python3 scripts/sepmo_packet.py diff <packet-a> <packet-b>
```

`build` writes `<unit>-<role>.md` and `<unit>-<role>.json`. `check` validates
the sidecar against this schema, requires the markdown prefix to equal the
assembler prefix (byte identity and prefix hash), requires every constraint
string to appear in that prefix, re-renders the dynamic section from the
sidecar fields, requires the rendered trailer to match the adapter
`AUTHORED_BY` entry, requires every `commands[]` entry to parse as a shell
command (`bash -n`, no backticks or prose), and reds prefix-negating phrases
in the dynamic section. `diff` prints a unified diff of `dynamic_markdown`
only.

Safety boundaries in the brief are preserved: `build` extracts `Writable:` /
`Closed:` / `never touch` / `untouched` lists verbatim and fails when a
boundary clause names a path the lists did not keep. It does not synthesize a
dependency rule the brief did not state.

pins: sepmo-e2/C-001, C-002
