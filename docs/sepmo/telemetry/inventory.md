# SEPMO worker telemetry inventory (E-0)

**Opened:** 2026-09-06. **Class:** campaign. **State:** live for the SEPMO
efficiency pilot.

**Retires:** freeze and archive this file when E-7 records the pilot outcome, or
when a successor inventory supersedes it. Link the successor from the archived
record.

**Measured:** 2026-09-06 on this machine. Sources are the worker adapters (read
only, outside the repo) and real run directories under `/tmp/muse-worker/` and
the OpenCode sqlite at `$HOME/.local/share/opencode/opencode.db`. Claude
sub-agent transcripts are not accessible. No field here is assumed: a missing
field is recorded as unavailable.

The collector that turns a run directory into one record is
[`scripts/sepmo_usage.py`](../../../scripts/sepmo_usage.py). The record schema is
[`usage-record.schema.json`](usage-record.schema.json). Wrapper edits stay
outside the repo; proposed patches live in
[`wrapper-patches/`](wrapper-patches/map.md).

## 1. Adapters in scope

RePark units on this fleet run on four worker interfaces. Desktop-application
controls that those CLIs do not expose are out of reach.

| Adapter | Binary / form | Effort / model flags | Default run-dir layout |
|---|---|---|---|
| Muse Spark | `muse exec --json --prompt-file … --model <id> --reasoning-effort <level> --max-model-steps N --approval-mode never --workspace <clone> …` | `--model` default `muse-spark-1.3`; `--reasoning-effort` `low\|medium\|high\|xhigh\|max\|ultra` (baseline used `max`); `--role worker\|critic` is in the prompt, not the argv | `/tmp/muse-worker/<lane>/<utc>/{prompt.md,cmd.txt,out.jsonl,stderr.log,exit,handback.json}`; `/tmp/muse-worker/runs.tsv` one row per round |
| opencode / kilo (GLM) | `opencode run --dir <clone> --agent worker\|critic --format json [--variant high] --auto <prompt>` (kilo is the same flag surface) | `--model` default `zai/glm-5.3-flash`; `--variant` is the effort stand-in; `--max-turns` becomes agent `maxSteps` in `config.json` | `/tmp/oc-worker/<lane>/<utc>/{prompt.md,config.json,cmd.txt,out.ndjson,stderr.log,exit,handback.json}`; `/tmp/oc-worker/runs.tsv` |
| Grok | `grok --cwd <clone> --model <id> --reasoning-effort <level> --prompt-file … --agent <role> --output-format json --max-turns N …` | `--model` default `grok-4.6`; `--reasoning-effort` (baseline used `high`); `--agent` `sepmo-actor\|critic-quality\|critic-security\|critic-logic` | `/tmp/grok-worker/<lane>/<utc>/{prompt.md,cmd.txt,out.json,stderr.log,exit}`; `/tmp/grok-worker/runs.tsv` |
| Claude sub-agents | Orchestrator-spawned sub-agents inside a Claude session (not a `*-worker.sh` run dir) | Effort and model are session / spawn flags in the desktop app, not a file in a run dir | No run-dir layout is available to this collector. Transcripts are not accessible. |

## 2. Field availability (measured)

Keep missing telemetry as unavailable. Do not convert it to zero. Cached input
is not added to uncached input. Reasoning tokens are not added to output.

### Muse

Event stream: JSONL records with `payload_type` and `payload`. `recorded_at` on
the 2026-09-05/06 files is a near-constant counter (`1780531400000000` plus a
small sequence), so it is **not** wall time. Wall time is `cmd.txt` mtime to
`exit` mtime.

| Field | Available? | Source | Unit |
|---|---|---|---|
| input tokens | no | `out.jsonl` has no model usage object | — |
| output tokens | no | same | — |
| cached input | no | same | — |
| cost | no | same; `runs.tsv` has no cost column | — |
| wall time | yes | `cmd.txt` mtime → `exit` mtime | seconds |
| step / turn count | yes | unique `payload.task_id` on `task.lifecycle.started` (this is the `--max-model-steps` unit) | count |
| tool-call count | yes | `payload_type=tool.result`; this is the `runs.tsv` tools column | count |
| exit status | yes | `exit` file (empty or absent if the round is still running) | process exit |
| model id | yes | `--model` in `cmd.txt`; also `run.model.configured.model_id` | identifier |
| effort | yes | `--reasoning-effort` in `cmd.txt` | enum |
| commits | yes when `handback.json` exists | `handback.json` `commits` length | count |

Finding: Muse `out.jsonl` has no model token counts at all. Some `tool.result`
`text` blobs carry `original_output_tokens`. That number is the tool's output
size for truncation, not billed model usage. The collector must not treat it as
`tokens_out`.

`runs.tsv` columns (wrapper): lane, stamp, rc, `muse`, model, session uuid,
tool-call count, terminal, `hb=yes|no`.

### opencode / kilo (GLM)

No `/tmp/oc-worker/` run directories from 2026-09-05/06 exist on this machine.
The wrapper layout and the NDJSON shape are taken from `oc-worker.sh` (read
only). Token and cost columns **do** exist in the OpenCode sqlite:

`$HOME/.local/share/opencode/opencode.db` table `session`:

| Column | Meaning (as stored) |
|---|---|
| `tokens_input` | uncached input tokens (integer) |
| `tokens_output` | output tokens (integer) |
| `tokens_reasoning` | reasoning tokens, separate from output (integer) |
| `tokens_cache_read` | cached input tokens (integer); do not add to `tokens_input` |
| `tokens_cache_write` | cache-write tokens (integer; 0 on the sampled GLM rows) |
| `cost` | session cost in USD (real) |
| `model` | JSON `{"id":"glm-5.3-flash","providerID":"zai","variant":"default"}` |
| `agent` | `worker` or `critic` |
| `time_created` / `time_updated` | unix ms |

Per-message `message.data` and `part.data` also carry
`tokens.{input,output,reasoning,cache.{read,write}}` and `cost`. A
`part.type=step-finish` event is one step. The wrapper counts those events in
`runs.tsv` and sums their `cost`.

Sampled GLM sessions (EX-15..EX-24, critic rounds) all have non-zero
`tokens_input`, `tokens_output`, `tokens_reasoning`, `tokens_cache_read`, and
`cost`. `tokens_cache_write` is 0 on those rows.

The collector reads a run directory's `out.ndjson` (and `config.json`), not the
sqlite. Opening the sqlite would pull workspace paths and credentials. When a
run dir is present, step-finish events carry the same token fields.

| Field | In a run dir (`out.ndjson`) | In sqlite (when run dirs are gone) |
|---|---|---|
| input tokens | yes, per `step-finish` | `session.tokens_input` |
| output tokens | yes | `session.tokens_output` |
| cached input | yes, `tokens.cache.read` | `session.tokens_cache_read` |
| cost | yes, sum of step `cost` | `session.cost` (USD) |
| wall time | `cmd.txt` → `exit` mtimes | `time_updated - time_created` (ms) |
| steps | count of step-finish events | not a dedicated column |
| tool calls | tool events in the NDJSON | not a dedicated column |
| exit | `exit` file | not stored |
| model / effort | `config.json` `model`; `--variant` | `session.model` JSON |

### Grok

The wrapper writes one JSON object to `out.json` at process exit
(`--output-format json`). The extractor in `grok-worker.sh` reads `sessionId`,
`stopReason`, `num_turns`, `total_cost_usd`. The skill records that
`runs.tsv` carries `total_cost_usd` (a one-turn probe ≈ $0.005).

This unit's own Grok round had `out.json` still empty while the process was
running, so a completed live object was not on disk at inventory time. Token
keys are therefore **unconfirmed on a live object**. The collector reads them
when present (`usage.input_tokens` and peers) and otherwise records
`missing_reason`. Grok CLI OTEL metrics name `grok_code.token.usage` with
`type=input|output|reasoning|cache_read`, but that stream is opt-in and is not
in the run dir.

| Field | Available in run dir? | Source | Unit |
|---|---|---|---|
| input tokens | unconfirmed on a completed live object | parse `usage` / `tokens` if present; else missing | provider tokens |
| output tokens | unconfirmed | same | provider tokens |
| cached input | unconfirmed | same | provider tokens |
| cost | yes, once `out.json` is written | `total_cost_usd` | USD |
| wall time | yes | `cmd.txt` → `exit` mtimes | seconds |
| step / turn count | yes, once `out.json` is written | `num_turns` | count |
| tool-call count | unconfirmed | `num_tool_calls` if present; else missing | count |
| exit | yes | `exit` file | process exit |
| model / effort | yes | `--model`, `--reasoning-effort` | identifier / enum |

### Claude sub-agents

Not available. There is no worker run-dir layout and the orchestrator transcripts
are not accessible to this collector. Every usage field is recorded with
`missing_reason` pointing at that gap. Do not invent tokens from word counts.

## 3. Frozen baseline — Muse rounds 2026-09-05/06

Adapter for every row: Muse Spark `muse-spark-1.3`, effort `max`. Token fields
are absent on every row. Wall time is `cmd.txt` mtime to `exit` mtime. Steps
are unique `task.lifecycle.started` task ids. Tool calls are `tool.result`
events (the wrapper `runs.tsv` tools column). Measured 2026-09-06.

| Lane | Stamp | Exit | Wall s | Steps | Tool calls | Hand-back | Commits |
|---|---|---:|---:|---:|---:|---|---:|
| aggavg | 20260905T125630Z | 0 | 7617.1 | 791 | 308 | yes | 8 |
| aggavgb | 20260905T172846Z | 0 | 2597.1 | 139 | 50 | yes | 3 |
| aggavgr2 | 20260905T160318Z | 0 | 2909.7 | 490 | 208 | yes | 8 |
| aggavgr3 | 20260905T170640Z | empty | 1042.4 | 136 | 64 | no | — |
| approxpct | 20260905T214015Z | 0 | 11015.3 | 1150 | 489 | yes | 9 |
| catio-fork | 20260905T132221Z | 0 | 2814.2 | 229 | 90 | yes | 3 |
| catio2 | 20260905T170733Z | empty | 989.9 | 240 | 105 | no | — |
| catio2b | 20260905T172846Z | 0 | 4414.9 | 660 | 268 | yes | 7 |
| catio2r2 | 20260905T184534Z | 0 | 2564.5 | 305 | 133 | yes | 4 |
| catio3 | 20260905T224409Z | 0 | 4617.5 | 495 | 187 | yes | 3 |
| catio3r2 | 20260906T011026Z | absent | — | 6 | 2 | no | — |
| catiokey | 20260905T184621Z | 0 | 2110.1 | 211 | 92 | yes | 1 |
| catiokeyr2 | 20260905T202042Z | 0 | 810.6 | 127 | 56 | yes | 1 |
| cdf1 | 20260905T113405Z | 0 | 13214.7 | 883 | 325 | yes | 10 |
| cdf1r2 | 20260905T154008Z | 0 | 1907.8 | 258 | 106 | yes | 4 |
| ex25 | 20260905T214117Z | 0 | 3849.9 | 408 | 163 | yes | 5 |
| ex26 | 20260906T005549Z | absent | — | 189 | 86 | no | — |
| icescan | 20260905T183353Z | 0 | 13714.2 | 1528 | 601 | yes | 5 |
| icescanfr2 | 20260905T225134Z | 143 | 1279.8 | 221 | 105 | no | — |
| icescanfr3 | 20260905T231257Z | 0 | 2470.9 | 289 | 109 | yes | 1 |
| nullab2 | 20260905T214006Z | 0 | 10395.8 | 1293 | 544 | yes | 7 |
| nullab2r2 | 20260906T010827Z | absent | — | 39 | 16 | no | — |
| types1 | 20260905T125630Z | 0 | 15087.8 | 2346 | 902 | no | — |
| types1b | 20260905T172846Z | 0 | 4858.3 | 451 | 183 | yes | 4 |
| types1r2 | 20260905T170901Z | empty | 912.7 | 146 | 60 | no | — |
| types1r3 | 20260905T185127Z | 0 | 522.2 | 128 | 56 | yes | 1 |
| types1r4 | 20260905T191305Z | 0 | 5145.1 | 537 | 222 | yes | 4 |
| types1r5 | 20260905T210413Z | 0 | 4523.3 | 329 | 135 | yes | 3 |

Raw evidence for the three reconciliation samples (collector vs a second pass
over the same files, 2026-09-06):

| Lane | Stamp | Raw `tool.result` | Raw unique `task.lifecycle.started` | `exit` bytes | Wall from mtimes |
|---|---|---:|---:|---|---:|
| ex25 | 20260905T214117Z | 163 | 408 | `0\n` | 3849.9 s |
| cdf1 | 20260905T113405Z | 325 | 883 | `0\n` | 13214.7 s |
| icescanfr2 | 20260905T225134Z | 105 | 221 | `143\n` | 1279.8 s |

Grok baseline on this machine: one in-flight round
`/tmp/grok-worker/sepmoe0/20260906T010334Z` with empty `out.json` (the process
had not exited). OpenCode baseline: sqlite session rows only; no
`/tmp/oc-worker/` run dirs.

## 4. Pilot strata mapped onto the units that ran

Brief §12 strata. The 2026-09-05/06 Muse sample is the frozen set. No passive
prose-only unit ran in this window.

| Stratum (brief §12) | Units that ran | Lanes (role of the round) |
|---|---|---|
| Passive prose and navigation | none in this sample | — |
| Mechanical edits with precise acceptance criteria | EX-25, EX-26 | `ex25` actor; `ex26` in flight |
| Python facade work with Rust ownership constraints | PERF-FACADE-CDF-1, TYPES-1, nullability follow-on | `cdf1` actor, `cdf1r2` critic round; `types1` actor, `types1b`/`types1r3`/`types1r4`/`types1r5` remediation, `types1r2` critic round; `nullab2` actor, `nullab2r2` critic round in flight |
| Rust semantic changes | PERF-AGG-AVG-1, approx-percentile | `aggavg` actor, `aggavgr2`/`aggavgr3` critic/remediation, `aggavgb` remediation; `approxpct` actor |
| Sensitive write or recovery behavior | catalog-IO, Iceberg scan | `catio-fork`/`catio2`/`catio3` actor, `catio2b` remediation, `catio2r2` critic round, `catiokey` fix unit, `catiokeyr2` remediation; `icescan` actor, `icescanfr2` critic round (exit 143), `icescanfr3` remediation |
| Broken environment or ambiguous evidence | the interrupted critic round | `icescanfr2` exit 143, no hand-back; empty-`exit` rounds `aggavgr3`, `catio2`, `types1r2` |

Kind tags the brief asked for:

| Kind | Lanes |
|---|---|
| perf unit | `cdf1`, `aggavg`, `icescan`, `catio-fork`, `catio2`, `catio3`, `types1` |
| fix unit | `catiokey`, `approxpct`, `nullab2` |
| EX batch | `ex25`, `ex26` |
| critic round | `cdf1r2`, `aggavgr2`, `types1r2`, `catio2r2`, `icescanfr2`, `nullab2r2`, `catio3r2` |
| remediation round | `types1b`, `types1r3`, `types1r4`, `types1r5`, `aggavgb`, `aggavgr3`, `catio2b`, `catiokeyr2`, `icescanfr3` |

## 5. Measurement-contract notes for E-1

- Null, never zero, when the adapter does not report a field.
- Muse wall time uses file mtimes, not `recorded_at`.
- OpenCode `tokens_cache_read` is cached input. Do not add it to `tokens_input`.
- OpenCode `tokens_reasoning` is not a subset to fold into `tokens_out`.
- Grok `total_cost_usd` is USD as reported by the CLI. Do not derive it from
  tokens.
- Claude remains unavailable until a run-dir layout exists.

pins: sepmo-e0-e1/C-001, C-002, C-003, C-008
