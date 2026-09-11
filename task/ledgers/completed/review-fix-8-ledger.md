# Unit ledger — REVIEW-FIX-8 · the PROFILES-1 probe is re-runnable and its table is true

**Unit:** REVIEW-FIX-8 · **Date:** 2026-09-11 · **Branch:** `fix/review-fix-8-conf-unread-1` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The REVIEW-1 sweep's Q-21 (probe not idempotent), Q-22 (three
UNREAD rows refuse invalid values at build), Q-23 (summary miscounts), Q-40
(`write.*` rows are table properties), Q-41 (no NOT MEASURED state), Q-55
(writes under tracked `docs/perf/`) and Q-56 (no discovery guard), plus the
RF-4 ruling narrowing CONF-UNREAD-1 to four DataFusion keys and handing the
three `repark.*` keys and two Iceberg properties to this card as
accepted-at-session / applied-at-table.

**Not in this step:** CONF-UNREAD-1 (next round owns the four DataFusion rows),
any engine code, any dependency file, `STATUS.md`, `briefs/next-sequence.md`.
The card Home names `scripts/profiles1_probe.py`; that path never existed —
the probe lives at `docs/perf/profiles-1-probe/profiles1_probe.py` (the Q-21
citation `scripts/profiles1_probe.py:92` is the same file's `write_parquet`
line). The probe stays where it is; the doc and map record the true path.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

## PROPOSITION LEDGER — REVIEW-FIX-8 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: the probe writes to a unique temporary directory per run, never under `docs/perf/`, and is idempotent — two runs in a row exit 0 with agreeing outputs. | `test_probe_runs_twice_with_identical_output` | **PROVEN** | Red first (§Red first: second run died `PATH_ALREADY_EXISTS` on the first run's `data/`). Green after: `2 passed in 13.51s`; both scripts use `tempfile.mkdtemp` per run, plans scrub the work prefix to `<work>`, two full outputs are byte-identical (`cmp` clean), and the pin snapshots the probe directory before/after (no staged file). Probe 2 run twice by hand: exit 0, identical, empty stderr. |
| C-002 | D-1a: every session the probe opens has discovery disabled via `REPARK_CONFIG=""` — a poisoned discovered file cannot reach a measurement. | `test_probe_ignores_a_poisoned_discovered_config` | **PROVEN** | Red first (§Red first: the poisoned `repark.toml` broke session build with `invalid DataFusion session config ... 'notanumber'`). Green after: both scripts set `os.environ["REPARK_CONFIG"] = ""` before opening any session (`discover` returns `None` on the empty value, `discovery.rs:15-18`), and the pin runs the full probe under a poisoned `HOME` and a poisoned CWD with no `REPARK_CONFIG` in its own environment — exit 0. |
| C-003 | D-2: the table gains the `VALIDATED` state and `concurrency_limit`, `file_scoped_rewrite`, `scan_pruning` move into it. | validation section re-run on the green tree | **PROVEN** | The green output refuses all nine validation probes, including `repark.scan.concurrency_limit=0/abc` and both spellings of `file_scoped_rewrite/scan_pruning=maybe`; the doc table carries the `VALIDATED` state and the three rows with their session-parse cites (`session.rs`, `target_scan.rs:99`, `merge/mod.rs:219,226`, `residual.rs:168`). |
| C-004 | RF-4: the two `write.*` rows are re-described as accepted-at-session / applied-at-table, measured per key, with no laundered row. | manual table-property measurement (scratch, `/tmp/rf8_table_probe*.py`) | **PROVEN** | Session side: `bogus` distribution-mode and `abc` target-size are both accepted and read back (no session validation). Table side: `write.target-file-size-bytes='4096'` as a TBLPROPERTY makes a MERGE rewrite land 50 data files against 5 on default; `write.distribution-mode='bogus'` refuses loud at CTAS (`write.distribution-mode 'bogus' is not supported — use none, hash, or range`). Consumers: `append.rs:272`, `distribution.rs:169`. Committed pins (`test_profiles1_table_properties.py`): bogus CTAS refuses, tiny-target MERGE rewrite out-files default 21 to 5 at 20k rows with row counts held at 20000. Bite: valid `hash` CTAS is accepted, and two default tables land 5 and 5, so both pins fail without the property. No residue row: neither key is unread-anywhere. Scoping truth recorded in the row: plain `INSERT INTO` travels the fork provider, which does not consult these properties. |
| C-005 | D-3: the summary counts agree with the table. | count re-derivation from the green output | **PROVEN** | Green output holds the same 20 keys with identical subjects, read-backs, runtime and validation outcomes as the base output (compared key by key modulo the work prefix). Table: PASSES THROUGH 13 (11 + the two `write.*`), VALIDATED 3, ACCEPTED BUT UNREAD 4 (CONF-UNREAD-1's), REFUSED 0, NOT MEASURED 0 = 20; §5 carries exactly these numbers. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Both pins fail on the base tree (`2 failed in 6.95s`):

```text
test_probe_runs_twice_with_identical_output:
E  repark.errors.AnalysisException: [PATH_ALREADY_EXISTS] Path
E  /tmp/oc-rf8/docs/perf/profiles-1-probe/data/out_default already exists.
E  Set mode as "overwrite" to overwrite the existing path.
test_probe_ignores_a_poisoned_discovered_config:
E  repark.errors.IllegalArgumentException: repark config error: invalid
E  DataFusion session config 'datafusion.execution.batch_size' = 'notanumber':
E  Error parsing 'notanumber' as usize
```

The first is Q-21 reproduced (the second run writes into the first run's
`data/`); the second is Q-56 reproduced (a discovered `repark.toml` reaches
session build) and doubles as the poison design proof: the fixed probe sets
`REPARK_CONFIG=""`, so the same poisoned run must exit 0.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-8
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card decision maps to one clause with its named pin or measurement; the double-run pin holds D-1 and the poison pin holds D-1a on the same runs.
      artifacts: [task/ledgers/staging/review-fix-8-ledger.md, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-2
      status: ATTACKED
      evidence: The adversarial inputs are a fixed-path second run (PATH_ALREADY_EXISTS) and a poisoned discovered file (invalid DF value); both fail red on the base tree and pass green after.
      artifacts: [task/ledgers/staging/review-fix-8-ledger.md, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-3
      status: ATTACKED
      evidence: No refusal behavior changed: the green output refuses the same nine validation probes as the base output, and the engine's own invalid-value refusals are quoted, not reworded.
      artifacts: [docs/perf/profiles-1-passthrough-probe-2026-09-09.md]
    - id: AT-4
      status: N/A
      justification: No concurrency, locks, or shared state; one probe process per run, tempdir per run.
    - id: AT-5
      status: N/A
      justification: No refusal behavior changed; the probe records refusals, the engine keeps them.
    - id: AT-6
      status: N/A
      justification: No format, schema, or default changed; output gains no field, plans lose only their volatile tmp prefix.
    - id: AT-7
      status: N/A
      justification: No hot loop or resource touch; one bounded probe run per pin, scratch data under the system temp root.
    - id: AT-8
      status: ATTACKED
      evidence: Existing seams only (tempfile, REPARK_CONFIG empty value, plan string scrub); no new mechanism, no engine change, no dependency change.
      artifacts: [docs/perf/profiles-1-probe/profiles1_probe.py, docs/perf/profiles-1-probe/profiles1_probe2.py, python/repark/tests/test_profiles1_probe_rerun.py]
    - id: AT-9
      status: N/A
      justification: No error message changed.
    - id: AT-10
      status: N/A
      justification: Probe branches are unchanged in kind; the pin exercises the full twenty-key run twice.
  complete: true
```
