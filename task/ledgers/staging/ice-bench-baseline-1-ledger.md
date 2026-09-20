# Unit ledger — ICE-BENCH-BASELINE-1 · the read bench's own "before" switch (round 1)

**Date:** 2026-09-19 · **Branch:** `perf/ice-bench-baseline-1` · **Base:** `22b586c7` (`origin/main`)
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The AWS "before" of the v1.5.0 read-performance campaign was never measured, and
the dispatch-only workflow `.github/workflows/aws-acceptance.yml` (job `ice-read-perf-bench`)
runs only from main. So one head must record both halves: a normal run (the "after") and a
BASELINE run that switches off what the campaign added and can be switched off — page-index
row selection and every shared cache.

**What it is.** No product behaviour changes. Four bench-only commits plus ledger/maps/docs:
`RunOptions.baseline` with a bare `--baseline` run flag; every run session built with the
metadata cache off, both cache byte budgets at zero, and the fork's
`iceberg.row_selection_enabled=false` inserted into the live session state's config extensions
from the bench; `baseline` / `baseline_switches` in the report JSON and the baseline in the
markdown header line; a boolean `baseline` dispatch input threaded through the bench job's two
run loops and its summary line. No new crate, no Cargo feature, no product Cargo.toml edit:
`iceberg-datafusion` is already a regular dependency of `repark-spark`, which the bench target
shares, so no dev-dependency was needed and no stop was required.

**Verified, not worked around.** All three catalog paths the bench registers build their
`CatalogCaches` from the session's settings — the memory catalog through
`memory_catalog_cached` with the session-registry caches, Glue and S3 Tables through
`register_catalog_spec` (hence `register_late_configured_catalogs`) with the same set — so the
builder config reaches the warm session, every fresh cold session, and the concurrent and
concurrent-cold sessions, local and remote alike, with no bench-side disabled-cache path and no
product-code change. `setup` sessions keep the defaults (`--baseline` is a run-only flag; setup
rejects it).

**What the baseline CANNOT switch off.** The timestamp predicate pushdown (fork #312) and the
`count(*)` fold (ICE-COUNT-FOLD-1) stay on in a baseline run. A baseline number is therefore
the campaign's two measurable improvements switched off — it is NOT "pre-campaign main", and
must never be read as one.

**Not in this unit:** running the AWS leg (dispatch-only, owner-approved; no AWS call was
made); the 200-file baseline numbers (the orchestrator records them);
`STATUS.md`.

## PROPOSITION LEDGER — ICE-BENCH-BASELINE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `RunOptions` gains `pub baseline: bool`, false in `RunOptions::local`; `run` accepts a bare `--baseline` flag; `USAGE` names it; `setup` refuses it and `--baseline <value>` is refused. | `pins_report::the_parser_accepts_a_bare_baseline_flag_and_defaults_it_off`. | PROVEN | Green on `1dfd54b7`. Mutation "drop the `--baseline` match arm in `parse_run`" reds it (2026-09-19). `pins.rs` stays at its 1000-line ceiling: the `r3` import folds to two lines (net −1) and the Q3 parser expectation carries `baseline: false`. |
| C-002 | With `baseline` true, every session a run creates is built with `repark.iceberg.metadataCache=false`, `manifestCacheBytes=0`, `footerCacheBytes=0` (the exported constants, through `ReparkSession::builder().config` in `bed::spark_session`) and a fork `IcebergScanOptions { row_selection_enabled: false, ..Default::default() }` inserted into the live session state's config extensions (`state_ref().write().config_mut().options_mut().extensions.insert`, the call that compiles on DataFusion 54.1). | `bed::spark_session(baseline)` (the one builder `open_session` takes, hence warm, cold, concurrent, concurrent-cold, local and remote); the three binding pins of C-005. | PROVEN | Green on `964d30b7`. Mutation "`open_session` ignores the flag" reds all three C-005 session pins (2026-09-19). |
| C-003 | The report JSON header carries `"baseline": <bool>` and, when true, `"baseline_switches": {"row_selection_enabled": false, "metadata_cache": false, "manifest_cache_bytes": 0, "footer_cache_bytes": 0}` (null when false); the markdown header line names `mode=` and `baseline=`. | The header asserts inside `a_warm_baseline_run_reads_every_footer_on_its_second_sample`; `the_human_table_names_the_baseline_in_its_header_line`. | PROVEN | Green on `001ad6d9`. Mutations " `baseline_switches` always null" and "drop the header `writeln!`" each red their pin (2026-09-19). The verification critic's V-001 (P2) found the human header took its flag from `options.baseline` while the JSON took it from the same field separately, so a mutation of the argument left both pins green; the call site now reads `document["baseline"]` and `document["mode"]`, so the header has no source of its own to diverge from and the JSON pin binds both (2026-09-19). |
| C-004 | The `ice-read-perf-bench` job gains a boolean dispatch input `baseline` (default false, "run with page selection off and no shared caches — the before half of a pair on one head"), passed through `env: BASELINE` (never `${{ }}` inside `run:`), appending `--baseline` to all eight `run --mode` invocations only when true, with `baseline=<value>` in the purpose summary line. | `test_ice_read_perf_bench_workflow.py`: `test_the_dispatch_offers_a_baseline_before_half_input`, `test_the_bench_run_steps_thread_the_baseline_flag_through_env`, `test_the_step_summary_names_the_baseline_beside_the_purpose`. | PROVEN | 14 passed on `c289f527` (11 pre-existing + 3 new). Mutations "empty the flag assignment / flatten the summary value" red the threading and summary pins, "default true" reds the input pin (2026-09-19). Empty `"${baseline_flags[@]}"` expands to zero arguments under `set -u` (measured on bash 5.2.21); `scripts/check_workflows_parse.py` parses all 13 workflows. |
| C-005 | Binding pins, each red-first: warm Q1 with `--baseline` reads every footer on its second sample (footer requests == file count, default 0); the same run shows zero metadata-cache hits (all four counters zero on every sample of every query); Q7 reads strictly more page bytes with page selection off at equal rows; the JSON header and the table header line carry the baseline. | `a_warm_baseline_run_reads_every_footer_on_its_second_sample`, `a_warm_baseline_run_records_no_metadata_cache_hits`, `page_selection_off_reads_more_page_bytes_on_a_selective_id_range`, `the_human_table_names_the_baseline_in_its_header_line`. | PROVEN | Green on `001ad6d9`; `cargo test -p repark-spark --test ice_read_perf_pins` — 26 passed. The 3-file × 400-row bed carries one page per column (manifest page census), so no query on it can differ by pages; the page pin grows the bed to 3 × 50,000 rows (3 pages per id/value column, 10 for payload). Measured warm Q7 (1,500 rows both ways): 92,890 page bytes by default, 161,729 with page selection off. Q1 second sample: 3 footer requests baseline vs 0 default (footer cache null vs 3 hits). |

## Measured observations (not clauses)

- Baseline warm Q7 also re-reads footers (26 requests / 13,631,488 B on 26 CPUs: the fork's scan re-pack splits the file across tasks and, with no footer cache, each task refetches the tail) while the default reads none; metadata cache 0/0/0 vs 1/0/0. Rows identical (1,500).
- The brief's test command `cargo test -p repark-spark --bench ice_read_perf_pins` names no such target: the manifest declares `[[test]] name = "ice_read_perf_pins"` (the `[[bench]]` is `ice_read_perf`, `harness = false`). The command run everywhere here is `cargo test -p repark-spark --test ice_read_perf_pins`.

## Gates

On `001ad6d9` / `c289f527`, `CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=6`:

- `cargo test -p repark-spark --test ice_read_perf_pins` — 26 passed.
- `python -m pytest python/repark-parity/tests/test_ice_read_perf_bench_workflow.py` — 14 passed.
- `uvx ruff@0.15.22 check` / `format --check` on the changed workflow pin file — clean.
- `cargo clippy --locked -p repark-spark --benches -- -D warnings -A clippy::disallowed_methods`
  (the `make rust-clippy` form, scoped) — clean. The brief's literal form without the `-A`
  reds on 3,622 pre-existing test-code `unwrap`s across the crate; per `clippy.toml` and the
  Makefile the disallowed list is live only in `rust-panic-ban` (`--lib --bins`), and this unit
  touches no lib/bin code.
- `cargo fmt --all` — clean (one reflow applied to the new parser pin before its commit).
- `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/qa-bs origin/main` — hits=0.
- Pre-commit hook gates observed green on every commit: map-sync, crate-dag, lib-rs, rust-file-size (704 files; `pins.rs` held at its ceiling), lib-py, docstring-presence, docs-compaction, manifest; `scripts/check_workflows_parse.py` — 13 workflows parse.

## Attestation

```
COVERAGE_ATTESTATION:
  pr_unit: ice-bench-baseline-1
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-visible answer changes. The switch lives entirely in the read bench; without --baseline every default is exactly main's.
    - id: AT-2
      status: ATTACKED
      evidence: Every pin was red under the mutation that removes its behaviour — the IcebergScanOptions insertion, the three cache configs, a parsing-but-inert flag, the JSON header field, the workflow env threading, the markdown header.
      artifacts: [crates/repark-spark/benches/ice_read_perf/pins_report.rs, python/repark-parity/tests/test_ice_read_perf_bench_workflow.py]
    - id: AT-3
      status: ATTACKED
      evidence: Warm and cold, local and remote catalogs, all four run modes through the single open_session funnel; the page pin grows the bed until pages exist so the comparison is not vacuous.
      artifacts: [crates/repark-spark/benches/ice_read_perf/pins_report.rs]
    - id: AT-4
      status: N/A
      justification: The bench runs its modes in one process under its own orchestration; the switch adds no shared state, only per-session configuration.
    - id: AT-5
      status: N/A
      justification: No credential, no catalog identity and no AWS call is involved; the baseline only removes caches and page selection.
    - id: AT-6
      status: ATTACKED
      evidence: The ledger records what the baseline cannot switch off — the timestamp-predicate pushdown of fork #312 and the count(*) fold — so a baseline number is never read as pre-campaign main.
      artifacts: [task/ledgers/staging/ice-bench-baseline-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: The measured page-byte pair at equal rows (92,890 by default against 161,729 with page selection off) and the footer pair (0 against one per file) are the unit's own numbers.
      artifacts: [crates/repark-spark/benches/ice_read_perf/pins_report.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, no Cargo feature and no product code changed; the dispatch input is documented in docs/tier2-aws.md and defaults to false.
      artifacts: [docs/tier2-aws.md, .github/workflows/aws-acceptance.yml]
    - id: AT-9
      status: ATTACKED
      evidence: The bench map, the parity-tests map and the staging map carry the change with pins citations.
      artifacts: [crates/repark-spark/benches/ice_read_perf/map.md, python/repark-parity/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The bench pin target and the workflow pin file both run green at the unit's head; CI runs the rest.
      artifacts: [crates/repark-spark/benches/ice_read_perf/pins_report.rs]
  complete: true
```
