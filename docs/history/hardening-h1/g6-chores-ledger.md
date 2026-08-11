# G-6 — hardening chores lane (four items, one PR)

> **ARCHIVED 2026-08-11** (G-9 — H-1 phase ledger promotion) — a historical record of everything
> delivered through the H-1 close gate (repark #35–#46), including the parallel G/N corpus units
> whose gap-map homes are H-2, kept for provenance and **not a source of live rules**: every rule
> still in force was verified live-elsewhere or promoted first
> ([promotion-ledger.md](promotion-ledger.md)). Relative links were repaired for this location on
> the same date; nothing else changed. Current state: [STATUS.md](../../../STATUS.md).

**Date:** 2026-08-10 · **Branch:** `grok/g6-chores` · **Worktree:** `/tmp/grok-g6` ·
**Charter:** `planning/grok/BRIEF-g6-chores-lane.md` (owner-approved; states 1–2 satisfied) ·
**Path:** STANDARD · **critic_engine:** `acc` · **Follow-up:** critic-overload validation.

Standing rule applied: verify-current-state-first — every item grepped before scoping; none were MOOT.

---

## Item 1 — N8: parity runner markdown report default

**Status:** DONE

**Problem:** `scripts/run_census.sh` defaults markdown to gitignored `target/census-reports/`
(C-2); `python/repark-parity/compat/runner.py` still defaulted to `task/`.

**Change:**
- Extracted `default_markdown_report_path(worktree, *, c4_expand, c3_expand, date_stamp)`.
- Default path is `{worktree}/target/census-reports/pyspark-compat-report[-c3-expand|-c4-expand2]-{date}.md`.
- `task/` only via explicit `--markdown`.

**Brief deviation note:** the brief said "OUTSIDE the repo tree (scratch)". Owner answer #2:
match the shell; C-2 as landed (`target/census-reports/`) is the policy SSOT. Brief wording
loses to the landed policy. Recorded here per owner instruction.

**Test:** `test_default_markdown_report_path_is_under_target_census_reports` in
`python/repark-parity/tests/test_compat_harness.py` — pins the resolved path only (no full census).

---

## Item 2 — four rustdoc intra-link warnings in `session.rs`

**Status:** DONE (comment-only; exempt test surface)

**Before** (`cargo doc -p repark-core --no-deps 2>&1 | grep session.rs`):

```
warning: public documentation for `config` links to private item `apply_datafusion_config_keys`
   --> crates/repark-core/src/session.rs:283:32
warning: public documentation for `memory_limit_gb` links to private item `MIN_MEMORY_LIMIT_BYTES`
   --> crates/repark-core/src/session.rs:303:47
warning: unresolved link to `list_iceberg_table_names`
   --> crates/repark-core/src/session.rs:985:18
warning: public documentation for `read_parquet` links to private item `object_store_s3`
    --> crates/repark-core/src/session.rs:1270:16
warning: `repark-core` (lib doc) generated 4 warnings
```

**Fix:** private helpers named in backticks (no `[link]`); public method linked as
`[Self::list_iceberg_table_names]`.

**After:** zero `session.rs` warnings from `cargo doc -p repark-core --no-deps`
(captured at unit gate).

---

## Item 3 — acceptance-harness location-mismatch fail-loud guard

**Status:** DONE (Glue-only live wire; pure comparison unit-tested without AWS)

**Probe result (bounded):** `DESCRIBE NAMESPACE` already yields a `Location` row when the
property is set (`python/repark/tests/test_describe_namespace.py` + engine
`execute_describe_namespace`). Facade `listDatabases` still returns `locationUri=None`.
**Live path wired through SQL DESCRIBE** — harness-local, zero engine change.

**Pure surface in `_acceptance.py`:**
- `normalize_location_uri` — trailing-slash strip only (S3 paths are case-sensitive).
- `assert_namespace_location_matches(actual=, expected=)` — exact equality after normalize;
  fail loud on mismatch (names both values + operator fix) and on `actual is None`
  (catalog-has-no-location).
- `location_from_describe_rows` / `probe_namespace_location_via_describe` /
  `assert_glue_scratch_namespace_location`.

**Glue leg:** after ensure-namespace in `test_process_silver_acceptance_against_glue`, call
`assert_glue_scratch_namespace_location(spark, GLUE_WAREHOUSE)`.

**S3 Tables leg:** skip with stated reason in code ("S3 Tables namespaces carry no location by
design — nothing to compare"); guard not called.

**Tests (AWS-free):** match, mismatch (message names both), no-location, DESCRIBE-row extract /
absent → None — in `test_acceptance_helpers.py`.

### Follow-ons (for owner to file — this unit does not file them)

1. **`spark.catalog.getDatabase` facade-parity API** — PySpark returns
   `Database(name, catalog, description, locationUri)`. Properly-verified unit with parity pins;
   also activates this guard's live leg on a public API (today: SQL DESCRIBE is the probe).
2. **Altitude-correct fix:** engine idempotent namespace-create fails loud when an existing
   namespace's location contradicts the requested one — catalog-layer / fork candidate. The
   harness guard is defence in depth on top of that, not the design.

---

## Item 4 — dual-wire checker

**Status:** DONE

**Script:** `scripts/check_parity_live_dual_wire.py` (+ `.sh` wrapper).
Compares `Makefile` `parity-live` ↔ `.github/workflows/parity-live.yml` to **each other**
(never a third hand-maintained list). Fail-closed on parse miss. Scope = this one pair
(one-line extensibility comment; no multi-pair framework).

**Wiring:**
- `make check-parity-live-dual-wire` — listed in `make help`
- in `make ci` chain
- ci.yml `guards` job step `parity-live dual-wire guard`
- named in AGENTS.md mechanical structure gates roster
- `scripts/map.md` updated

**Provocation proofs (verbatim; never committed as fixtures):**

### must-PASS (live tree)

```
$ python3 scripts/check_parity_live_dual_wire.py
parity-live dual-wire: OK (maturin@1.14.1, extras=['ml-ext', 'numpy', 'pandas', 'polars', 'record'], uv-run=['--locked', '--no-sync'])
exit:0
```

### must-FAIL — drift one flag on workflow side

Temporary edit: `uv run --locked --no-sync pytest` → `uv run --locked pytest` in
`parity-live.yml`, then restore.

```
ERROR: parity-live dual-wire parse incomplete on parity-live.yml: uv run missing flags ['--no-sync']
ERROR: parity-live dual-wire drift on uv-run-flags: Makefile parity-live=['--locked', '--no-sync']  parity-live.yml=['--locked']
parity-live dual-wire: FAIL — Makefile `parity-live` and .github/workflows/parity-live.yml disagree on load-bearing tokens (change one, change the other).
exit:1
```

### must-FAIL — drift one flag on Makefile side

Temporary edit: drop `--extra polars` from the `parity-live` recipe, then restore.

```
ERROR: parity-live dual-wire parse incomplete on Makefile parity-live: uv sync missing --extra ['polars']
ERROR: parity-live dual-wire drift on uv-sync-extras: Makefile parity-live=['ml-ext', 'numpy', 'pandas', 'record']  parity-live.yml=['ml-ext', 'numpy', 'pandas', 'polars', 'record']
parity-live dual-wire: FAIL — Makefile `parity-live` and .github/workflows/parity-live.yml disagree on load-bearing tokens (change one, change the other).
exit:1
```

---

## Gates (unit-wide)

| Gate | Result |
|---|---|
| `make ci` | **green** (includes dual-wire) |
| `make test` | **green** |
| `make py-test` (parity harness) | **green** — 147 passed |
| `make py-test-facade` | **green** — 2540 passed, 44 skipped |
| `make check-manifest` | **green** |
| `make check-parity-live-dual-wire` | **green** |
| `cargo doc -p repark-core --no-deps` | **zero `session.rs` warnings** (item 2 after) |

## ACC remediation (cycle 1)

Critic-1 (Quality) and Critic-2 (Security) both returned **NEEDS_REMEDIATION**. Remediations:

| ID | Sev | Disposition |
|---|---|---|
| Q-001 | S2 | **REMEDIATED** — `test_runner_main_wires_default_markdown_through_helper` (source pin that `main` calls helper; no raw `task/` default) |
| Q-002 | S2 | **REMEDIATED** — stub tests for `probe_*` + `assert_glue_scratch_*`; structural pin Glue calls guard / S3 Tables does not |
| Q-003 | S2 | **REMEDIATED** — `.github/workflows/map.md` updated (ci guards inventory + dual-wire is PR-visible) |
| Q-004 / SAF-003 | S2/Low | **REMEDIATED** — maturin version from recipe (`$(MATURIN)` → global pin; last inlined `uvx maturin@` wins) |
| SAF-001 | S1-ish | **REMEDIATED** — env from last `uv run … pytest` step only |
| SAF-002 | S1-ish | **REMEDIATED** — last-wins on load-bearing commands in document order |
| Q-005 | S3 | **ACCEPTED_FLAGGED** — REQUIRED_* floors are intentional fail-closed, not a third SSOT list for comparison |
| Q-006 | S3 | **ACCEPTED_FLAGGED** — residual module-level `crate::object_store_s3` link; method-level fixed; not in the four-warning greps |
| Q-007 | S3 | **REMEDIATED** — this gates table filled |

## Critic Overload

### Wave 1 CCC-α — NEEDS_REMEDIATION (S1 OPEN)

Merged OPEN ≥S1: W1-Q-001 (Glue pin import-only), W1-SEC-001/002/003 (last-wins decoy,
double recipe, unpinned floors), W1-L-001/002/003 (env value floors, trailing decoy,
pytest path floor).

**Actor remediations:** unique-match (not last-wins); absolute floors for
`REPARK_PARITY_LIVE=1`, `SPARK_LOCAL_IP=127.0.0.1`, `pytest path=python/repark/tests`;
last→unique `parity-live:` recipe + refuse `parity-live::`; unique `MATURIN :=`;
echo/printf/true/false/`&& echo` drop; trailing `#` strip; Makefile env must prefix
the uv-run-pytest invocation; Glue pin AST Call after create_namespace.

### Wave 3 CCC-β — NEEDS_REMEDIATION then remediated

Residuals W3-Q-001/002, W3-SEC-001/002/003, W3-L-001/002/003 attacked compound echo,
comment-out pin, multi-line decoy, `::` multi-fire, env-on-wrong-line. Closed by the
Wave-1→Wave-3 fix pass above (re-verified: dual-wire live OK; synthetic decoys red;
acceptance helpers 25 passed; `make ci` green).

### Residual ACCEPTED_FLAGGED (below reasonable shell-lexer scope / charter)

| ID class | Note |
|---|---|
| pytest `-k` / `--ignore` coordinated shrink | Path floor only; full arg-vector floor out of dual-wire chore scope |
| `env -u REPARK_PARITY_LIVE` wrappers | Hostile argv rewrite; would need full shell parse |
| Automated must-FAIL fixtures committed as pytest | Brief forbids committed provocation fixtures; synthetic self-checks run in session + ledger |

### Convergence

**ACC-CONVERGED** after remediation cycles.  
**Critic Overload:** waves 1 + 3 findings triads + Actor remediations; waves 2/4/5 folded into
Actor remediation (find→fix, not pure double-Actor). Label: **OVERLOAD-PARTIAL** (full five-wave
ceremony abbreviated after S1 queue closed and `make ci` green) with residual exotic greenwash
classes honestly flagged above.

### Follow-ons (owner files — not this unit)

1. `spark.catalog.getDatabase` facade-parity API (also activates location guard on public API).
2. Engine idempotent namespace-create fails loud on location contradiction (catalog/fork).
