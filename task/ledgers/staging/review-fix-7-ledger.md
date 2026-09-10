# Unit ledger — REVIEW-FIX-7 step 1 · no TOML injection, no secret echo, ambient cloud catalog warns

**Unit:** REVIEW-FIX-7 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-7` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The review sweep's only security-shaped finding class (Q-18, Q-19) plus the
RF-3 ruling on Q-20 from
[review-fix-slate-2026-09-10.md](../../roadmap/mid-term/review-fix-slate-2026-09-10.md):
the Python mirror interpolated profile and source names raw into TOML headers, a TOML
parse failure echoed the offending source line into the facade error, and a
CWD-discovered file could reach AWS on ambient credentials with no disclosure. One
round, Python half tier M, Rust half tier I.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Not in this step:** CFG-2 named-source registration (the database-table refusal stays),
`CatalogKind::Rest` (the REST-catalog card owns that variant), any dependency file,
`STATUS.md`, `briefs/next-sequence.md`.

## PROPOSITION LEDGER — REVIEW-FIX-7 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: a profile, catalog or database-source name carrying `]`, `.`, a quote or a newline refuses at construction with `ValidationError`, and every rendered header segment is quoted with `_toml_text`, so `to_toml()` output reads back as the same tables. | `test_profile_name_with_header_break_refuses`, `test_catalog_name_with_header_break_refuses`, `test_source_name_with_newline_refuses`, `test_profile_name_with_quote_refuses`, `test_spaced_profile_name_renders_one_quoted_header` | **PROVEN** | Red first on the base tree: `Failed: DID NOT RAISE ValidationError` for the profile-break and source-newline pins, `assert '"analytics east"' in '[analytics east.conf]...'` for the quoting pin (`4 failed, 13 passed`). Green after: `17 passed` (`.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q`). The catalog-break pin was already refused on base via the older dot-only rule and stays green as the generalized regression. |
| C-002 | D-2: a TOML parse failure carries `error.message()` and the line/column position only, never the source snippet the `toml` crate's `Display` prints, so a file whose second line is `aws_access_key_id = AKIA_PROBE` cannot echo the key into the facade error. | `a_parse_error_carries_position_but_never_the_source_line` | **PROVEN** | Red first: base `Display` measured as `TOML parse error at line 2, column 21` plus the verbatim `aws_access_key_id = AKIA_PROBE` line, so the pin FAILED on base; green after (`cargo test -p repark-core --lib config_file`: `63 passed; 0 failed`). The pin asserts the message contains `line 2` and contains neither `AKIA_PROBE` nor the source line. The Python mirror never parses TOML (typed construction only), so this pin lives engine-side; the facade surfaces the engine message unchanged. |
| C-003 | D-3 (RF-3): a catalog block whose `type` is `glue`, `s3tables` or `rest` in a file found by CWD or home discovery emits exactly ONE warning at session build naming the file path and the catalog name, while the same file named by `REPARK_CONFIG` or the `configFile` builder option, and a discovered file with only a local catalog, warn nothing. The session still builds. | `discovered_glue_catalog_warns_once_naming_path_and_catalog`, `home_discovered_glue_catalog_warns_once`, `repark_config_named_glue_catalog_warns_nothing`, `forced_glue_catalog_warns_nothing`, `discovered_memory_catalog_warns_nothing` | **PROVEN** | Red first: `error[E0609]: no field 'warnings' on type 'FileConfig'` (13 errors) on the base tree; green after in the same `63 passed` run. `FileConfig` carries `warnings`; `load_file_config` marks a load trusted when the `configFile` forced path or a non-empty `REPARK_CONFIG` named the file, and otherwise records one warning naming the path and the sorted cloud catalog names; `prepare_build_state` prints each warning at session build. `rest` is matched by the predicate but still refuses first through the pre-existing unrecognized-value error until `CatalogKind::Rest` arrives, so that arm goes live with the REST-catalog card and needs no further edit. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-7-step-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card decision maps to one clause with its named pins; the pins assert the decided behavior directly (refusal class, message contents, warning count and naming).
      artifacts: [task/ledgers/staging/review-fix-7-ledger.md, python/repark/tests/test_config_mirror.py, crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Adversarial header names exercised (header-break profile, quoted catalog, newline source, quoted profile) plus the boundary between refused and renderable (spaced profile renders quoted and reads back as one profile); malformed TOML and each discovery source (CWD, home, REPARK_CONFIG, forced, absent-cloud) exercised.
      artifacts: [python/repark/tests/test_config_mirror.py, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The invalid-TOML path refuses with a sanitized message (position kept, snippet dropped); the database-table CFG-2 refusal and the unrecognized-catalog-type refusal still fire unchanged through the refactored helpers.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-4
      status: N/A
      justification: No concurrency, locks, or shared state; the warning is a per-load Vec on a locally owned struct, and discovery inputs arrive as parameters.
    - id: AT-5
      status: ATTACKED
      evidence: This unit is the security surface: header injection is refused and quoted, parse errors no longer echo source lines, and ambient-credential cloud catalogs warn naming path and catalog.
      artifacts: [python/repark/src/repark/config.py, crates/repark-core/src/config_file.rs, crates/repark-core/src/config_file/wiring.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Quoted headers parse back to identical tables (existing round-trip pins green); the sanitized parse error keeps line and column; the warnings field defaults empty so undiscovered and trusted loads behave exactly as before.
      artifacts: [python/repark/tests/test_config_mirror.py, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-7
      status: N/A
      justification: No hot loop, growth, or resource touch; one bounded scan of the effective profile's catalog table per load.
    - id: AT-8
      status: ATTACKED
      evidence: The `toml::de::Error` contract is used as documented (`message()` plus `span()` mapped to line and column locally); no new dependency; the `ProfileSources` import is the crate's own type.
      artifacts: [crates/repark-core/src/config_file.rs, crates/repark-core/src/config_file/wiring.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The discovery warning prints at session build naming path and catalogs, and the parse error names line and column; both failure paths diagnose without leaking source text.
      artifacts: [crates/repark-core/src/session.rs, crates/repark-core/src/config_file.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every added branch has a naming input: each refused mark, the span-absent error shape, trusted versus discovered loads, and each cloud spelling. The `rest` arm is forward-compat (still refused first until CatalogKind::Rest arrives) and is disclosed in C-003 rather than left as silent handling.
      artifacts: [python/repark/tests/test_config_mirror.py, crates/repark-core/src/config_file/tests/wiring.rs]
  complete: true
```
