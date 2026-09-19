# Unit ledger — ICE-DROP-NS-1 · `DROP NAMESPACE` on a non-empty namespace refuses like Spark

**Date:** 2026-09-19 · **Branch:** `fix/ice-drop-ns-1` · **Base:** `0e3a899f` (`main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18: 1:1 parity with Spark's Iceberg integration. On main,
dropping a non-empty namespace succeeds, the namespace vanishes, and its tables become
unreadable — a silent data-loss shape. Spark refuses every spelling with
`NamespaceNotEmptyException`. The fix shares one pre-drop emptiness check between both SQL
doors; CASCADE never cascades (Spark's Iceberg catalogs agree).

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`,
`.github/`, AWS credentials or envs, nested-namespace support, Python reachability of the
native router.

## PROPOSITION LEDGER — ICE-DROP-NS-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 26-cell Spark 4.1.2 oracle is committed verbatim with provenance and SHA-256, and the recorder re-derives every cell on live Spark (`record`/`check`). | Fixture file plus map.md plus the live leg green. | PROVEN | SHA-256 `9f83aac0…7985b9` equals the measured recording; `check` normalizes only the volatile call-site counter and secs. |
| C-002 | The facade door refuses a non-empty drop on all seven spellings over both catalogs, and the namespace still lists and the table still reads `[[1]]`. | `test_ice_drop_ns_1.py` refusal pins, green after step 2. | PROVEN | Red on the base tree (`DID NOT RAISE` — the silent drop); green on the fix. The asserted core is Spark's `Namespace <ns> is not empty.` substring, so both catalog spellings are covered. |
| C-003 | The facade door drops an empty namespace (plain and CASCADE) and a namespace emptied by an explicit table drop. | `test_ice_drop_ns_1.py` drop pins, green before and after. | PROVEN | Control cells: they pass on main and stay green, so the fix refuses only the non-empty shape. |
| C-004 | The facade door answers `[SCHEMA_NOT_FOUND]` naming `catalog`.`namespace` for a missing namespace; `IF EXISTS` on a missing namespace stays a no-op. | `test_ice_drop_ns_1.py` missing pins plus the pre-existing idempotency test. | PROVEN | Red on main (`NamespaceNotFound => No such namespace`); green on the fix. |
| C-005 | Nested namespaces stay out of scope and the boundary is pinned: Spark's child-namespace refusal is recorded, RePark's nested `CREATE` refuses loud. | Oracle child cells plus the boundary pins, green before and after. | PROVEN | Spark `Contains 1 child namespace(s).` (InMemory; Hadoop prints the bare text) is in the fixture; RePark `CREATE NAMESPACE n.c` refuses with the two-part message and is pinned, not changed. |
| C-006 | The native door (`DROP SCHEMA`) refuses a non-empty schema with the same text and keeps everything; an empty schema drops (round 1 kept a blanket `CASCADE` refusal ahead of the check; C-011 removes it). | Rust ANSI-door pins, green after step 2. | PROVEN | Pinned in `repark-sql`, where the door is reachable. The Python `repark.sql` callable plans catalog DDL through plain DataFusion and never reaches the native router (measured: `CREATE`/`DROP SCHEMA` there touch DF schemas only, the Iceberg namespace survives both) — so no Python spelling can pin this door; wiring `AnsiDialect` into Python is a separate product decision. |
| C-007 | One helper (`repark-iceberg::catalog::refuse_non_empty_namespace_drop`) lists tables, then child namespaces, and refuses before any catalog call; both doors call it; CASCADE and `IF EXISTS` do not bypass it. | Helper unit tests plus both doors' pins. | PROVEN | `repark-iceberg` owns the catalog plumbing both doors already call, so no crate-DAG edge was added. Facade checks existence first (SCHEMA_NOT_FOUND before the helper); native keeps its CASCADE refusal first. |
| C-008 | Rust pins cover the helper (refusal, empty, dropped-first, missing) and both doors (every facade spelling, native refusal and dropped-first). | Crate test targets green. | PROVEN | 4 helper + 4 facade + 2 native tests; full lib suites `repark-iceberg` 538, `repark-sql` 358, `repark-spark` 1191 pass. |
| C-009 | `ICE-DROP-NS-1` reads **FIXED 2026-09-19** with before/after, pins, the nested-namespace boundary, and the exception-class residual. | Registry diff plus this ledger. | PROVEN | Row sits near the namespace DDL rows (§2.4) with the measured Spark texts on both catalogs. |
| C-010 | The targeted gates are green and no existing test assumed a non-empty drop succeeds. | Pasted summary lines in §Gates. | PROVEN | The sweep found one candidate (`test_ice_write_options_1.py` V-03 arm drops a table-holding namespace): it stays green unchanged because the write-options refusal runs ahead of the router. Full lib suites green; no test needed editing. |
| C-011 | (verification critic, 2026-09-19) Both doors share one Spark `[SCHEMA_NOT_FOUND]` text; the native door checks every spelling (its blanket `CASCADE` refusal is gone, so an empty schema drops under `CASCADE` as on Spark); the helper's child-namespace arm and the native `IF EXISTS` path are pinned. | `schema_not_found_on_drop` in `crates/repark-iceberg/src/catalog/mod.rs`; `drop_schema_if_exists_and_cascade_still_refuse_a_nonempty_schema`, `drop_schema_missing_and_empty_cascade_answer_like_spark`, `refuse_non_empty_namespace_drop_refuses_a_namespace_holding_a_child_namespace`. | PROVEN | Critic F-001/F-002 (P2): the native `IF EXISTS` mutation and the removed child arm left every test green; both now red on mutation (the IF-EXISTS bypass measured red by the orchestrator). F-003 (P3): the full Spark text on both doors. F-004 (P3): views registered as residue (3). |

## Gates

Measured on the release native built from the step-2 tree:

- Offline `-n 4`: `test_ice_drop_ns_1.py` 24 passed, 1 skipped (live-only); every file
  `rg -l "DROP (NAMESPACE|DATABASE|SCHEMA)" python/repark/tests` finds green unchanged.
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2): `test_ice_drop_ns_1.py` green, recorder
  `check` reproduces the fixture (rc 0).
- `cargo test -p repark-iceberg --lib` 538 passed; `cargo test -p repark-sql --lib` 358
  passed; `cargo test -p repark-spark --lib` 1191 passed, 5 ignored (pre-existing).
- `cargo fmt --all --check` clean; canonical clippy plus the panic-ban invocation clean on
  the three crates; `check_rust_file_size.py` clean (the `tests.rs` baseline is untouched —
  the native pins live in `schema_ddl/tests.rs`).
- `.venv/bin/ruff check .` clean; `ruff format --check` on the changed Python clean.
- `check_ledger_grammar.py`, `check_docs_links.py`, `sync_map_md.py --check` clean.
- Comment ban: 0 hits (no comment added to any source file).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-drop-ns-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every refusal expectation reads its core from the committed Spark 4.1.2
        oracle, and the live leg re-derives the fixture (rc 0). No RePark-only answer was
        written into a pin; the count text is asserted only where the memory catalog emits it.
      artifacts: [python/repark/tests/ice_drop_ns_1_spark_oracle.json, python/repark/tests/test_ice_drop_ns_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the base tree (16 failed with DID NOT RAISE or the old message,
        recorded in step 1); the 8 control pins passed before and after, which shows the
        refusal pins discriminate the fix rather than the harness.
      artifacts: [python/repark/tests/test_ice_drop_ns_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: All seven facade spellings pinned per catalog with Arrow-path survival
        (lists plus SELECT ids); the ANSI door pinned in Rust where it is reachable; the
        helper pinned directly including the dropped-first shape.
      artifacts: [python/repark/tests/test_ice_drop_ns_1.py, crates/repark-spark/src/tests/namespace_ddl.rs, crates/repark-sql/src/schema_ddl/tests.rs, crates/repark-iceberg/src/catalog/tests/namespace_drop.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The check lists live catalog state on every drop; nothing is cached. The
        facade existence probe runs before the helper, so IF EXISTS keeps its no-op contract
        and missing stays SCHEMA_NOT_FOUND rather than a listing error.
      artifacts: [crates/repark-spark/src/namespace_ddl.rs, crates/repark-sql/src/schema_ddl.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The refusal interpolates only the already-resolved namespace name into a
        fixed message; no user text reaches a sink. No new panic paths: the helper
        propagates listing failures and the doors keep their error folds.
      artifacts: [crates/repark-iceberg/src/catalog/mod.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Residues are named in the registry row rather than hidden (exception class,
        nested namespaces). The Python repark.sql routing limit is stated in the test module
        and here, with the measured evidence.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_drop_ns_1.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock or performance claim; the check adds two catalog listings to a DDL path.
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md, Cargo.toml or Cargo.lock edit, no crate-DAG change, no Python
        product change. Size ceilings hold; the exact-baseline tests.rs file is untouched.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_crate_dag.py]
    - id: AT-9
      status: ATTACKED
      evidence: The registry row reads FIXED with before/after and residues. Every touched
        directory's map.md carries the change with pins, and check_docs_links, sync_map_md
        and check_ledger_grammar are clean.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Full lib suites of the three touched crates pass; the write-options V-03
        arm (the one existing non-empty-drop statement) stays green unchanged; the facade
        idempotency tests and the parser production tests pass.
      artifacts: [python/repark/tests/test_ice_write_options_1.py, crates/repark-sql/tests/parser_productions.rs]
  complete: true
```
