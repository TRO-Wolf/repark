# Unit ledger — P2B: repark-spark skeleton (router spine + door seams)

**Unit:** phase-2 PR-2 · **Brief:**
[../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §1 "PR-2" · **Design:**
[../docs/design/sql-doors.md](../docs/design/sql-doors.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** MERGED 2026-08-07 (PR #9)

## Scope

Land the Spark door's spine and its two session seams in one PR:

- `crates/repark-spark` spine, copy-then-re-home from v1 `repark-sql`: `normalize.rs`,
  `spark_ast.rs`, the DESCRIBE/SHOW module, `metadata_tables.rs`, `time_travel.rs`, plus the
  `lib.rs` router (`execute` / `execute_with_read_only` / `execute_inner`). Small dependency
  modules may ride if the spine needs them to compile — each declared below.
- `SparkDialect`: `repark_core::SqlDialect` impl adapting
  `execute(EngineContext<'_>, &str)` to the ported
  `execute_with_read_only(ctx, catalogs, query, read_only)` positionally (the seam mirrors
  v1's exact argument set — seam-adaptation edit class).
- `SparkExtension`: `repark_core::SessionExtension` impl re-homing v1 `build()`'s inline
  registrations — `configure` = the cardinality/`repark.sql.*` `ConfigExtension` (r24 SB1);
  `register` = `repark_functions::register_all` + the analyzer-rules loop.
- DF-54.1 subquery-guard placement (design G8) discharged: the guard is a **core session
  default** (it re-homed inside `build()` verbatim at phase-1 PR-C, so no code moves in this
  PR); PR-2 adds the missing proof — the NEW bare-Session pin
  `bare_session_without_extension_carries_df_54_1_subquery_guard` in
  `crates/repark-core/src/session/tests.rs` — and the G8 comment note at the guard site.
  `SparkExtension` deliberately does NOT set the flag.
- Deferred test #1 (`temp_view_then_sql_runs_the_spark_function_shim`) lands as
  `crates/repark-spark/tests/session_extension.rs` against the real
  `Session + SparkExtension + SparkDialect`; its manifest row closes
  ([port/deferred-tests.md](port/deferred-tests.md)).
- Module test batteries ride WITH their modules (metadata_tables 15, time_travel 10,
  spark_ast 6, normalize/describe in-module sets) + one refuse test per temporary refuse arm
  + the two NEW seam tests above.

Out of scope: the handler modules (PR-3a/3b), `repark-ta` (PR-4), the ANSI door (PR-5/6),
the v1 lib-root 200-test router battery (rides PR-3b when the router completes — census note
below), carve-out files.

## Edit classes (declared, bounded)

1. **Verbatim copy** — the spine modules from the pin; bodies byte-faithful.
2. **Prefix renames (mechanical)** — crate `repark_sql` → `repark_spark`;
   `repark_write::`/`repark_catalog::` → `repark_iceberg::write::`/`repark_iceberg::catalog::`;
   `repark_core::` → `repark_common::`; `repark_session::` → `repark_core::`.
3. **TEMPORARY refuse arms** — router match arms whose handlers land in PR-3a/3b (ctas,
   create_table, alter, merge, insert_overwrite, ref_ddl, call, namespace_ddl, local_fs_ddl,
   drop handlers, postgres P11 message sites if any) are loud `NotImplemented` refusals naming
   the construct + "lands in phase-2 PR-3a/3b"; ONE refuse test each rides this PR. Each arm
   is TEMPORARY, restored by the named PR (WS1 enumerates the arms here as they land).
4. **Seam adaptation** — `SparkDialect::execute` destructures `EngineContext` into v1's three
   positional arguments; no behavior change.
5. **Extension re-home** — v1 `build()` inline registrations → `SparkExtension` hooks at the
   same construction positions (`crates/repark-spark/src/extension.rs`).
6. **Shared `cfg(test)` tracing harness** (phase-1 class 6) — only if ported module tests
   collide on global subscriber installs; reuse the repark-iceberg pattern.

No other edit class is authorized; anything else is a STOP.

## Riders (declared temporary omissions / deviations)

1. ~~**TA registration OMITTED from `SparkExtension.register`**~~ — v1 `build()` also ran
   `repark_ta::udf::register_all`; the `repark-ta` crate lands PR-4, where `SparkExtension`
   composes `TaExtension`. TEMPORARY, restored in PR-4 (brief §1).
   **DISCHARGED phase-2 PR-4 (2026-08-08):** `SparkExtension.register` now calls
   `repark_ta::TaExtension.register(ctx)` as its last step — v1 `build()`'s exact order
   (function registry → analyzer rules → TA UDFs, `v1-pin/crates/repark-session/src/lib.rs:320-329`).
   The door **composes** the owning crate's extension rather than calling `udf::register_all`
   itself, per design Q11 (the TA set is door-neutral). Pinned by
   `repark_spark::extension::tests::register_composes_the_ta_extension_window_udfs` (bit-exact)
   and the 7 ported `ta_window::sql_route_*` rows. Ledger:
   [p2e-ta-ledger.md](p2e-ta-ledger.md).
2. **Write-knob split (conformance note, not an omission)** — v1 `build()`'s engine write
   knobs (`with_merge_session_knobs`, scan/write concurrency) re-homed into the phase-1 core
   `build()` itself (PR-C), so `SparkExtension.configure` carries only the
   cardinality/`repark.sql.*` install — exactly what the core `extension.rs` seam doc
   assigns to the hook. Nothing is double-installed.
3. **NEW seam constructor `EngineContext::new` (repark-core)** — `#[non_exhaustive]` forbids
   literal construction outside repark-core, which blocked door-crate tests (E0639 observed
   during assembly); added as the one sanctioned downstream constructor with its own pin
   (`engine_context_new_is_the_downstream_constructor`). Additive, outside the ported census;
   seam growth stays field-additions per the dialect doc.
4. **PR-1 rider #1 (doc-comment re-home) partially dischargeable here** — the
   `repark-sql::spark_ast` references in repark-functions maps can re-point to repark-spark
   once the spine merges; integrator's call whether it rides PR-2 or PR-3b.

## WS1 enumeration — refuse arms + partial riders (edit class 3, as landed)

**TEMPORARY refuse arms in `src/router.rs`** (each: loud `NotImplemented` naming construct +
restoring PR; one refuse test in `src/router/tests.rs`; restored verbatim-from-pin by that PR):

| Arm | Where | Restoring PR (module) |
|---|---|---|
| CTAS (`CreateTable` with query) | match arm | PR-3a (ctas) |
| column-def CREATE TABLE | match arm | PR-3a (create_table) |
| DROP TABLE | match arm | PR-3a (namespace_ddl) |
| DROP NAMESPACE\|DATABASE\|SCHEMA | match arm | PR-3a (namespace_ddl) |
| ALTER … (all forms — I6, I7, residual refusals) | pre-parse leading-`ALTER` sniff | PR-3a (alter) |
| CREATE NAMESPACE\|SCHEMA\|DATABASE | pre-parse sniff (bare form must not fall through to a DF in-memory schema) | PR-3a (namespace_ddl) |
| MERGE INTO (parseable arm + unparsable fallthrough — v1's "could not parse MERGE" `Plan` message replaced by the refuse) | match arm + fallthrough | PR-3b (merge) |
| INSERT OVERWRITE | match arm | PR-3b (insert_overwrite) |
| CALL | match arm | PR-3b (call) |
| CREATE/DROP/REPLACE BRANCH\|TAG (incl. explicit bare-`REPLACE BRANCH|TAG` sniff v1 parsed in `ref_ddl`) | pre-parse sniff + fallthrough arm | PR-3b (ref_ddl) |

**TEMPORARY omissions (WS1):**

5. **Write-to-branch sniff omitted** — v1's r25 T2 STOP at the top of
   `execute_with_read_only` calls `ref_ddl::sniff_write_to_branch`; restored verbatim with
   `ref_ddl` in PR-3b (interim: branch-suffixed write targets hit planning's "table not
   found"). Comment marker at the v1 call site.
6. **`catalog_ops.rs` PARTIAL rider** — spine subset only; `reject_path_escape_ident`,
   `sqlparser_err`, the r24 P7 `reregister*` family, and `namespace_schema_name` return with
   their PR-3a/3b consumers.
7. **`namespace_ddl.rs` PARTIAL rider** — `consume_word` only (describe_show dependency).
8. **`normalize.rs` rewrite bypass** — the ALTER token rewrites + merge star rewrite +
   GenericDialect ALTER switch removed with a TEMPORARY comment (unreachable behind the
   ALTER/MERGE refusals); restored with `alter` (PR-3a) / `merge` (PR-3b). Unconsumed CTAS
   helpers (`build_transform_field`, `property_value`, `build_partition_spec`) carry
   self-cleaning `#[expect(dead_code)]` until PR-3a.
9. **lib.rs re-export trim** — v1's `pub(crate) use` handler groups + `#[cfg(test)]`
   describe_show re-exports return with their consumers (lib-root battery, PR-3b).
10. **root Cargo.toml** — `regex` added to `[workspace.dependencies]` (carried from the v1
    root manifest; describe_show consumer); repark-functions/repark-spark internal entries.
11. **Cross-WS mechanical fixes** — WS1 applied two gate fixes inside WS2's files:
    `SparkDialect::default()` → `SparkDialect` in `tests/session_extension.rs` (clippy) and a
    cargo-fmt reflow in `src/extension/tests.rs`.

Edit-class 6 (shared tracing harness): NOT needed — no ported spine test installs a global
subscriber.

## Census obligation — PARTIAL (closes PR-3b)

The 342-name `repark_sql::` → `repark_spark::` empty sorted-diff is a **PR-3b acceptance**,
not a PR-2 one (the v1 lib-root tests module rides PR-3b with the completed router). PR-2's
obligation: every ported spine module carries its full v1 in-module battery under the prefix
map; NEW (outside the census): the refuse-arm tests, the two `SparkExtension` seam batteries
(`extension/tests.rs`), the G8 bare-Session pin, and the landed deferred-#1 name
(`session_extension::temp_view_then_sql_runs_the_spark_function_shim`).

## Gate results (integrator fills)

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | PASS | exit 0 at each commit; commit 1 additionally checked out detached (fmt + canonical clippy + panic-ban green); verify panel re-ran per-commit |
| `make preflight` (PR head) | PASS | exit 0 at PR head (zizmor: no findings, 7 workflows parse) |
| spine module batteries green (`cargo test -p repark-spark`, never `--all-features`) | PASS | 58 lib + 1 integration, 0 failed; counts match v1 pin batteries under the prefix map |
| `cargo test -p repark-core` (incl. the G8 pin) | PASS | 80 passed incl. `bare_session_without_extension_carries_df_54_1_subquery_guard` |
| refuse test per temporary arm | PASS | router/tests.rs: one refuse test per TEMPORARY arm (CTAS, CREATE, DROP table/ns spellings, ALTER, MERGE, INSERT OVERWRITE, CALL, ref-DDL) + TRUNCATE verbatim pin |
| deferred-#1 row closed in manifest | PASS | row removed from task/port/deferred-tests.md; test lands as `session_extension::temp_view_then_sql_runs_the_spark_function_shim` |
| forbidden-literal sweep (tree + `git log -p`) | PASS | 0 hits over full PR diff and all commit messages (13-term case-insensitive list) |
| map.md lockstep (`check_map_md.sh`) | PASS | exit 0 at PR head; map updates ride the code commits |

## Deviations / STOPs

- Verify-panel fix: 3 doc-comment sites in ported modules still named the v1 `repark-session`
  crate (`describe_show.rs` ×2, `spark_ast.rs` ×1) — edit-class 2 prefix rename applied to
  doc text (`repark_session::` → `repark_core::`); widens the doc-re-home rider beyond the
  repark-functions map references.

## Retrospective

*(filled at unit close, per SEPMO)*
