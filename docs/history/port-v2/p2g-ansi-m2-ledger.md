# Unit ledger — P2G: the ANSI door, milestone 2 (PR-6) — the door closes

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

**Unit:** phase-2 PR-6 · **Brief:**
[phase-2-sql-doors.md](phase-2-sql-doors.md) §1 "PR-6" · **Design:**
[docs/design/sql-doors.md](../../design/sql-doors.md) (LAW for this unit) · **Predecessor:**
[p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md) (PR-5 / M1) · **Status:** MERGED 2026-08-08 (PR #14; archived 2026-08-09)

This is the **single home** for PR-6's per-Q delivery record. `docs/design/sql-doors.md` is not
annotated with delivery notes — it stays the design, and points here (no duplication; the design
doc's §2 rulings and this ledger's "delivered" column are read together).

Two workstreams shared the worktree: **WS1** (handlers — ALTER, MERGE, time travel, branch/tag
DDL, the refuse set) and **WS2** (the repark-core R2 fix, the cross-door protocol, both matrices,
the seam freeze, the docs and this ledger). This ledger records WS2's half in first person and
cites WS1's landed surfaces by test name.

## The R2 core fix — the one repark-core change (design §2 Q8)

PR-5's R2 spike found the gap and filed it here. **Fixed:**
`crates/repark-core/src/session.rs::apply_datafusion_config_keys` +
`pub const DATAFUSION_CONFIG_PREFIX`.

Before: the builder's `.config(k, v)` map was repark/spark-shaped only — read by
`parse_catalog_specs`, the write/scan concurrency readers, the S3-region resolver and the
extension `configure` hook — and **nothing in it ever reached `SessionConfig`**. So
`datafusion.catalog.information_schema = true` was silently inert, and `SHOW TABLES` / `DESCRIBE` /
`information_schema.*` were dead in BOTH doors.

After: every `datafusion.`-prefixed key is applied to the `SessionConfig`, in sorted key order,
**after** the typed setters and the core defaults (so an explicit conf wins, including over the
G8 DF-54.1 subquery guard a user might knowingly re-enable) and **before** the extension
`configure` hook (so an extension still configures against final DataFusion options). An unknown
or unparsable key is an `Error::Config` naming the key — a silently-inert conf key is the exact
defect being removed, so the fix must not reintroduce it one typo over.

**This is config plumbing, not a seam change.** `SqlDialect` and `SessionExtension` are untouched;
both doors reach the behaviour as ordinary session configuration. The seam freeze (below) is
therefore unaffected.

Scope discipline: the prefix filter is deliberately narrow. `repark.*` / `spark.*` keys keep
their existing consumers and their PySpark-style tolerance of unknown keys — pinned by
`builder_non_datafusion_config_keys_stay_ignored`.

| Test (`repark-core`, `session::tests::…`) | What it pins |
|---|---|
| `builder_datafusion_config_key_reaches_session_config` | Two keys land in `SessionConfig` (the fix itself) |
| `builder_non_datafusion_config_keys_stay_ignored` | Unprefixed keys still tolerated, DataFusion options untouched |
| `builder_unknown_datafusion_config_key_fails_loud` | A misspelled `datafusion.*` key is `Error::Config` naming the key |
| `explicit_datafusion_config_overrides_a_core_default` | Application order: explicit conf beats the core default |
| `information_schema_enumerates_a_registered_iceberg_catalog_through_the_session` | Q8 core half: namespaces + tables enumerate, `SHOW TABLES` + `DESCRIBE` work, on the PRODUCT path |
| `show_tables_still_refuses_without_the_information_schema_conf` | The negative half — the delivery is attributable to the conf, not to a default |
| `information_schema_still_exposes_the_dollar_metadata_tables` | The open product question, pinned as current behaviour |

## Q8 INTROSPECTION — delivered, with one honest caveat

**Enumeration through the fork's providers WORKS.** The PR-5 spike had already proved the
machinery on a raw `SessionContext`; the only thing missing was the ability to turn the conf on,
and that is now fixed. Verified on the product path AND through the ANSI door:

- `information_schema.schemata` enumerates a registered Iceberg catalog's namespaces.
- `information_schema.tables` / `information_schema.columns` enumerate its tables and columns.
- Stock `SHOW TABLES` and `DESCRIBE t` plan and execute through `AnsiDialect` with **no door-side
  parser** — which is the entire content of the Q8 "delegate" ruling.

Door-side tests: `crates/repark-sql/tests/introspection.rs` —
`information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door`,
`show_tables_and_describe_delegate_through_the_ansi_door`,
`introspection_still_refuses_without_the_information_schema_conf`.

**The caveat, stated rather than buried:** the fork's `$`-suffixed metadata tables enumerate as
`BASE TABLE` alongside the real table. Trino hides these from `SHOW TABLES`; we do not, today.
This was the R2 spike's second finding ("product question, low severity") and it is **still
open** — whether `repark_iceberg::catalog`'s `SchemaProvider::table_names` should filter them is a
fork/core decision, not a door parser, and deciding it inside a door PR would be deciding it in
the wrong place. It is pinned as CURRENT behaviour in two tests
(`metadata_tables_currently_enumerate_alongside_the_real_table` and the repark-core twin) so that
filtering them later flips a test red on purpose. The `INTROSPECTION` matrix row is scoped to what
is proven: delegation works.

Also still deferred with triggers, unchanged from the design: Trino `SHOW SCHEMAS FROM` and
`SHOW CREATE TABLE` (design §2 Q8).

## Q11 TA toll — delivered

`crates/repark-sql/tests/ta_toll.rs`, on a **native** session (no Spark anything) with
`repark_ta::TaExtension` installed at the build hook:

- `ta_ema_through_the_ansi_door_is_bit_exact_against_the_golden` — `ta_ema(close, 21) OVER (ORDER
  BY ts)` through ANSI-door SQL, compared `f64::to_bits`-exactly against the recorded C TA-Lib
  0.4.0 golden (`crates/repark-ta/tests/goldens/ema_21.bin` over `fixture_close.bin`). No golden
  re-recorded; the comparison idiom is the goldens' own.
- `ta_non_literal_period_refuses_loud_through_the_ansi_door` — the design's named refuse row.
- `ta_is_absent_from_a_native_ansi_session_without_the_extension` — the other side of "opt in",
  without which the smoke row could be green for the wrong reason.

## Q13 / graft G5 — the two-session cross-door protocol

`crates/repark-sql/tests/cross_door.rs`. **Every row runs two sessions** — a native
(no-extension) session through `AnsiDialect` and a Spark-extended session through `SparkDialect`,
each over its OWN in-memory catalog, compared on the Arrow path (value AND type) — except the one
row that is explicitly recorded as single-session-legal.

| Row | Test | Session profile |
|---|---|---|
| CTAS (content + schema) | `cross_door_ctas_produces_the_same_table_content_and_schema` | TwoSession |
| INSERT round trip | `cross_door_insert_lands_the_same_rows` | TwoSession |
| ALTER (evolved schema + table rename) | `cross_door_alter_lands_the_same_evolved_schema` | TwoSession |
| MERGE (result table) | `cross_door_merge_produces_the_same_result_table` | TwoSession |
| Time travel (snapshot pin) | `cross_door_time_travel_pins_the_same_snapshot_content` | TwoSession |
| Identifier case folding (the Q10 doc row) | `cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted` | TwoSession |
| Namespace DDL — the single-session boundary | `cross_door_namespace_ddl_is_single_session_legal` | Native, single-session `sql_with` (recorded as such, NOT claimed as TwoSession) |
| The protocol's own guard rail | `extensions_are_session_scoped_not_dialect_scoped` | TwoSession |

Two results worth recording rather than smoothing over:

1. **Identifier case folding does NOT diverge between the doors.** The design anticipated a
   door-vs-door divergence row; what the test found is that unquoted identifiers agree AND quoted
   identifiers agree — both doors treat a quoted identifier as case-SENSITIVE, because both sit on
   stock DataFusion resolution. The real divergence is **repark-vs-Apache-Spark** (Spark resolves
   `` `ID` `` case-insensitively by default), inherited engine-wide rather than introduced by
   either door. Recorded, not fixed: changing it means changing the Spark door's resolution
   semantics against stock DataFusion, which is a decision, not a bug fix.
2. **The guard-rail row is load-bearing, not decoration.** `date_add` is absent on a native
   session and resolves on a Spark-extended session driven through `AnsiDialect` — the concrete
   demonstration that single-session cross-door rows are invalid.

Home + DAG: the file lives in `repark-sql`'s `tests/` with `repark-spark` and `repark-ta` as
**dev-dependencies**. `scripts/check_crate_dag.py` scopes layering to NORMAL edges (dev- and
build-dependencies excluded by design — the same carve-out `repark-spark`'s dev-dep on
`repark-common` already rides). Verified: `make check-crate-dag` → *11 internal edges clean across
7 of 7 mapped crates*. **No product edge exists; nothing in either door's `src/` names the other.**

## Seam freeze (design §3) + the ADR discharge

- `docs/design/session-api.md` — `SqlDialect::execute(EngineContext<'_>, &str)` and
  `SessionExtension` flip **UNSTABLE → FROZEN (2026-08-08)**, with a new "Seam freeze" subsection
  carrying the G5 line: **extensions are session-scoped, not dialect-scoped** — a Spark-extended
  session has Spark expression semantics through every door — plus its three consequences (two
  sessions for cross-door evidence; no cross-door-extension evidence in a door's own matrix;
  door-neutral extensions compose the same way). `EngineContext` stays `#[non_exhaustive]`, so
  adding a field remains non-breaking; changing or removing one now needs a superseding note.
- `docs/adr/0002-two-sql-doors.md` — decision 5's "one deliberate design pass before the first
  public commit of that surface" obligation is **discharged**, citing `docs/design/sql-doors.md`
  (and its provenance) as the artifact, plus the two `matrix.rs` files as the durable mechanism
  behind decision 4's "one test row per door". Deliberately NOT discharged: the
  maintenance-as-callable-ops pin, which keeps its trigger (Q7).

## Surface matrix (Q13) — the counts this PR lands

`repark_common::surfaces::ALL` = **43** capability IDs, unchanged (no ID added or retired).

| Door | Tested | DeliberatelyAbsent | Δ from PR-5 |
|---|---|---|---|
| `repark-spark` | 40 | 3 | `CROSS_DOOR_EQUIVALENCE` flipped |
| `repark-sql` (M2) | 39 | 4 | 10 rows flipped |

**The ten ANSI flips:** `INTROSPECTION` (Q8), `ALTER_TABLE_RENAME`,
`ALTER_TABLE_SCHEMA_EVOLUTION`, `ALTER_TABLE_PROPERTIES`, `MERGE`, `TIME_TRAVEL`,
`BRANCH_TAG_DDL`, `IDENTIFIER_CASE_FOLDING`, `TA_FUNCTIONS`, `CROSS_DOOR_EQUIVALENCE`.

**The four ANSI absences that remain are absent BY RULING, not by sequencing** — there are no `M2`
deferral rows left, and the `M2` const is deleted from the matrix so one cannot be written back
casually:

| ID | Ruling | Equivalent + trigger | Refuse pinned by |
|---|---|---|---|
| `ALTER_TABLE_PARTITION_FIELDS` | §2 Q3 — deferred from SQL entirely | callable `UpdatePartitionSpec`; trigger = dbt-repark or first user need; future spelling `SET PROPERTIES partitioning = ARRAY[…]` | `alter::tests::partitioning_refuses_citing_q3_and_names_the_callable_op` |
| `INSERT_OVERWRITE` | §2 Q9 — omitted, Trino-faithful (dbt-trino evidence, graft G10) | MERGE / DELETE+INSERT / `CREATE OR REPLACE TABLE … AS SELECT`; Spark door + callable op keep OV1 reachable | `refusals::tests::insert_overwrite_refusal_steers_three_ways_and_cites_the_evidence` |
| `TRUNCATE` | permanent targeted refuse (ANSI twin of C4-L-001) | `DELETE FROM t` / `CREATE OR REPLACE TABLE … AS SELECT`, both named in the message | `refusals::tests::truncate_refusal_names_both_meanings` |
| `MAINTENANCE_CALL` | §2 Q7 / ADR-0002 — callable ops only | the callable ops; future spelling `EXECUTE proc(arg => v)`; trigger = dbt-repark post-hooks + superseding ADR note FIRST | `refusals::tests::call_refusal_steers_to_callable_ops_and_names_the_trigger`, `refusals::tests::alter_execute_refusal_declares_itself_the_future_spelling` |

**The three Spark-door absences that remain:** `TABLE_OPTION_SORT_ORDER` and
`TABLE_OPTION_UNKNOWN_KEY_REFUSE` (no Spark spelling exists to guard — `TBLPROPERTIES` is a raw
map) and `WRONG_DOOR_SNIFF` (the sniff points AT this door).

**Q6 branch/tag DDL was NOT deferred.** Design §2 Q6 named it the first deferral candidate if PR-6
overran; it did not, so the row is `Tested` and the SCOPE-rationale contingency never fired.

Matrix audit changes (both doors), each of which is itself a mutation-checked test:

- ANSI: `ansi_rows_are_native_or_unit_only` → `ansi_rows_never_claim_a_spark_extended_session`.
  The ban that matters is `SparkExtended`; `TwoSession` is now legal here precisely because its
  ANSI half runs on a native session and its Spark half is the control. New:
  `two_session_rows_name_surfaces_both_doors_have` (a `TwoSession` claim on an ANSI-only surface
  describes a comparison that cannot exist). `m1_ships_the_briefed_scope` →
  `m2_closes_the_ansi_door`, now pinning the four absences by name rather than a count of flips.
- Spark: `no_row_claims_the_two_session_profile` →
  `only_the_cross_door_row_claims_the_two_session_profile` — the original invariant restated so it
  keeps working after the one legitimate claim exists.
  `spark_door_absences_are_the_five_declared_ones` → `…_three_declared_ones`.

The Spark door's `CROSS_DOOR_EQUIVALENCE` row cites a test in the OTHER crate's binary
(`repark-sql tests/cross_door.rs::…`), spelled with its crate so the reference is followable. That
is the honest place for it: the protocol needs both doors in one process, and only a
dev-dependency may cross the door boundary. Consequence recorded here so a reviewer is not
surprised: `cargo test -p repark-spark` alone does not execute that row's evidence.

## Deviations

1. **`repark-spark` and `repark-ta` are DEV-dependencies of `repark-sql`.** The design's §1 dep
   table lists neither. Both are test-only and DAG-legal (normal-edge scoping, verified); the
   cross-door protocol is unimplementable without the first and the Q11 toll without the second.
   Nothing in `src/` may name either — that is the invariant, not the dependency list.
2. **The `CROSS_DOOR_EQUIVALENCE` row's cited test name is crate-qualified**, breaking the
   matrices' "verbatim `cargo test -- --list` name" convention. The convention assumes the test
   lives in the door's own binary; for this one ID that is impossible by construction, so the row
   states where it actually lives rather than naming something unfindable.
3. **`ALTER_TABLE_RENAME` and `ALTER_TABLE_SCHEMA_EVOLUTION` cite the SAME cross-door test.** The
   surfaces are distinct and both are exercised in it (three column evolutions, then
   `RENAME TO` with an old-name-is-gone assertion); the evidence is shared, as it already is for
   the Spark door's `TABLE_OPTION_FORMAT` / `TABLE_OPTION_RAW_PROPERTIES` pair.

## Verify-panel findings (post-assembly fix pass)

Four independent verify panels re-ran every gate on the assembled branch and reproduced the
integrator's numbers. Three defects survived reproduction; all three are in the NEW ANSI door and
all three are fixed in `fix(pr-6): address verify-panel findings`, each with a regression pin that
was confirmed RED against the pre-fix source before it went green.

| # | Defect | Reproduction | Fix | Pin |
|---|---|---|---|---|
| 1 | `refusals::recognize_alter_table_execute` searched the WHOLE statement for the bare word `EXECUTE`, so `ALTER TABLE ice.sales.orders ADD COLUMN execute BIGINT` refused as "ALTER TABLE … EXECUTE BIGINT is not supported yet" — a legal schema-evolution statement rejected pre-parse, with the column's TYPE named as the "procedure" | probe on the recognizer; also `RENAME COLUMN a TO execute` | the test is ANCHORED to the verb slot — the word after the (dotted / quoted) table name — via `verb_slot_after_table_name`, which walks OFFSETS because a quoted name part contributes no word | `refusals::tests::alter_execute_recognizer_is_anchored_to_the_verb_slot` (6 legal statements) + `…_finds_the_verb_after_any_name_spelling` (6 name spellings) |
| 2 | `ref_ddl::reject_trailing` filtered the leftover tail through `Sig::ident`, so trailing NUMBER / punctuation tokens were SILENTLY DROPPED — `… DROP BRANCH audit 5` dropped the branch and `… CREATE BRANCH audit AS OF VERSION 7 99` created it — the exact "ignore it" the fn doc forbids | probe through `try_parse_ref_ddl` | `Sig::Other` now carries its source text, `reject_trailing` refuses on ANY leftover and NAMES it; a trailing `;` is stripped in `tokenize_significant` (one statement is still one statement) | `ref_ddl::tests::trailing_non_identifier_tokens_refuse_too` + `…::a_trailing_semicolon_is_not_a_trailing_clause` |
| 3 | `time_travel::register_pinned_view` registered `__repark_ansi_tt_<n>` and never deregistered it: one permanent relation per `FOR … AS OF` relation per query on a long-lived session, AND user-visible in `SHOW TABLES` / `information_schema.tables` — the very surface the R2 fix in this PR turns on. (The `deregister_table` on the old line 147 was dead: the name comes from a monotonic counter and can never pre-exist.) | 3 pinned reads on one session → `information_schema.tables` listed `__repark_ansi_tt_1|2|3` | a `PinnedViews` record threaded through the rewrite; `router::execute` splits the post-rewrite pipeline into `execute_time_travelled` so `pinned.release(cx.ctx)` runs on every `?` / `return` path (**corrected — see below**). Safe because planning resolves the relation into a `TableScan` that owns the provider — the returned `DataFrame` still collects | `tests/introspection.rs::time_travel_pinned_views_do_not_leak_into_the_introspection_surface` (asserts: no leftover row under EITHER prefix, and the pinned read still returns its row + Arrow type) |

> **Correction to finding 3, filed 2026-08-11 by unit H-1b (V2 Engine Hardening).** The Fix cell
> above was half-true twice over, and both halves are now fixed in code and restated here.
>
> 1. **"EVERY exit path" overstated the guarantee.** The release runs on every `?` / `return`
>    path — not on unwind or future-drop. `PinnedViews` deliberately carries no `Drop` impl (it
>    would have to own a `SessionContext` clone), and neither source exists today: panics are
>    banned in prod code and the PyO3 facade drives this via `block_on`. The claim is now worded
>    "every `?` / `return` path" here, in `crates/repark-sql/src/router.rs`, and in the Spark
>    door's copy of the same split. It is a scope statement, not a defect.
> 2. **The wrong half of the leak was fixed — the tracked name was released, the composed one was
>    not.** `register_pinned_view` builds its view over `repark_core::read_table_at`, which
>    registers a `__repark_tt_<n>` of its OWN before returning the frame. Only the
>    `__repark_ansi_tt_<n>` went into the ledger, so every `FOR … AS OF` relation still left one
>    untracked relation behind — on the very door this row declares fixed. The pin could not see
>    it: it filtered `LIKE '__repark_ansi_tt%'` only. Re-measured at H-1b, unchanged since this
>    row was written: three ANSI pinned reads → `ansi_leftover=[]`,
>    `tt_leftover=["__repark_tt_1","__repark_tt_2","__repark_tt_3"]`. H-1b records BOTH names in
>    the same ledger (inside `register_pinned_view`, so the reader-options caller of
>    `read_table_at` — whose registration must SURVIVE — is untouched by construction) and adds
>    the `'__repark_tt%'` half to the pin, captured RED before the fix. **Closed except for two
>    named residuals:** (a) a `read_table_at`-INTERNAL failure between its own `register_table`
>    and its `ctx.table` lookup leaves a name this side cannot observe — closing that needs
>    Option 2 of the re-port map (thread the ledger into `read_table_at`), whose blast radius
>    reaches the reader-options caller; and (b) if `core_pinned_name` ever answers `None` for a
>    frame that DID carry a core-minted pin (a changed plan shape or prefix upstream), the leak
>    returns silently — fenced by the pin's broadened `LIKE '__repark_tt%'` assertion, which reds
>    on the leftover rather than on the recovery.

Rejected panel items, with reasons:

- **"stale local `main`" (orchestration).** Correct observation, not a repo defect: this
  worktree's `main` ref is behind `origin/main`. Both hygiene passes were re-run against the
  stated base `e953bdf` explicitly rather than against `main`, and the superset scoping is also
  clean. Nothing to change in the branch; the integrator must not read `main..HEAD` counts here.
- **"a second agent is writing to this worktree live."** That was the verify panels' own
  throwaway probes (`crates/repark-sql/tests/zz_probe.rs`, transient edits to two test modules)
  appearing and being reverted while the panels ran concurrently. No source change.

## Riders carried forward

1. **OPEN (fork/core, low severity):** whether `repark_iceberg::catalog`'s
   `SchemaProvider::table_names` should filter the 16 `$`-suffixed metadata tables out of
   `SHOW TABLES` / `information_schema.tables`, as Trino does. Pinned as current behaviour in two
   tests so the decision cannot be made silently.
2. **Carried from PR-5, still an orchestrator item:** the matrix cannot verify that a cited test
   NAME still exists (a Rust test binary cannot enumerate its own tests). The check needs a
   harness-level gate — `cargo test -- --list` diffed against the matrices — living in the
   Makefile / `.github/`, which are orchestrator-only carve-outs. Recorded, not half-built.
3. **Case folding vs Apache Spark** (see Q13 result 1): a real divergence, currently inherited
   engine-wide. If it is ever to be fixed, it is a Spark-door resolution decision, not a matrix
   row.
4. **CLOSED 2026-08-11 by unit H-1b (V2 Engine Hardening).** ~~NEW, from the verify pass — the
   SPARK door has the same time-travel view leak.~~
   `crates/repark-spark/src/time_travel.rs` registered `__repark_tt_<n>` and never deregistered
   it, exactly as the ANSI door did (finding 3 above). It was PRE-EXISTING — phase-1 code, not
   PR-6's — but this PR is what made it visible, because the R2 fix lets ANY session enable
   `information_schema`. Left out of the fix commit deliberately: the Spark router's
   time-travel call sits inside a different pipeline shape, and re-plumbing it is a behavioural
   change to a door this PR was not scoped to touch. The ANSI-side fix is the template
   (`PinnedViews` + release after planning — **see the correction to finding 3 above**: that
   template was itself still leaking its composed core half when this rider named it, so it was a
   structural template only). Filed as the first rider of the next Spark-door unit.
   **How it closed:** H-1b re-ported the shape by meaning, not by patch (the `Cow` juggling
   differs — the Spark router borrows its `sql` parameter where the ANSI one consumes an owned
   rewrite): `PinnedViews` + record-before-register in `crates/repark-spark/src/time_travel.rs`,
   and an `execute_time_travelled` split in `crates/repark-spark/src/router.rs` releasing on
   every `?` / `return` path. Pins:
   `crates/repark-spark/src/tests/time_travel.rs::time_travel_temp_views_do_not_survive_a_successful_statement`
   and `…_a_failed_statement`, both re-run under two deliberate mutations (drop the release →
   both red; release only on `Ok` → the error-path pin alone reds, which is what earns it its
   place). The same unit corrected finding 3 above, both halves. Unit ledger:
   `task/h1b-ledger.md`.

## Gate table

| Gate | Command | Result |
|---|---|---|
| repark-core unit tests | `cargo test -p repark-core --lib` | 87 passed / 0 failed / 0 ignored (80 before; +7 R2/Q8) |
| Crate-DAG layering | `make check-crate-dag` | 11 internal edges clean across 7 of 7 mapped crates (dev-deps correctly excluded) |
| ANSI matrix audit | `cargo test -p repark-sql --lib matrix` | 4 passed |
| Spark matrix audit | `cargo test -p repark-spark --lib matrix` | 6 passed (3 matrix audits + 3 profile/registry pins) |
| Q8 door battery | `cargo test -p repark-sql --test introspection` | 5 passed (4 + the time-travel leak pin from the fix pass) |
| Q11 TA toll | `cargo test -p repark-sql --test ta_toll` | 3 passed |
| Cross-door protocol | `cargo test -p repark-sql --test cross_door` | 8 passed |
| Workspace tests | `cargo test --workspace` (NEVER `--all-features`) | 1175 passed / 0 failed / 0 ignored (1170 before the fix pass; +5 regression pins) |
| Fast gate / full CI | `make ci` / `make preflight` | see the integrator's assembled run |
