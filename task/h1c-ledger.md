# Unit ledger — H-1c: the `$`-metadata introspection rider

**Unit:** V2 Engine Hardening H-1c · **Slate:**
[../briefs/v2-engine-hardening.md](../briefs/v2-engine-hardening.md), unit "H-1c" · **Design:**
[../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md) §2.1 (the second of
the three known correctness issues) + §4 decision D2 ("decide in the unit") · **Status:** drafted
2026-08-10; **panel REJECT the same day, fix pass applied** (see "Fix pass — 2026-08-10" at the end
of this ledger, which supersedes one gate line above); awaiting assembly.

Goal: end the open question with a recorded decision and a deliberate pin update.

## The decision

**Outcome (a) — FILTER.** Metadata tables stop enumerating. The filter lives at the catalog layer,
in `MetadataProjectionSchemaProvider::table_names`
(`crates/repark-iceberg/src/catalog/metadata_projection.rs`), and never in a door parser. They stay
addressable by name.

The decision, the evidence, the rejected alternative and the fork-repin criteria are recorded in
**[../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md)**
— the ADR is the authoritative home, and this ledger does not restate it. What follows is the
*process* record: the evidence-gathering, the deviations, and the gate results.

## Scope

- **`crates/repark-iceberg/src/catalog/metadata_projection.rs`** — the filter plus its four
  decorator-level pins (the fixture reproduces the fork's synthesis rather than mocking a result).
- **`crates/repark-sql/tests/introspection.rs`** — the ANSI-door pin, FLIPPED (not deleted).
- **`crates/repark-core/src/session/tests.rs`** — the bare-core-session pin, FLIPPED (not deleted),
  and split into the two halves of the claim (hidden / still queryable).
- **`crates/repark-spark/src/tests.rs`** — the Spark-door pin (new; the gate requires both doors).
- **`python/repark/tests/test_metadata_tables.py`** — the facade pins (new; the gate requires the
  facade).
- **`docs/adr/0006-…md`** (new) + `docs/adr/map.md`.
- **`STATUS.md`** — the entry is REMOVED (a fixed defect gets no entry and no registry row, per
  STATUS's own boundary paragraph), with a one-LINE "closed out of this section" pointer (state +
  ADR link, no semantics) so the absence is legible.
- **`docs/spark-sql-iceberg-parity.md`** — the §2.1 note that carried the open question is now a
  one-line pointer: ruled 2026-08-10, fixed not declared, so **no row is added** (see D-3).
- **`map.md` lockstep** — `crates/repark-iceberg/`, `crates/repark-iceberg/src/catalog/`,
  `crates/repark-sql/tests/`, `crates/repark-core/src/`, `crates/repark-core/src/session/`,
  `crates/repark-spark/src/`, `python/repark/tests/`, `docs/adr/`, `task/`.

Out of scope: any change to the fork (read-only inspection); the fork's own `table_names`
behavior (a fork-side follow-up, not RePark's to make from here); the other two STATUS known
issues (H-1a / H-1b).

## Evidence gathered before deciding

Recorded because decision D2 says decide **on evidence**, and because a reader six months from now
needs to be able to check the basis rather than trust the conclusion.

### E-1 — what the fork actually does (read-only, vendored source)

`~/.cargo/git/checkouts/iceberg-rust-<hash>/b009ac1/crates/integrations/datafusion/src/schema.rs`,
the rev pinned in the workspace `Cargo.toml`. `IcebergSchemaProvider::table_names` does **not**
return the catalog's listing: it flat-maps each listed base table into itself **plus** one
`format!("{table_name}${}", metadata_table_name.as_str())` per `MetadataTableType::all_types()`.

`MetadataTableType` (`crates/iceberg/src/inspect/metadata_table.rs`) has **fifteen** variants
today: `snapshots`, `manifests`, `files`, `data_files`, `delete_files`, `entries`, `all_files`,
`all_data_files`, `all_delete_files`, `all_entries`, `history`, `refs`, `metadata_log_entries`,
`partitions`, `all_manifests`. So a namespace of N base tables enumerated as **16 N** names.

Resolution is independent: `table()` and `table_exist()` each `split_once('$')` and build the
metadata table on demand. **Nothing about addressability reads `table_names()`.** That is the fact
that makes "hide" a strictly smaller change than "declare" — it costs nothing at all.

### E-2 — where a filter can live, and what it reaches

`MetadataProjectionSchemaProvider` (RePark-owned) already wraps every schema provider the engine
registers: `snapshot_all_schemas`, `build_namespace_schema`, and `CatalogProvider::register_schema`
in `crates/repark-iceberg/src/catalog/provider.rs`. There is no unwrapped registration path, so a
filter in its `table_names` is total.

DataFusion 54.1: `datafusion-catalog/src/information_schema.rs` builds `tables`, `views` and
`columns` by iterating `schema.table_names()`; `datafusion-sql/src/statement.rs` plans
`SHOW TABLES` as literally `SELECT * FROM information_schema.tables`. **The "twin path" the
acceptance gate asks about is the same path** — `SHOW TABLES` cannot disagree with
`information_schema.tables` by construction. Both spellings are still asserted at **both doors and
the facade** (the bare-core-session pin issues `information_schema` queries only), because "by
construction" is a claim about today's DataFusion, not a pin.

Same file: `make_tables` calls `schema.table_type(&name)`, whose `SchemaProvider` default is
`self.table(name)`, and `make_columns` / `make_views` call `schema.table(&name)` outright. Each
such resolution costs the fork **two** `load_table` calls, not one: `IcebergTableProvider::try_new`
loads the base table for its schema, then `metadata_table()` loads it again ("Load fresh table
metadata for metadata table access", fork `crates/integrations/datafusion/src/table/mod.rs`). So on
the OLD behavior every introspection query performed **thirty** object-storage round-trips per base
table (2 × 15 synthesized names) — measured with a `load_table` counter under a one-base-table
namespace: **31 calls before the filter, 1 after**.

### E-3 — what Apache Spark + Iceberg does, and what oracle basis we honestly have

**We cannot observe it here, and the ADR says so.** The live oracle tier is plain PySpark 4.1.2
(`_live_parity.py::build_spark_engine`, `make parity-live`, `.github/workflows/parity-live.yml`):
no `iceberg-spark-runtime` jar is provisioned, no Iceberg catalog is configured, and the harness's
`Engine` surface is `createDataFrame` / DataFrame API / `functions` — there is no path in it that
creates an Iceberg table, so there is no way to ask a live Spark what `SHOW TABLES` returns on an
Iceberg catalog. Standing up an Iceberg-enabled Spark is a real unit (a jar, a catalog config, a
warehouse), not a line in this one.

So the Spark half is **documented**, and the documents are named so the basis is checkable:

- Iceberg's Spark docs treat metadata tables as a naming convention over an existing table —
  "Metadata tables are identified by adding the metadata table name after the original table name.
  For example, history for `db.table` is read using `db.table.history`"
  ([Spark Queries → "Inspecting
  tables"](https://iceberg.apache.org/docs/latest/spark-queries/#inspecting-tables)). They are never
  presented as catalog entries.
- The implementation agrees: `SparkCatalog.listTables` delegates to
  `icebergCatalog.listTables(Namespace.of(namespace))` and maps the persisted identifiers,
  synthesizing nothing ([apache/iceberg
  `spark/v4.0/…/SparkCatalog.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java));
  the metadata-table suffix is recognised on the load path instead.

This limitation is not a footnote — it is **load-bearing for the decision** (see D-2): a "keep and
declare" outcome would have needed a live mirror it cannot have.

### E-4 — what Trino does

Documented, with the document named: "You can query each metadata table by appending the metadata
table name to the table name: `SELECT * FROM "test_table$properties"`", while the same page's
account of listings is about the metastore — "The `SHOW TABLES` statement,
`information_schema.tables`, and `jdbc.tables` will all return all tables that exist in the
underlying metastore" ([Trino — Iceberg connector, "Metadata
tables"](https://trino.io/docs/current/connector/iceberg.html#metadata-tables)). `t$snapshots` is
not a metastore table, so it is addressable and unlisted.

Stated at its true strength: the docs give the addressing rule and the listing rule and the
conclusion follows from the two — they contain no sentence reading "metadata tables are hidden from
`SHOW TABLES`". This is the shape adopted — hidden from the listing, addressable by name — and it
is also what the ANSI door's Trino-flavoured positioning would lead a user to expect.

### E-5 — the pins that existed, and what they asserted

| Site | Test (before) | Asserted |
|---|---|---|
| `crates/repark-sql/tests/introspection.rs` | `metadata_tables_currently_enumerate_alongside_the_real_table` | `count(*) > 0` over `information_schema.tables` matching `orders$%` |
| `crates/repark-core/src/session/tests.rs` | `information_schema_still_exposes_the_dollar_metadata_tables` | the same count, on the bare session |

Both were deliberate red-on-purpose instruments. Both were flipped in the same diff as the
behavior, neither was deleted, and each new doc comment names its former test name and the reason
it changed — the brief's "a pin that changes without a stated reason is indistinguishable from a
regression" clause.

## Decisions taken in the unit

- **D-1 — the filter is narrow, and shares the fork's vocabulary.** A name is dropped only when it
  splits on `$` into a suffix that parses as a fork `MetadataTableType` **and** a base the wrapped
  provider knows. Rejected: `name.contains('$')`, which would silently hide a real table named
  `q1$fy26`. The `MetadataTableType` read means a fork rev that adds a metadata table is covered
  with no edit here — the anti-pattern this repository already has a lesson about (a gate keyed off
  a hand-maintained table, 2026-08-10) is avoided by not maintaining a table.
- **D-2 — "keep and declare" was rejected on four grounds**, recorded in the ADR. The one worth
  repeating in a process ledger: the registry's honesty mechanism for a declared row is the live
  mirror, and E-3 shows this row could not have had one. A documented-basis row with no drift
  detector, for a difference from *both* reference engines, where a three-line fix exists, is the
  weakest artifact this repository knows how to produce.
- **D-3 — no registry row, and STATUS loses its entry.** The registry's §6 boundary is explicit: a
  known defect whose fix lands is not "disposed of" as a divergence; the fixing unit **deletes** the
  STATUS entry rather than moving it. The outcome here is a convergence with both engines, so a row
  would be a false claim of difference. The §2.1 note in the registry — which existed only to say
  "this is an open issue, not a row" — is rewritten to record the closure and point at the ADR, so
  the pointer chain a reader follows does not dead-end.
- **D-4 — the STATUS entry is deleted but its absence is annotated.** A bare deletion would leave a
  future reader unable to tell "fixed" from "forgotten", and the campaign is mid-flight with two
  sibling units still describing their issues in that section. A single "Closed out of this
  section" line carries state + the ADR link and nothing else, and it is deleted at campaign
  close-out. This is the brief's "state + link" instruction applied to an outcome the brief's own
  boundary paragraph sends to removal. **Corrected 2026-08-10 (fix pass):** the drafted paragraph
  claimed "no semantics" while stating three behavioral facts, and the registry's §2.1 note stated
  them a third time — a parity grep would have found three descriptions of one fact, the exact
  condition §6 forbids. Both are now pure pointers (one line each, zero behavior words); the
  semantics live ONLY in ADR-0006.
- **D-5 — the core-session pin was split into two tests rather than grown.** The claim has two
  halves that fail differently: a filter in the wrong method satisfies "does not enumerate" and
  breaks "still queryable". One test asserting both would go red on either, but the *name* of the
  failing test is what a reader gets first — two names make the two failure modes distinguishable
  without reading the body. The same split is why the door and facade pins each assert both halves.

## Deviations and flags

- **F-1 — the facade census additions ledger was not touched.** Two facade node ids are added
  (`test_metadata_tables.py`), and `task/port/added-python-tests.txt` exists for exactly that
  bookkeeping. It was left alone to match the precedent set one commit earlier: H-1d added a facade
  test (`test_parity_live.py::test_disclosures_mirror_the_registry`) and did not append either. The
  port census closed at milestone one and is not a CI gate. **Flagged, not silently skipped:** if
  the campaign re-runs a census, H-1c's two ids and H-1d's must be declared together, and the
  standing lesson ("DO give the census a mirror ADDITIONS ledger", 2026-08-08) argues the
  convention should either be re-armed for the campaign or explicitly retired.
- **F-2 — a pre-existing fork sharp edge, out of scope, reported not fixed** (extended 2026-08-10 in
  the fix pass; the first draft understated it by half). Two halves, both fork-level:
  1. *Unreachable.* A real Iceberg table named `<base>$<not-a-metadata-type>` (e.g. `a$b`) is
     **unreachable** through the fork today: `IcebergSchemaProvider::table()` splits on `$`
     unconditionally and fails with `invalid metadata table type: b` rather than resolving the real
     table. The filter deliberately leaves such a name **visible** in the listing (its suffix does
     not parse), which is the honest side of the trade — an unaddressable name that is listed is a
     bug you can see.
  2. *Still enumerates.* Worse, and missed in the first draft: for that same base the fork
     synthesizes `a$b$snapshots` … ×15, and the filter **cannot drop any of them** —
     `split_once('$')` takes the FIRST separator, so `a$b$snapshots` reads as base `a` / suffix
     `b$snapshots`, which is not a `MetadataTableType`. `SHOW TABLES` therefore still shows all
     sixteen names for a `$`-base. `rsplit_once` is not the fix: the fork's own `table_exist("a$b")`
     splits on the first `$` too and answers false, so the base-existence guard could never confirm
     such a base. Every one of the sixteen is unresolvable through the fork either way.
  **Disposition: document and pin, do not engineer around a fork-level pathology.** The 16-name
  listing is now asserted in `the_filter_keeps_names_the_fork_did_not_synthesize`, so the residue is
  pinned rather than latent, and it is written down in ADR-0006 ("Residue"),
  `crates/repark-iceberg/map.md` and the catalog `map.md` `## Debug` table. **The fork-side row (a
  `$`-tolerant `table()` / `table_names()` in `IcebergSchemaProvider`) is filed by the orchestrator
  against the fork, not from this unit** — capability status lives only in the fork's
  `docs/parity/GAP_MATRIX.md`.
- **F-3 — no new mechanical gate, so no provocation proofs.** The unit ships behavior and pins, not
  a detector; the brief's provocation-proof requirement is scoped to "any new mechanical gate". The
  equivalent evidence for a behavior change is the mutation note on each pin (every new test's doc
  comment names the edit that turns it red), which is what `docs/testing.md` asks for instead.
- **F-4 — `docs/spark-sql-iceberg-parity.md` is a high-traffic file.** PR #36 and a parallel sweep
  unit also edit it. H-1c's edit is confined to the seven-line note under §2.1 (registry rows and
  §6 are untouched), so a rebase conflict should be textually local — but the branch must be
  rebased on `main` immediately before hand-off and the note re-read, not merely re-merged.

## Gate results — DRAFT PASS (2026-08-10, superseded in part)

Every exit code below is the REAL one (`PIPESTATUS[0]`, never a pipe's).

> **One line in this table was false and is corrected below.** The `make py-test-facade` row read
> "GREEN (exit 0) — 2533 passed, 44 skipped". It was a **lucky row ordering**, not a passing test:
> the new facade pin compared two unordered metadata-table scans element-wise, and the verifier
> measured it **red 11 out of 11 runs** (`1 failed, 2532 passed, 44 skipped`, REAL_EXIT=1) on this
> same tree with the source byte-identical. The draft green was a single run of a ~1-in-6 coin
> flip. The true counts, after the assertion was made order-insensitive, are in the fix pass at the
> end of this ledger. The rest of this table re-ran clean and is unchanged.

| Gate | Result (draft pass) |
|---|---|
| `make ci` | **GREEN** (exit 0) — fmt, clippy, panic-ban, crate-DAG (20 edges), `check_lib_rs` (9 roots), `check_lib_py` (53 files), `check_manifest` (12 components), `cargo check`, ruff check + format (238 files), `uv lock --locked`, taplo, typos |
| `make test` (Rust workspace) | **GREEN** (exit 0) — **1275 passed, 0 failed, 0 ignored** (1269 at H-1d + 6 new: 4 decorator units, 1 Spark door, 1 core session; the ANSI-door and core flips are in-place) |
| `make py-test-facade` | ~~**GREEN** (exit 0) — **2533 passed, 44 skipped**~~ **FALSE — the run was a lucky ordering; measured red 11/11 by the verifier.** See the correction note above and the fix pass below |
| `scripts/check_map_md.sh` (staged-set method) | **GREEN**, with a negative control: unstaging `crates/repark-iceberg/src/catalog/map.md` reds it (`ERROR: … was not updated in this commit`), restaging returns it to green |
| `scripts/check_manifest.sh` | **GREEN** — "12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index, the status document and the crate maps" |
| Public-hygiene two-pass | **ZERO** on both passes — added-line content grep over the staged diff, and author metadata |

One clippy finding was raised and fixed inside the unit: `clippy::doc_markdown` on a bare product
name in the new module doc (`make ci` is the gate that caught it — a bare `cargo clippy
--all-targets` is NOT the repository's invocation, which passes `-A clippy::disallowed_methods`
because the panic ban is a separate `--lib --bins` target).

## Fix pass — 2026-08-10 (post-panel)

The four-verifier panel returned **REJECT** on one BLOCKER (a claimed-green facade gate that was
measured red 11/11) plus four MAJORs and three NITs. Every disposition below was applied in this
worktree; nothing was deferred, and one item was deliberately *not* engineered around (F-2, by
disposition).

| # | Finding (severity) | What was actually wrong | Action taken |
|---|---|---|---|
| 1 | **BLOCKER** — nondeterministic facade pin | `test_a_hidden_metadata_table_is_still_queryable_at_the_facade` compared two unordered metadata-table scans element-wise. The draft's single green was a lucky ordering; the verifier measured 11/11 red | Assertion is now order-insensitive (`sorted(...) == sorted(...)`), matching this module's own idiom (`test_snapshots_schema_and_count`, `test_snapshots_count_show_and_partial_projection` compare the same column as a set). Docstring states why order is not a promise. The false gate line is struck through and corrected in "Gate results" above |
| 2 | **MAJOR** — perf number wrong by 2× | The saving is **thirty** `load_table` round-trips per base table, not fifteen: the fork does two per synthesized name — `IcebergTableProvider::try_new` loads the base table for its schema, `metadata_table()` loads it again (fork `crates/integrations/datafusion/src/table/mod.rs`). The verifier **measured** 31 → 1 on a one-table namespace | Corrected at all four sites — ADR-0006 "Cost, not only aesthetics" and its "Alternative considered" bullet, `metadata_projection.rs` module doc, and E-2 above — and the basis upgraded from code-reading to **measured**, with the measurement described in one sentence in the ADR. ACTOR-REPORT.md §§3/4 is **stale on this number**; this ledger supersedes it |
| 3 | **MAJOR** — `$`-in-base corner | "Drops exactly the synthesized names" is false for a base table whose own name contains `$`: `split_once` takes the FIRST `$`, so `a$b$snapshots` is unrecognisable and all sixteen names still enumerate | **Documented and pinned, not engineered around** (fork-level pathology; `rsplit_once` cannot fix it because the fork's own `table_exist("a$b")` splits the same way). The three "drops exactly" claims (module doc, ADR decision 1, `crates/repark-iceberg/map.md`) now state the true rule; ADR gains a "Residue" consequence; F-2 is extended with the enumeration half; `the_filter_keeps_names_the_fork_did_not_synthesize` now pins the 16-name listing for base `a$b`; catalog `map.md` `## Debug` gains the row. **The fork-side row is filed by the orchestrator against the fork**, not from this unit |
| 4 | **MAJOR** — three descriptions of one fact | STATUS's "Closed out of this section" paragraph and the registry §2.1 note each restated the behavior, so a parity grep found three descriptions — the condition registry §6 exists to forbid | Both cut to **pure pointers**: STATUS is one line (fixed in H-1c 2026-08-10 + ADR link, zero behavior words); §2.1 is "ruled 2026-08-10: fixed, not declared — so no row here (§6)" + ADR link. Semantics live ONLY in ADR-0006. D-4 above records the correction |
| 5 | **MAJOR** — false citation | `metadata_projection.rs` claimed the end-to-end coverage lives in `catalog/tests.rs`; that file contains no metadata-table pin at all | The fixture doc now names the real sites: `crates/repark-sql/tests/introspection.rs`, `crates/repark-spark/src/tests.rs`, `crates/repark-core/src/session/tests.rs`, `python/repark/tests/test_metadata_tables.py` |
| 6a | NIT — self-contradiction | "nothing addressable is ever hidden" contradicts decision 2, whose point is hidden-**and**-addressable | ADR decision 3 and the predicate doc now say what was meant: **no ordinary table is hidden**, nothing stops being addressable, and an unresolvable `$`-name is left visible rather than quietly disappeared |
| 6b | NIT — uncited documented basis | The two claims carrying the decision named no document | Both cited, in honest documented-basis form: [Iceberg — Spark Queries, "Inspecting tables"](https://iceberg.apache.org/docs/latest/spark-queries/#inspecting-tables) + [`SparkCatalog.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java) (`listTables` synthesizes nothing), and [Trino — Iceberg connector, "Metadata tables"](https://trino.io/docs/current/connector/iceberg.html#metadata-tables). E-4 states the Trino citation at its true strength: the docs give the addressing rule and the listing rule; the "unlisted" conclusion follows from the two rather than being quoted |
| 6c | NIT — stale fork pin | `test_metadata_tables.py`'s module docstring pinned rev `4723104b` with line ranges that do not exist at the workspace rev | Repointed to `b009ac1` and switched to **symbols, not line numbers**, so the next repin cannot silently rot it |
| — | Panel NIT not in the dispositions, fixed anyway | E-2 claimed both spellings are asserted "at every entry point"; the bare-core-session pin issues `information_schema` queries only | E-2 now reads "both doors and the facade", and names the exception |

### Provocations re-run (verbatim)

**must-FAIL — the extended residue pin discriminates.** Mutation: "engineer around" the `$`-base
residue in `is_synthesized_metadata_table_name` — `rsplit_once('$')` plus a base guard relaxed to
`inner.table_exist(base) || base.contains('$')` (the change the ADR's Residue bullet argues against):

```
test catalog::metadata_projection::tests::the_filter_keeps_names_the_fork_did_not_synthesize ... FAILED
thread '…::the_filter_keeps_names_the_fork_did_not_synthesize' panicked at
crates/repark-iceberg/src/catalog/metadata_projection.rs:432:9:
first-`$` split: `a$b$snapshots` reads as base `a` / suffix `b$snapshots`, not a metadata table — so the filter cannot drop it
test result: FAILED. 9 passed; 1 failed; 0 ignored; 236 filtered out
REAL_EXIT=101
```

A second mutation (predicate replaced by the naive `name.ends_with("$<type>")` suffix test) reds the
same test on its older `ghost$snapshots` guard at `:411`. Source restored byte-identically after
each (`diff` against a pre-mutation copy: no output), and the test file re-runs green:
`test result: ok. 10 passed; 0 failed; 0 ignored; 236 filtered out`, `REAL_EXIT=0`.

**must-FAIL — the pre-fix facade assertion, on this same tree.** The element-wise comparison was
restored temporarily and run 12×:

```
PRE-FIX ASSERTION: PASS=3 FAIL=9
E   At index 0 diff: 1819267016418053120 != 6177263815757421179
E   At index 0 diff: 4960357649119442083 != 8487159203780261699
E   At index 0 diff: 1414093224103959854 != 9137047812987409383
E   At index 0 diff: 5841581277173770317 != 4327166435018302258
```

That is the BLOCKER reproduced under the fixer's hand — the scan order genuinely varies run to run,
so the draft's single green was luck. File restored byte-identically afterwards.

**must-PASS — the fixed pin is deterministic.** Same node id, 12 consecutive runs:

```
run 1..12  rc=0   1 passed in ~0.58s each
PASS=12 FAIL=0
```

### Gate results — FIX PASS (2026-08-10)

Exit codes captured with `cmd > log 2>&1; EXIT=$?` — never through a pipe (the draft pass's
`${PIPESTATUS[0]}` after a parenthesised pipeline reports `tail`'s status, not the gate's; that
error is recorded here so it is not repeated).

| Gate | Result |
|---|---|
| `make ci` | **GREEN**, real exit 0 — crate-DAG 20 edges, `check_lib_rs` 9 roots, `check_lib_py` 53 files, `check_manifest` 12 components, `cargo check --locked --workspace`, ruff 0.15.22 check + format (238 files), `uv lock --locked`, taplo 0.9.3, typos 1.47.2 |
| `make test` (Rust workspace, never `--all-features`) | **GREEN**, real exit 0 — **1275 passed, 0 failed, 0 ignored** summed over 31 `test result: ok` lines (unchanged: the new `$`-base assertions extend an existing test rather than adding one) |
| `make py-test-facade` (FULL suite) | **GREEN**, real exit 0 — **2533 passed, 44 skipped**, 0 `FAILED` lines. Run twice end-to-end (93.61 s and 103.43 s); the second is the one whose exit code was captured directly |
| Facade determinism loop (12×, `test_a_hidden_metadata_table_is_still_queryable_at_the_facade`) | **12/12 GREEN** (rc=0 each). Same loop against the pre-fix assertion: **3 pass / 9 fail** |
| `scripts/check_map_md.sh` (staged-set method) | **GREEN**, real exit 0, with the negative control re-run: unstaging `crates/repark-iceberg/src/catalog/map.md` gives `ERROR: … was not updated in this commit (map.md lockstep rule)` at **exit 1**; restaging returns exit 0 and `git diff --cached \| sha256sum` is identical before and after (`1924d450…`) |
| `scripts/check_manifest.sh` | **GREEN**, real exit 0 — "12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index, the status document and the crate maps" |
| Public-hygiene two-pass | **CLEAN** both passes — pass 1: added-line grep over the staged diff against the forbidden-content classes (local filesystem paths, tool-vendor domains, session identifiers, ARNs, long numeric ids, scratch/worktree paths, sibling repo names, author email); pass 2: the same classes over the **full content** of all 18 staged files. Zero real hits. Five *reported* hits, all inspected and all false positives of the ≥12-digit AWS-account-id heuristic: four are the verbatim Iceberg snapshot ids in the must-FAIL transcript above (i64s generated by an ephemeral local memory catalog — no account, no ARN, no resource), and the fifth was this row itself, which then named the patterns literally — reworded to class descriptions so the hygiene record cannot itself trip the gate. Verbatim snapshot-id capture was kept rather than redacted, because the ids are the evidence that the two scans differ only in order |
| Relative-link check | **0 broken targets** over every staged Markdown file plus the untracked `ACTOR-REPORT.md` |

**Hand-off state:** all changes staged, not committed; no pushes. Untracked: `ACTOR-REPORT.md`
(stale on the perf number and on the facade gate — this ledger is the authority) and this fix pass's
own entries. No scratch files remain in the worktree.
