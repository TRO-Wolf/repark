# Roadmap intake — 2026-08-21

**What this is.** The single record of every campaign brief, design note, queue, and grant that
had existed only in planning space and never in either repository. It is an **intake**, not a
plan of record: [../STATUS.md](../STATUS.md) remains the SSOT for current state, and each item
below graduates into the spine (STATUS.md deferred capabilities / [../PROJECT.md](../PROJECT.md)
roadmap sections) as it is chartered. Nothing here is authoritative merely by being written down.

**Evidence date.** Every status below was verified 2026-08-21 against the merged-PR history of
this repository (through #193), the owned `iceberg-rust` fork (through #206), and the dbt adapter
(through #6) — and, where a claim was disputed between sources, against the tree itself.

**How to consume it.**

1. **Re-verify before you depend on it.** Statuses are dated, and the tree moves. Re-check against
   fresh `origin/main` before writing any item here into a repo document.
2. **Hygiene applies.** Express items in neutral repo-safe wording — never cite planning-space
   paths, private repository or employer names, absolute home paths, or session identifiers.
   The forbidden-pattern pre-push hook rejects violations and does not catch all of them.
3. **`[OWNER]` marks an open decision.** Record it; do not resolve it on the owner's behalf.
   Decisions the owner *has* ruled are marked **RULED** with the date, and stop being decisions.
4. **Section B is closed.** It exists so finished work is not re-proposed. Do not re-add from it.

---

## A. OPEN WORK — the roadmap additions

### A1. Owner-granted immediate queue (grant of 2026-08-17; open on the owner's word)

| Unit | Description | Status |
|---|---|---|
| **B1 — euro-comma decimal cast** | `CAST('1.234,56' AS DECIMAL)`-class strings: oracle-first (Spark almost certainly does NOT locale-parse — expect NULL off-ANSI / error on-ANSI); outcome is a registry row or a kernel fix, whichever the oracle indicts. | Queued. |
| **D2 — Iceberg write-boundary nullability relax** | A `declareSorted(..., tightenNulls=True)`-narrowed frame written to Iceberg must emit exactly the tightened key fields as optional/nullable again (track the tighten-set; relax only those). Data-contract stays optional at the write boundary. | Queued. D1 (the tighten flag + refusals) landed as #173. |
| **c22 — "have-it-all" function wave 1** | Implementation of the ratified FN-3 wave-1 queue (see A3). | Queued; the queue needs a truth-up first — `randstr`/`uniform` already landed via #190. |

### A2. Iceberg MOR operability — the write-path maintenance wave (MW)

**Chartered by the owner 2026-08-21 and green-lit for immediate start** (OD-4, below).

Source: three independent evaluations of whether merge-on-read is production grade, reconciled
into one scope and re-verified against the tree. **Verdict:** the MOR write path is
production-grade in correctness terms; MOR *as a capability* is blocked on maintenance
operability. Every `CALL system.*` maintenance procedure refuses on exactly the production
catalogs, so a MOR table accumulates position-delete files with no in-engine way to compact,
expire, or reclaim them — the abort path's `CommitStateUnknown` design explicitly defers
reclamation to orphan maintenance that cannot run there.

**The discovery that resized the campaign:** the fork at the current pin already ships
`delete_orphan_files`, `rewrite_position_delete_files`, `remove_dangling_delete_files`,
`delete_reachable_files`, `convert_equality_delete_files`, `rewrite_manifests`,
`rewrite_table_path`, and the stats actions. **None of them is local-only**, and the engine-side
refusal carries no local-path assumption either — the fence is pure policy. The whole campaign is
engine-side wiring with **zero fork work on the critical path**. The in-repo comment calling
orphan-files a "fork-queue residual" is stale and is trued up by MW-3.

Units, dependency-ordered:

| Unit | Scope | Size |
|---|---|---|
| **MW-0** | Charter + measurement floor: fork action signatures for the four wired actions; oracle-pinned `CALL` result schemas; a baseline MOR delete-file-growth demo that MW-2/MW-4 must move. Lands the campaign's design note and slate. No product change. | S |
| **MW-1** | Lift the maintenance fence on `expire_snapshots` / `rewrite_data_files` / `rollback_to_snapshot` for **both** remote catalog policies (OD-1). Refusal-preservation pins for unknown procedures; error text pinned. | S/M |
| **MW-2** | Wire `CALL system.rewrite_position_delete_files` — the one procedure that exists specifically to serve MOR. Scope floor matches `rewrite_data_files` austerity (no filter, no sort); deferrals documented loud. | M |
| **MW-3** | Wire `CALL system.remove_orphan_files`. Highest danger: deletes files with no rollback. Dry-run default, `older_than` floor, declared registry divergence (OD-2). Trues up the stale comment, the supported-procedure list, the crate maps, and the guide's maintenance table. | M |
| **MW-4** | MOR leg in the tier-2 AWS acceptance: CTAS at `merge-on-read` → MERGE → identical MERGE (idempotent) → assert position-delete files exist → row parity → compact + expire → row parity again. Gated on OD-3. | M |
| **MW-5** | Registry + docs truth-up, campaign close: new divergence rows, STATUS scorecard flip, guide and map lockstep, re-measure the MW-0 baseline and record the delta. | S |

**Measured at intake (2026-08-21), carried in as MW-0 inputs:**

- The `CALL` result-schema oracle **runs**: an Iceberg Spark-4.0/Scala-2.13 runtime loads into the
  pinned PySpark 4.1.2 oracle and executes procedures. `rewrite_data_files` and
  `rewrite_position_delete_files` measured clean. `expire_snapshots` and `remove_orphan_files`
  fail on a Spark 4.0→4.1 binary break (`DataSourceV2Relation.create` signature drift) — their
  schemas come from the Iceberg jar's own `OUTPUT_TYPE` constant instead (Q5, RULED).
- **An undeclared divergence, found at intake:** the engine pins `rewrite_data_files` at four
  result columns; Spark 4.1 returns **five** — `removed_delete_files_count` is absent. MW-1 fixes
  it and MW-5 registers it.
- `rewrite_position_delete_files`'s measured Spark result schema matches the fork result type's
  four accessors exactly — names, order, and int/bigint split. MW-2 gets schema parity for free.
- The fork's orphan action already defaults `older_than` to `now − 3 days` and prefix-mismatch to
  `Error`, and refuses up-front when `gc.enabled` is false. It exposes no `dry_run`, but its
  delete function is replaceable and it returns the full orphan list regardless of the deleter —
  so MW-3's dry-run default is engine-side plumbing, not fork work. It takes **no catalog**: it
  runs off the table's FileIO.
- The BUG-001 valve gates SQL `DELETE`/`UPDATE` under merge-on-read with an unpartitioned current
  spec and multiple specs in history — and **never gates `MERGE`**. MW-4's leg is unaffected.

**Owner decisions:**

| ID | Decision | Disposition |
|---|---|---|
| OD-1 | Lift the maintenance fence for the service-managed catalog too, or the explicit-location catalog only? | **RULED 2026-08-21 — lift for BOTH.** The owner's reason: the service-managed surface is arguably the more important of the two. This overrides the intake recommendation to keep it fenced, so the hazard it was fencing must now be *handled* rather than avoided: service-side maintenance racing an in-flight engine position-delete is a named hazard class in the fork's engine contract. MW-1 carries it as an explicit, documented condition. |
| OD-2 | Orphan-files safety posture: dry-run default plus a minimum `older_than` floor. | **RULED 2026-08-21 — adopt both**, and declare the resulting divergence from Spark's more dangerous default as a registry row. |
| OD-3 | Extend the tier-2 acceptance role with scoped delete on the scratch prefix. | **RULED 2026-08-21 — yes.** Owner executes the change; MW-4 is gated on it and no other unit is. |
| OD-4 | Campaign green-light and priority against A1. | **RULED 2026-08-21 — green-lit, start immediately.** |
| Q5 | The two procedures whose result schemas the 4.1.2 oracle cannot measure. | **RULED 2026-08-21** — read the schema out of the Iceberg jar's own constant rather than installing a second oracle or asserting from documentation. |

**Interim posture (true today, until MW lands).** Copy-on-write upsert on the explicit-location
catalog is defensible for a bounded pilot: single-writer-per-table enforced, wheel and fork rev
pinned, a deliberate isolation choice, an external engine for snapshot expiry, and reconciliation
after every run. **MOR anywhere in production: no** — lab and staging only, local catalogs, with
an operator compacting. **Never:** blind retry after `CommitStateUnknown`; flipping the merge mode
on a table with incremental CDC consumers; DELETE-then-INSERT as an upsert pattern; multi-writer
on one table.

**Watch, do not schedule:** `WHEN NOT MATCHED BY SOURCE`; REST/Hive/Nessie catalogs;
branch-targeted writes (REF-1, fork API work); sort/z-order rewrite strategies; incremental
snapshot reads; `remove_dangling_delete_files` as its own procedure.

**Format v3 and deletion vectors left this list on 2026-08-21** — owner-scheduled after MW-2
measured the exposure. See **A12**.

### A3. Function-surface completion (the largest open program)

**FNP campaign — openly incomplete.** #190 (41 names, `__all__` 333→360) + follow-ups
#191/#192/#193 landed; the campaign's own record says more units remain (next FNP-15/FNP-16).
A name-by-name reconciliation of the old FN-2 queue (GT3, GA, G1–G6 batches: try-family,
scalar odds, engine aggregates, datetime constructors, JSON family, design-gated G6 pile —
lambdas/generators/time-window/grouping-sets/NTZ/TIME/to_char) against what FNP absorbed has
NOT been done and is itself a small unit.

**FN-3 "have-it-all" families** (owner ruling 2026-08-17: the 75 refuse-pile names flip to
BUILD — "I want our data engine to have it all"). Designs ratified, implementation unstarted:

| Family | Names | Design | Implementation |
|---|---|---|---|
| H1 hash/crypto | `sha`/`sha1`; `aes_encrypt`/`aes_decrypt`/`try_aes_decrypt` (GCM/CBC/ECB, Spark-exact IV/AAD) | Ratified | Not landed |
| H2 serializers | `to_csv`, `to_xml` (dep-free row→string kernels) | Queue row | Not landed |
| H3 string/random | `mask` (+ `randstr`/`uniform` **already landed** #190) | Ratified | `mask` not landed |
| H4 sketches | HLL (4) + Theta (7) UDAFs over Binary via datasketches crate; `count_min_sketch` = Spark's own V1 CMS, not DataSketches; KLL **blocked** (no crate feature); `bitmap_*_agg` sibling | Ratified (~10–17 eng-days) | Not landed |
| H5 xpath | `xpath` + 8 typed variants (sxd-document/sxd-xpath; no libxml) | Queue row | Not landed |
| H6 VARIANT | 8 functions over the pinned parquet 58.4 `variant_experimental` encoding; Parquet IO + Iceberg V3 variant are later fork-gated increments | Ratified (~13–22 eng-days; biggest single item) | Not landed |
| H7 geospatial | 5 `st_*` names, WKB+SRID physical type, CRS84 axis order, no GEOS | Ratified (~7–12 eng-days first increment) | Not landed |
| H8 file-block metadata | `input_file_block_start`/`length` (documented-unavailable contract `("",-1,-1)`) + `input_file_name` stub upgrade | Queue row | Not landed |
| W7 collation | Owner ruling supersedes the old refuse-default: `collation(col)` → `'UTF8_BINARY'`, `collate(col,'UTF8_BINARY')` typed no-op, any other name refuses loud | Ruled | Not landed |

Carve-outs (refuse-by-physics; deliverable is a registry payload only, not code):
`reflect` / `java_method` / `try_reflect` (no JVM), `unwrap_udt` (no UDT system) — payload
rows not yet confirmed written.

**Tabled by owner (keep visible, do not schedule):** `LOG-1` — SQL-door `log(x)` is base-10
(DataFusion) vs Spark's natural log — AND `F.log`'s missing 2-arg overload; both tabled
together because the only available kernel lacks Spark's null-guard (six non-positive edges
return `-0.0`/`NaN`/`-inf` where Spark returns NULL).

### A4. Correctness items — new, unregistered, or disclosed-but-unfixed

- **Nightly live-oracle tier is RED and has been every night since 2026-08-16.** Verified
  2026-08-21: the pinned-Apache smoke suite fails 3, of which two identified:
  (1) `test_udf_with_collated_string_types` — the one EXPECTED red (collation refusal);
  (2) `test_infer_map_pair_type_with_nested_maps` — **a real, unregistered divergence**:
  nested-map value inference yields string `'200.5'` where Spark keeps float `200.5`.
  Needs: triage + registry row or fix, plus a mechanism so the expected red doesn't mask
  real reds (allowlist or split job). Third failing entry unidentified — triage it too.
- **MERGE audit residuals M17/M18 (plausible, unconfirmed, unfixed):** M17 —
  non-deterministic MERGE source is re-planned per pass and can disagree across passes (Spark
  materializes the source once); M18 — `resolve_affected_data_files` couples to
  `current_snapshot()` rather than an explicitly pinned snapshot. Confirm-or-refute unit.
- **Dotted-path struct select** `df.select("p.a")` refuses where PySpark resolves by dotted
  path (oracle the exact precedence + backtick rules first). Never reached by its bug round.
- **`count()` qualified-name ambiguity** on deep dynamicFlatten plans. Never reached.
- **ANSI-door `repark.sql()` wrong-door-sniff reachability** follow-through (binder-ceiling
  scoped). Never reached.
- **Duplicated `__repark_cdf_*` qualified-field schema smell** on createDataFrame-sourced
  frames — noted beside the fixed explode case-loss bug; disposition after the dynamicFlatten
  rewrite (#183) unconfirmed. One-session check.
- **GT1-FIX named residual families** (already in STATUS.md — cross-reference, don't
  duplicate): regexp mid-surrogate scan gap, `$`/`(?m)^` terminator families, ANSI-off
  semantics, numeric implicit-cast breadth, `split_part` NULL/0 edge, SQL-literal backslash
  escapes, `CAST(x AS BINARY)` unimplemented, `bin(True)`/`rint` boolean over-accept, double
  stringify Infinity/E-notation.

### A5. Unmerged work sitting on branches — **[OWNER]** merge / rework / abandon

- **Temp-view-home hardening rounds 4–7** (branch tip `147b79e`, never PR'd to main): the
  post-#173 orchestrator extension — DDL-door refuse, PreExecute belt, temp-view-home
  discipline, read-side home fixes; includes the 7 disclosed engine-side bare-name scratch
  sites and the `listTables` cache/checkpoint filter gap (both already disclosed as STATUS
  residuals). Decision: rebase-and-land, re-scope, or abandon with the disclosures standing.
- **TA `ad`/`obv` single-write construction** (local commit `b605236`, never pushed): coded
  and gold-verified, paused by the owner's 2026-08-17 halt before its measurement gate ran.
  Decision: revive behind the FRESH measurement gate or drop.

### A6. dbt adapter track

Landed: session-lifetime P0 (dbt-repark #6). Open, in order:
- **DBT-U2** — re-pin to the current engine wheel, repair the dev venv, land FIRST CI
  (lint + unit + gated AWS path; the M0.7 gate has been unmet since 2026-08-10), re-derive the
  residual-refusal table against current engine semantics.
- **DBT-U3** — standard verbs: `dbt seed` (literal-rendering override), `docs generate`
  (get_catalog), DESCRIBE type-name mapping (currently can mis-drive `on_schema_change`).
- **U-4** — honesty pass: ANSI-TRUE default, `memory_limit_gb` profile key, merge-residual
  README, early refuse of `WHEN NOT MATCHED BY SOURCE`.
- **U-5** — sources + real sample project.
- Engine prerequisites (fork/engine side, unscheduled): **persistent non-AWS catalog**
  (memory catalog is process-ephemeral — `dbt run` twice fails offline), **durable Iceberg
  VIEW surface** (owner-ELEVATED 2026-08-10; needed for honest `materialized='view'`),
  **session concurrency contract** for `threads > 1` (adapter hard-refuses today).
- Recorded not-units: python models, snapshots, microbatch.

### A7. Performance / TA track

- **Sort-elimination remainder:** PR-D3 — Spark-door EXPLAIN must render the executed
  (post-rewrite) plan, and the sort pins re-anchored to execution-layer evidence; then the
  deferred `ParquetReadOptions::file_sort_order` and Iceberg declared-sort-order plumbing.
- **Spill campaign commits 2–3:** R2 expose spill-dir + `max_temp_directory_size`, refuse the
  silent `temp_directory` twin loudly; R3 RAM/cgroup-relative default pool + document the
  `sort_spill_reservation_bytes × target_partitions` non-spillable floor; land the
  `spill_count > 0` regression battery. (Commit 1, FairSpillPool one-truth, landed #143.)
- **TA-PERF measurement gate:** §8 measurements 1–3 (kernel race / many-symbols /
  wide-SELECT cache counters) still un-run; the baseline doc explicitly does NOT fund the
  multi-slot cache or kernel-internal work without them. Measure before building.
- **TA-DISC** (small, doc-only): parity-disclosure rows for the deliberate oracle-over-C
  divergences (TYPPRICE association, LINEARREG ~46,340-period overflow bound, libm bit
  sensitivity, NaN real-param rejection).
- **TA round 2** (kernel sweep + serving-path work on declareSorted): never started; see also
  the unpushed `ad`/`obv` commit in A5.
- **CDL candle family (61 fns):** promote-on-demand only; the Int32-UDF-lane design that
  gates it is fully written. Math-transform (15) + HT (6): recorded permanent no.
- **Profiling unblock** (operator action, not code): `perf_event_paranoid`, flamegraph,
  heaptrack still un-run; the safe-vs-unchecked ceiling verdict (P-3) has no recorded
  artifact — confirm or re-run.
- **Window frame R4 (120 vs 90)**: parked until a DataFusion-compatible seam appears; do not
  bump deps for it.

### A8. Fork track (owned iceberg-rust)

- **Upstream reconciliation (the +355/+356 merge):** a fully worked six-stage plan exists
  (vendor-only → catalog/storage → read-path → datafusion-integration → writer/transaction →
  encryption/Variant/OpenDAL deferred), plan-only, execution explicitly its own
  **[OWNER]**-gated campaign. Drift at plan time: ~356 behind / 239 ahead. This gates the
  QueryPlanner unit below and grows daily.
- **QueryPlanner seam (last DELETE-subquery residual):** semantic-plan-level correlated
  /uncorrelated distinction to narrow the remaining valve; sequenced AFTER the upstream-merge
  decision (the API is a semver surface).
- **Java `iceberg-core` test-battery port:** increment 1 landed (fork #196); later increments
  unscheduled.
- Partitioning-unification campaign: **fully landed** (fork #200–#206) including the
  priority-1 positional partition-tuple corruption fix; only recorded residue is the
  accepted-finer position-delete-rewrite grouping (documented, by design).
- Fork maintenance actions (orphan files, position-delete rewrite): shipped fork-side; the
  engine-side wiring is the MW campaign (A2).

### A9. Validation & documentation workstreams (owner roadmap additions of 2026-08-16)

Datasets workstream (a) fully landed (#153/#158/#161/#163). Still queued:
- **(b) Iceberg statement census** — DML/DDL/system-op inventory vs PySpark → gap rows.
- **(c) examples-doc harness** — executable examples with drift as a red gate (the examples
  directory currently declares execution gating as "arrives with the examples-harness
  workstream").
- **(d) cross-engine function matrix** — polars/duckdb oracles beside the PySpark oracle.
- Secrets-flagging feature (opt-in conf) — needs a design note before any unit.

### A10. Release / packaging

- **crates.io publishing: structurally deferred** — the `[patch.crates-io]` fork pin cannot
  ship in published crates; unblock = publish the fork crates under owned names. PyPI wheel
  publishing is live and proven (seven tags).
- Release runbooks now exist in-repo (`.agent/skills/`); keep them in lockstep with
  `docs/release.md` (whose "phase 0 / nothing wired" opening is stale — truth-up pending).

### A12. Format-v3 and deletion vectors — **owner-scheduled 2026-08-21**

Promoted out of "watch, do not schedule" after MW-2 found the engine reports a **silent no-op** on
a v3 table: `rewrite_position_delete_files` returned four zeros where every delete file stayed
put. MW-2 shipped a loud refusal (queued divergence `B-MOR-3`), which closes the safety hole and
leaves the capability gap.

**The fork is far ahead of the engine here, and that is the headline.** Read at pin `0c5fd58`,
the fork already ships deletion-vector **read and write** — `delete_vector.rs`, a full `puffin/`
module (blob / metadata / reader / writer), `writer/base_writer/deletion_vector_writer.rs` with
`DVFileWriter::{with_previous_deletes, delete, close_with_result}`, DV application on scan through
`arrow/delete_filter.rs` + `caching_delete_file_loader.rs` — plus row-lineage fields in the spec
and the v3 types (`Variant`, `TimestampNs`, `Unknown`). So this track is **mostly engine-side
wiring**, not a fork campaign. That is the opposite of the assumption behind the old "watch" line,
and it is why the item moved.

**Where the engine stands today**, measured rather than assumed:

| Surface | State | Site |
|---|---|---|
| `CREATE TABLE … 'format-version' = '3'` | Refuses loudly | `create_table.rs:158`, `ctas.rs:153` |
| Merge-on-read `MERGE` on v3 | Refuses loudly | `merge/mod.rs` `resolve_merge_mode` |
| Merge-on-read `DELETE` / `UPDATE` on v3 | Refuses loudly | `predicate_dml.rs:835` |
| `rewrite_position_delete_files` on v3 | **Refuses loudly (MW-2)** | `call.rs` deletion-vector guard |
| **Reading** a foreign v3 table | **Not gated — unverified** | no format-version check on the read path |
| v3 types (`variant`, `timestamp_ns`, `unknown`, geo) | Not wired | — |

The read path is the one line above with no refusal and no evidence, and it is the first thing
V3-1 settles.

**Proposed units.** Sequenced so the first one is also the fixture every later one needs.

- **V3-1 — read a v3 table, and build the cross-engine fixture.** A v3 table with Puffin deletion
  vectors, written by the Spark 4.0.1 oracle, read back by this engine with the vectors applied
  and the row set proven against Spark's. Blocked on a way to address a foreign table at all:
  this engine has no Hadoop-catalog surface, so the fixture needs either a catalog shim or an
  adopt-existing-table path — **that decision is V3-1's first question, not an implementation
  detail.** Lands the pin that promotes `B-MOR-3` to a row, and unblocks everything below.
- **V3-2 — create v3 tables.** Lift the CREATE/CTAS refusal behind an explicit opt-in; the
  default stays v2 until V3-3 lands, because a v3 table this engine cannot do row-level writes on
  is a trap.
- **V3-3 — merge-on-read writes on v3.** The big one. Swap the position-delete writer for the
  fork's `DVFileWriter` when the table is v3, and lift the three write-side refusals together —
  `MERGE`, `DELETE`, `UPDATE`. Everything downstream of it (`row_delta`, validation) is fork-side
  and already there.
- **V3-4 — row lineage.** v3 **mandates** it (`_row_id`, `_last_updated_sequence_number`). Needs
  its own scope pass: it is a per-row obligation on every write path, not a column to add.
- **V3-5 — v3 maintenance.** Note the MW-2 refusal may be **permanent and correct**: a deletion
  vector is file-scoped and is never bin-packed, so `rewrite_position_delete_files` has nothing to
  do on a DV table by design — Spark's own answer there is zeros. What v3 needs instead is
  DV-shaped maintenance, and the fork's `remove_dangling_delete_files` is the surface. Do not
  scope this as "make the MW-2 refusal go away".
- **V3-6 — v3 types.** `variant` overlaps the already-ratified **H6 VARIANT** item in A3, which
  names Iceberg V3 variant as a later fork-gated increment; `timestamp_ns` overlaps the landed
  ANSI nanosecond-timestamp refusal (A11 branch). Reconcile with those rather than duplicating.

**Sequencing against MW.** V3-1 can run any time — it touches no MW surface and produces the pin
MW-2 could not. V3-2 and later want MW closed first: MW-4's live acceptance is the campaign's only
real-catalog evidence, and adding a second format version underneath it before it runs would mean
proving two things at once.

**One decision for the owner, and it is not urgent.** MW-2's refusal is **stricter than Spark**,
which returns zeros. That is the same shape as OD-2's dry-run default and was taken on the same
reasoning, but it is a drop-in break: a Glue job calling this on a v3 table gets an error where
Spark gave it a useless success. Reversible in one line if you would rather match Spark. See
`B-MOR-3`.

### A13. The shared CTAS fallback root — **surfaced by MW-3, 2026-08-21**

`register_memory_catalog(name, warehouse)` accepts a warehouse and, for a namespace created
without a `location` property, does not write there. Tables land at
`<std::env::temp_dir()>/repark_ctas/<catalog>/<namespace>/<table>` via the E-4 fallback
(`ctas.rs::resolve_create_location`). Reproduced outside the test harness: the supplied warehouse
stayed **empty** while both CTAS and `CREATE` + `INSERT` wrote to the shared root.

This is the documented fallback, not a defect, and it is not being called one here. Three
consequences are worth a decision rather than a shrug:

1. **The path is keyed by names alone.** Nothing process-specific is in it, so two independent
   sessions using `mem.ns.events` share one directory on the machine. In this repo's own test
   tree that directory had grown to **2.7 GB** across 100+ table names, and one table's directory
   held 139,179 files.
2. **A warehouse argument that is silently ignored is a surprising API.** The caller passed a
   path; nothing tells them it was not used. The `RequireExplicitLocation` error message already
   coaches callers to create the namespace with a location "so RePark writes to the intended
   warehouse instead of a temporary directory" — the local catalog just never says it.
3. **It is the one place a maintenance procedure can destroy another session's data.** MW-3's
   `remove_orphan_files` now REFUSES a table under that root, because orphan removal subtracts one
   table's reachable set from a directory listing and in a shared directory another session's live
   files look exactly like orphans. That guard is a fence around the symptom.

**Options, none of them scoped yet:** default the fallback under the supplied warehouse rather than
the process temp dir; make the fallback per-session (a run id in the path); warn once when the
fallback fires; or leave it and keep the MW-3 fence. The first is the smallest change that removes
the sharing, and it also makes the warehouse argument mean what it appears to mean.

Write-path work, so deliberately outside the MW campaign.

### A11. Strategy horizon (context for the owner only — NOT for the public repo)

An enterprise-platform strategy memo and a competitive analysis exist as owner-side documents
(2026-07-31/08-01, marked not-for-distribution). The roadmap may carry a neutral "platform
direction under evaluation" line at most; do not import their content, framing, or names into
the public repo.

---

## B. CLOSED — every other uncommitted brief, accounted for (do not re-add)

Evidence: merged-PR mapping. Format: brief → landing.

- **Overnight/day conductor waves 2–15** (G-sweeps, H-2 corpus, O/X/Y/Z/W/V/S/R waves,
  MG-1/MG-2, M-audit fixes, FN-A..F, TA-VOL, DS-1..4, BH-1/AL-1, spill c1, M11, splits):
  all landed — repark #47–#165 span. Per-unit mapping lives with the campaigns' own ledgers
  in this directory and under [../docs/history/](../docs/history/map.md).
- **Conductor-16 fork partitioning campaign** → fork #200–#206 (complete).
- **Conductor-17 explode case-loss** → #154; **conductor-18 datasets** → #153/#158/#161/#163;
  **conductor-19 bench+allocator** → #155/#159/#162 (mimalloc wired on measured win).
- **Conductor-26 GT1-FIX** → #180 + #181 (4 SQM rounds; residuals in A4).
- **Fork FW wave** (timestamptz projection, Arrow tz=UTC, metadata-provider parity, provider
  walk scope) → fork #192–#195; **13F residuals** → fork #196–#198; **repin** → the current
  pin via #104 (its two predecessor PRs closed unmerged, content folded in).
- **FN function batches A–F** → #108/#110/#115/#119/#122/#125; **FN-GX/GT1/GT2** →
  #160/#172/#174; **FNP tranche 1 + fix rounds** → #190–#193.
- **B4 smartCsv delimiter** → #175 (owner-ratified descope-and-salvage); **DF-2 dynamicFlatten
  outer-explode** → #176; **dynamicFlatten native rewrite** → #183.
- **v0.4.0 / v0.5.0 releases** → #182 + tags; PyPI live.
- **Planning-only briefs that produced their deliverable** (recon manifests, design notes,
  lane plans, upstream-merge plan, dbt lane plan, sketches/variant/geo designs): complete AS
  designs; their implementations are the A3/A8 items.
- **Dissolved without need:** the dbt import-move (zero old-era spellings existed); the S/R
  landing sweep lane (re-homed orchestrator-side — see C below).
- **Planning-space documents:** fork-repin brief (superseded — pin has moved twice since); agent-agnostic
  repository proposal (campaign closed, archived in-repo); SEPMO process suggestion memos
  (process, not product); TA/architecture research notes (absorbed into the perf campaign
  inputs); the three independent MOR evaluations (superseded by the reconciled scope in A2).

## C. UNKNOWN — verify before the roadmap asserts anything

1. **P-3 profiling/unsafe-ceiling verdict** — no recorded artifact found either way.
2. **S/R-wave registry landing absorption** — whether the orchestrator-absorbed sweep fully
   shipped; check the registry's history around 2026-08-14/15 for the four S-wave ledger
   citations.
3. **Old FN-2 GT3/GA/G1–G6 queue vs FNP absorption** — needs the name-by-name reconciliation
   (A3 first bullet).
4. **Third nightly-red failure** — unidentified beyond the two in A4.
5. **Carve-out registry payloads** (reflect/java_method/try_reflect/unwrap_udt) — written or
   not.

---
---

*Intake close: eleven open workstreams (A1–A11), one closed ledger (B), five verification items
(C). The MW campaign (A2) is chartered and starts immediately; everything else stays queued until
the owner opens it.*
