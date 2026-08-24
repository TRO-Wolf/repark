# MW-6 — `CALL system.rewrite_manifests`

**Date:** 2026-08-23 · **Branch:** `feat/mw6-wave` · **Base:** `c8553d3` (`main`, post-#228) ·
**Charter:** owner, 2026-08-23 (slate:
[../../../briefs/next-sequence.md](../../../briefs/next-sequence.md) "MW-6"; evidence:
[../../roadmap/mid-term/roadmap-intake-2026-08-23.md](../../roadmap/mid-term/roadmap-intake-2026-08-23.md)
row MW-6) · **Fork pin:** `5e7b2e4` (RP-1,
[rp-1-fork-repin-ledger.md](rp-1-fork-repin-ledger.md))

**Retires:** moved to `completed/` in this departure commit.

Engine-only. `rewrite_manifests` is the sixth maintenance procedure and the first one whose
counts the fork action does not return. Every number below is measured — on the live Spark 4.0.1
+ Iceberg 1.10.0 oracle, in the Iceberg 1.10.0 jar's bytecode, or in the fork source at the pin.

## 1. Where each fact was read

| Fact | Read at |
|---|---|
| Result schema (2 × `IntegerType`, `nullable = false`) | jar `RewriteManifestsProcedure.OUTPUT_TYPE` static initializer (`javap -p -c`, zulu-17) |
| Argument list (`table` STRING required, `use_caching` BOOLEAN, `spec_id` INTEGER) | jar `RewriteManifestsProcedure.PARAMETERS` |
| The two counts are `Iterables.size(result.rewrittenManifests())` / `addedManifests()` | jar `RewriteManifestsProcedure.toOutputRows` |
| Spark's per-leg no-op rule + current-spec filter | jar `RewriteManifestsSparkAction.rewriteManifests` / `findMatchingManifests` / `lambda$findMatchingManifests$5` |
| Summary keys `manifests-created` / `manifests-kept` / `manifests-replaced` / `entries-processed` | fork `crates/iceberg/src/transaction/rewrite_manifests.rs:349-361` at `5e7b2e4` (`~/.cargo/git/checkouts/iceberg-rust-*/5e7b2e4`) |
| Delete manifests are never re-clustered | same file, `perform_rewrite` + module doc "Delete manifests are immune" |
| No-current-snapshot is `DataInvalid`, not a panic | same file, `TransactionAction::commit` |

The jar is `iceberg-spark-runtime-4.0_2.13-1.10.0.jar`; the oracle is the `spark40-oracle`
virtualenv (PySpark 4.0.1) that MW-1 stood up. The pinned 4.1.2 oracle still cannot execute
Iceberg maintenance procedures (the `DataSourceV2Relation` break recorded under `MOR-1`), so
4.0.1 is the oracle, as it was for MW-1/MW-2/MW-3.

## 2. What the oracle answered (2026-08-23)

Every row below is one `CALL ice.system.rewrite_manifests(…)` on a Hadoop-catalog table:

| Fixture | Spark result | Spark's new snapshot summary |
|---|---|---|
| 5 data manifests, unpartitioned | `5, 1` | `manifests-replaced=5 created=1 kept=0` |
| the same table immediately again | `0, 0` | no snapshot committed |
| 6 data manifests, partitioned by `grp` | `6, 1` | `replaced=6 created=1 kept=0` |
| table with no snapshot | `0, 0` | no snapshot committed |
| 2 spec-0 + 3 spec-1 manifests (spec evolved) | `3, 1` | `replaced=3 created=1 kept=1` |
| the same table, `spec_id => 0` | `0, 0` | — (one matching manifest is already at target) |
| `spec_id => 7` (no such spec) | `IllegalArgumentException: Invalid spec id 7` | — |
| 4 manifests, `use_caching => false` | `4, 1` | — |
| 5 manifests, `use_caching => true` | `5, 1` | — |
| unknown argument `bogus` | `AnalysisException [UNRECOGNIZED_PARAMETER_NAME] … [table, spec_id, use_caching]` | — |
| 1 data + 1 delete manifest | `0, 0` | no snapshot committed |
| 1 data + 2 delete manifests | `2, 1` | `replaced=2 created=1 kept=1` |
| 5 data + 3 delete manifests | `8, 2` | `replaced=8 created=2 kept=0` |
| 5 manifests, `commit.manifest.target-size-bytes = 4096` | `5, 5` (manifests stay 5) | `replaced=5 created=5` |
| 12 manifests, same target | `12, 12` (manifests stay 12) | `replaced=12 created=12` |
| `use_caching => 'true'` / `'yes'` / `'no'` (STRING) | `5, 1` each — Spark casts the string | — |
| `use_caching => 1` (INT) | `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] … requires the "BOOLEAN" type, however "1" has the type "INT"` | — |

Result schema, verbatim from the oracle:

```json
{"fields":[{"metadata":{},"name":"rewritten_manifests_count","nullable":false,"type":"integer"},
{"metadata":{},"name":"added_manifests_count","nullable":false,"type":"integer"}],"type":"struct"}
```

The last four rows were measured during the Critic's remediation (2026-08-23, same oracle) and
are registry rows `MANIFEST-3` and `MANIFEST-2`.

Two facts the charter did not have, both measured:

1. **Spark rewrites DELETE manifests in a second leg of the same procedure.** The fork's action
   carries every delete manifest forward byte-identical, by design, so the delete leg has no
   counterpart here. Registry row `MANIFEST-1`.
2. **Spark's default is not "rewrite everything"** — `RewriteManifestsSparkAction` filters
   manifests to the table's CURRENT partition spec, and `spec_id` moves that filter. The fork
   does expose a manifest predicate (`RewriteManifestsAction::rewrite_if`), which the charter's
   "no fork filter" note did not account for. This engine uses it to pin Spark's default and
   still refuses the `spec_id` argument, per the owner's ruling (registry `MANIFEST-2`).

## PROPOSITION LEDGER — MW-6 — 2026-08-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The result is Spark's two columns, in Spark's order, both `int32` and both NON-nullable. | Assert names, Arrow types and nullability on the collected batch, both doors. | PROVEN | jar `OUTPUT_TYPE` (`iconst_0` per `StructField`) + oracle schema JSON above; `call_rewrite_manifests_compacts_like_spark`, `test_rewrite_manifests_compacts_like_spark`. |
| C-002 | The counts come from the NEW snapshot's summary — `manifests-replaced` → `rewritten_manifests_count`, `manifests-created` → `added_manifests_count` — and a missing key errors instead of reporting a fabricated zero. | Read the keys in the fork source at the pin; assert the engine's counts equal Spark's on the same fixture. | PROVEN | fork `rewrite_manifests.rs:349-361`; `summary_count` refuses a missing/unparsable key; `call_rewrite_manifests_compacts_like_spark` gets `5, 1`. |
| C-003 | Five data manifests become one, with Spark's counts and an unchanged live row set, **at the default 8 MB manifest target**. | Count manifests before/after, compare row count across the call; the target-size caveat is C-011. | PROVEN | `(5,0,5)` → `(1,0,1)`, result `5, 1`, rows equal; oracle answered `5, 1` on the same shape. Above the target the added count diverges — C-011 / `MANIFEST-3`. |
| C-004 | A no-op rewrite answers `0, 0` AND commits no snapshot, matching Spark's `targetNumManifests == 1 && matching.size() == 1`. | Re-run on a freshly rewritten table; assert the SEEDING call's non-zero counts first, then zeros and an unchanged snapshot count. | PROVEN | `call_rewrite_manifests_no_op_returns_zeros_and_commits_nothing`, `the_no_op_rule_is_sparks_target_num_manifests_rule`, `test_rewrite_manifests_no_op_returns_zeros`. Provocations: dropping the guard answers `1, 1` (§4 P1); INVERTING it now trips the seeding assert (§4 P5) — before remediation the pin survived that mutation. |
| C-005 | When the data leg has nothing to do and two or more delete manifests remain, the call REFUSES rather than answering zeros. | Build 1 data + 3 delete manifests; assert the refusal names the count. | PROVEN | `call_rewrite_manifests_refuses_zeros_while_delete_manifests_stay`, `zeros_refuse_only_when_spark_would_compact_delete_manifests`. Spark compacts that shape (`2, 1` at two delete manifests), so zeros would be false. |
| C-006 | Only the CURRENT partition spec's manifests are rewritten; older-spec manifests are kept. | Evolve the spec through `ALTER TABLE … ADD PARTITION FIELD`, then assert the counts and the surviving old-spec manifests. | PROVEN | `call_rewrite_manifests_rewrites_only_the_current_spec`: `(3,0,5)` → `(1,0,3)`, result `3, 1`; oracle `3, 1` with `manifests-kept=1`. Provocation: dropping `rewrite_if` answers `5` (§4 P2). |
| C-007 | `spec_id` refuses loud, named or positional. | Assert both spellings error and the message names the argument. | PROVEN | `call_rewrite_manifests_argument_surface_is_sparks`, `test_rewrite_manifests_spec_id_refuses_and_use_caching_is_accepted`. |
| C-008 | `use_caching` is accepted, type-checked as a boolean literal, and changes no count. | Assert a quoted value refuses and `use_caching => true` returns the same counts as the bare call. | PROVEN | same two tests; oracle measured `4, 1` / `5, 1` with the option off and on — a Spark-side DataFrame cache, not a behaviour. The literal rule is STRICTER than Spark, which casts `'true'` / `'yes'` / `'no'` and runs, refusing only a non-castable type (INT → `DATATYPE_MISMATCH`); measured in remediation and disclosed as `MANIFEST-2`, not claimed as parity. |
| C-009 | A table with no current snapshot answers `0, 0` and commits nothing, where the fork action errors. | Call on a freshly created empty table; assert zeros and zero snapshots. | PROVEN | `call_rewrite_manifests_on_a_table_with_no_snapshot_returns_zeros`. Provocation: running the action there fails `DataInvalid => Cannot rewrite manifests: table has no current snapshot` (§4 P3). |
| C-011 | Above `commit.manifest.target-size-bytes` the engine's `added_manifests_count` is its own measured number, disclosed against Spark's rather than asserted equal to it; `rewritten_manifests_count` matches Spark at every size measured. | Pin the ENGINE's counts on a 4 KB-target fixture; register the row with both sides. | PROVEN | `call_rewrite_manifests_added_count_diverges_above_the_target_size` — engine `5, 3` (manifests → 3) where Spark answers `5, 5`; 12 manifests → engine `12, 6`, Spark `12, 12`. Registry `MANIFEST-3`. |
| C-010 | The delete-manifest gap is DISCLOSED with both sides measured, not absorbed: the engine reports its data leg and leaves delete manifests in place. | Registry row with the oracle numbers; a pin on the engine's side of it. | PROVEN | registry `MANIFEST-1`; `call_rewrite_manifests_reports_the_data_leg_and_leaves_delete_manifests` — `(4,3,7)` → `(1,3,4)`, result `4, 1`, where Spark answers `8, 2` on its own 5+3 shape. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 11/11.

```yaml
KILLED_ASSUMPTIONS:
  - "The fork action has no manifest filter": REMOVED (RewriteManifestsAction::rewrite_if is a ManifestFile predicate; the engine uses it to pin Java's current-spec default. The spec_id ARGUMENT still refuses, per the owner ruling)
  - "Spark rewrites only data manifests": REMOVED (doExecute runs a DATA leg and a DELETES leg and sums them — measured 8,2 on 5 data + 3 delete manifests)
  - "A no-op is whatever the fork does": REMOVED (Spark returns zeros and commits NOTHING; the fork would rewrite a single manifest into itself and answer 1,1)
  - "The 4.1.2 oracle cannot run this procedure, so the schema must come from the jar": PARTLY REMOVED (the jar constant was read first, then the 4.0.1 oracle EXECUTED the procedure and agreed — the constant is corroboration, not the only source)
  - "Both engines size manifests the same way because they read the same property": REMOVED (remediation, F-MW6-1: Java repartitions into ceil(total/target); the fork rolls on a running estimate. Equal at the 8 MB default, different above it — MANIFEST-3)
  - "A non-boolean use_caching refuses on Spark too": REMOVED (remediation, F-MW6-2: Spark casts 'true'/'yes'/'no' and runs; only a non-castable type refuses. The stricter rule is kept and disclosed, not claimed as parity)
RISK_HEATMAP:
  - risk: the counts are read from a snapshot summary the fork could stop writing, and a missing key would silently become 0
    severity_if_realized: S1
    mitigation: C-002 — summary_count errors instead of defaulting
  - risk: a merge-on-read table answers zeros while delete manifests pile up, reading as "already clean"
    severity_if_realized: S1
    mitigation: C-005 refusal + registry MANIFEST-1
  - risk: an evolved-spec table rewrites manifests Spark keeps, so counts and layout diverge silently
    severity_if_realized: S2
    mitigation: C-006 rewrite_if + its provocation
  - risk: the procedure commits a snapshot on every call, growing metadata for no work
    severity_if_realized: S2
    mitigation: C-004 no-op guard (no commit) + C-009
  - risk: added_manifests_count is read as Spark's number on a large table and a migrating job's assertion breaks
    severity_if_realized: S2
    mitigation: C-011 + registry MANIFEST-3 (disclosure; the rewritten count and the row set are unaffected)
CLARIFYING_QUESTIONS:
  - "The charter says tests carry `pins: mw-6/C-NNN`, but the grammar gate keys a citation on the ledger's filename, so the citations here read `mw-6-rewrite-manifests/C-NNN`. Renaming the ledger to `mw-6-ledger.md` would restore the shorter spelling."
  - "The charter says the Actor files no COVERAGE_ATTESTATION, while `check_ledger_grammar.py` requires one on any ledger whose clauses are all PROVEN. This ledger is filed WITHOUT it, so `make check-ledger-grammar` reds on exactly that finding until the Critic files it."
```

## 3. What the engine does, and why each guard exists

The fork's action is a `TransactionAction`, so the engine composes it and reads the committed
snapshot rather than a result object:

1. no current snapshot → zeros (Spark's answer; the action errors).
2. Spark's data-leg no-op rule → zeros, no commit.
3. a zero answer with ≥ 2 delete manifests → refuse (C-005).
4. otherwise `rewrite_manifests().cluster_by(constant).rewrite_if(current spec)`, commit, then
   read the two summary keys.

`cluster_by` takes ONE key on purpose: below the manifest target size Java writes one manifest
per matching spec and so does the fork, so one key reproduces Spark's answer on any table that
fits the target — every fixture here bar one, and the 8 MB default. **Above** the target the two
sizings differ (Java repartitions into `ceil(total / target)`; the fork rolls on a running
estimate), which is registry row `MANIFEST-3` and clause C-011 — corrected in remediation, where
this paragraph had claimed the two agree.

The body lives in `crates/repark-spark/src/call/rewrite_manifests.rs` rather than in `call.rs`,
which measures 1,406 lines — 94 under its 1500-line `check_rust_file_size` ceiling (the first
draft of this ledger said 105, which was never the count on the delivered tree).

## 4. Provocation proofs (branch liveness)

Each guard was removed, the pin was watched go RED, and the guard was restored. Nothing below is
committed.

| # | Provocation | Result (verbatim) |
|---|---|---|
| P1 | delete the no-op guard | `call_rewrite_manifests_no_op_returns_zeros_and_commits_nothing` FAILED — `assertion left == right failed  left: 1  right: 0` |
| P2 | delete `.rewrite_if(…)` | `call_rewrite_manifests_rewrites_only_the_current_spec` FAILED — `only the current spec's three manifests are rewritten  left: 5  right: 3` |
| P3 | run the fork action with no snapshot | `call_rewrite_manifests_on_a_table_with_no_snapshot_returns_zeros` FAILED — `rewrite_manifests on an empty table must not error: External(DataInvalid => Cannot rewrite manifests: table has no current snapshot)` |
| P4 | delete the delete-manifest refusal | `call_rewrite_manifests_refuses_zeros_while_delete_manifests_stay` FAILED — `zeros must refuse while Spark would compact the delete manifests` (the call returned `Ok`) |
| P5 | INVERT the no-op guard (`if !is_data_leg_noop(…)`) — the Critic's M1 mutation | after remediation: `call_rewrite_manifests_no_op_returns_zeros_and_commits_nothing` FAILED — `left: 0  right: 5` on the seeding call. Before remediation the same mutation left the pin GREEN, because the inverted guard zeroed the seeding rewrite and the second call then met five manifests and took the inverted early return |

## 5. Lockstep

- Registry: `MANIFEST-1` (delete manifests, BACKLOG — fork work), `MANIFEST-2` (`spec_id`
  refuses, `use_caching` no-op and boolean-literal-only, DECLARED) and `MANIFEST-3`
  (`added_manifests_count` above the manifest target size, BACKLOG — fork work) in
  [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
- Guide: "Compacting manifests" in
  [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md); the
  unsupported-procedure example no longer names `rewrite_manifests`, which now works.
- Maps: [../../../crates/repark-spark/src/call/map.md](../../../crates/repark-spark/src/call/map.md)
  (new), `src/map.md`, `src/router/map.md`, `src/tests/map.md`,
  `python/repark/tests/map.md`.
- STATUS: six maintenance procedures; the two new registry rows; the sequenced remainder is
  MW-7 → MW-8 → V3-2.

## 6. Critic remediation (2026-08-23)

Two S2 findings, two S3 nits and one weak pin, all fixed on this branch; the Critic's own
executions matched the oracle on 15 other shapes, so nothing else moved.

| Finding | What was wrong | What changed |
|---|---|---|
| F-MW6-1 (S2) | `added_manifests_count` diverges above the manifest target size, undisclosed, and the `cluster_by` comment claimed the two sizings agree | registry `MANIFEST-3` (both sides re-measured on the 4.0.1 oracle here: Spark `5,5` and `12,12`), new clause C-011 with the ENGINE's numbers pinned (`5,3` / `12,6`), comment and §3 corrected |
| F-MW6-2 (S2) | the code and a pin comment asserted "a non-boolean refuses on Spark too", which was never measured | measured (Spark casts `'true'`/`'yes'`/`'no'` and runs; INT refuses with `DATATYPE_MISMATCH`), refusal KEPT as the stricter rule, disclosure added to `MANIFEST-2`, both comments and C-008's evidence corrected |
| F-MW6-3 (S3) | ledger said `call.rs` sits 105 lines under its ceiling | measured 1,406 of 1500 → 94 |
| F-MW6-4 (S3) | `call/map.md` said the router kept "five procedures" | six (`grep -c '^async fn execute_' call.rs`) |
| weak pin (M1) | the no-op pin survived an inverted guard, because the seeding rewrite no-opped too | the seeding call's `5, 1` is now asserted before the second call's zeros; the mutation trips it (§4 P5) |
