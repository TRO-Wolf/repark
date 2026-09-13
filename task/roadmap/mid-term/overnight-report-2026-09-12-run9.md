# Overnight report — run 9 of 2026-09-12 (the v1.5 track: facade, silver, perf)

**Session:** one Opus orchestrator (overnight-9), 2026-09-12 18:22 → 2026-09-13 06:30 local; from ~20:30 the box was
shared with orchestrator B (run 9b: CFG-2, DYNCFG-1) and a PLATFORM-1 lane from another session.
**Grants:** G-1 (squash-merge on green), G-2 (bounded decisions), G-3 (stop 06:30 or list exhausted), G-4 **Grok** for
actor rounds launched before 20:40, **Devin SWE-2** for actor rounds launched at or after 20:40, Grok for every S2-21
reviewer, no Muse or GLM; G-5 (seeds a card requires — none were needed). Release pipeline, tags and STATUS.md untouched.
**Procedure:** [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Slates:** [slate 2](cheap-tier-slate-2-2026-09-09.md) S2-21 (perf reviewers) and S2-26 (PERF-UNPIVOT-1, PERF-CAST-1);
[facade audit §7–§8](../epic-term/facade-audit-2026-09-10.md); [silver compiler §17–§18](../epic-term/deterministic-silver-layer-compiler-2026-09-12.md).

## 1. What landed

| Scope | Unit / step | PR | Merged | Rounds |
|---|---|---|---|---|
| 1 | FACADE-2 step 1 — 221 byte-identical `Column` display goldens, `isinstance` pin | #544 | `dbdc8b0d` | Grok 1 |
| 1 | FACADE-2 step 2 — Group-2 `Column` display strings render in Rust (54 Python assembly sites gone) | #549 | `23bd047b` | Grok 1, Devin 1, Grok review + re-check |
| 1 | FACADE-2 step 3 — Group-1 typed native constructors, generic `name(args)` render in Rust; `F.lit(datetime)` −97 % | #557 | `37541c1a` | Devin 3 (one OOM-killed), Grok review |
| 2 | Silver S-0 — contract and storage feasibility (reading ledger, ten fork probes) | #543 | `d5c36c98` | Grok 1 |
| 3 | Silver S-1 — typed `SilverPlan` in `repark-core` (strict TOML, canonical identity, deterministic explain) | #546 | `65202f25` | Grok 2, Grok review |
| 4 | PERF-UNPIVOT-1 step 2 — `describe` / `summary` on a pure plan; 500-column describe 10.19 s → 8.2 s | #553 | `65abfd46` | Devin 2 (+1 launcher slip), Grok review + re-check |
| 5 | PERF-CAST-1 step 2 — release re-measure; owner is stock DataFusion `SqlToRel` → apache/datafusion#25248 | #550 | `c7fb4ce5` | Devin 1 |
| + | FACADE-3 step 1 — `createDataFrame` release baseline per shape, 156 goldens, pickle pin | #555 | `927fa4d3` | Devin 1 |
| + | FACADE-3 step 2 — rows / tuple / dict inference in Rust (nested −92.7 %, explicit schema −78 %) | #559 | merge chain in flight at report time (rebased onto `main`, gated green) | Devin 1, Grok review |

**Complete tonight:** FACADE-2 (all three steps), Silver S-0 and S-1, PERF-UNPIVOT-1, PERF-CAST-1. Every merge went
through update-branch → checks → squash `--match-head-commit` → tree equality; every product PR had its S2-21 review.
Fork evidence branch: `probe/silver-s0` @ `ac2d6ab8` on TRO-Wolf/iceberg-rust (no PR).

## 2. Findings worth the owner's eye

1. **P1 defect on `main`: nesting `F.abs` (and `F.cbrt`) is exponential in native memory.** `abs` is built facade-side
   as `when(c < 0, 0 - c).otherwise(c)`, embedding its child three times per level: an `F.abs` chain holds ~76 MB to
   depth 10, then 705 MB at 12, 1 963 MB at 13, 5 742 MB at 14 and an aborting allocation failure; uncapped it reached
   84 GB and triggered a **global OOM kill** on this box (INC-R9-1, §6). `+ 1`, `cast`, `F.sqrt` chains stay flat through
   depth 40. Candidate card **ABS-EXPR-1** (§5); owner question Q-R9-3.
2. **SIL-4 is implementable at the fork pin — but not through RePark's default overwrite.** `Transaction::commit`
   refreshes and re-applies on a moved head, so an overwrite without `validate_no_conflicting_data` silently deletes a
   concurrent append, and `commit.retry.num-retries=0` does not stop that rebase; RePark's default overwrite isolation
   (`snapshot`) does not refuse it either. With validation armed the conflict refuses as non-retryable `DataInvalid`.
3. **PERF-CAST-1 is upstream.** The wide-aggregate plan stays superlinear on release (exponent 1.511); stock DataFusion
   `SqlToRel::statement_to_plan` owns 96 % of the wall (exponent 1.81) on both a stock context and a RePark session,
   while `DataFrame::aggregate` builds the same plan in 0.012 s. Filed as
   [apache/datafusion#25248](https://github.com/apache/datafusion/issues/25248); the strict-xfail pin flips when a fix lands.
4. **Actor measurements needed independent re-checks twice.** FACADE-2 step 2's actor reported −34 % where the
   stationary release pass was +2.77 % (a drifting base worker), and PERF-UNPIVOT-1's first pure plan was +27 % slower
   than the bridge it replaced until the review located the 2 505-expression projection. Both were caught before merge.
5. **Parity gaps pinned as-is by FACADE-3 step 1:** `createDataFrame(verifySchema=…, samplingRatio=…)` raise a bare
   `TypeError`; inferred binary columns show as `string` in `simpleString()`; a decimal at the envelope edge raises a bare
   `decimal.InvalidOperation` (OBS-R9-3..5).

## 3. Decisions taken under G-2

- R9-D-1 FACADE-2 has no slate card; the card is assembled from audit §4/§7/§8 into three steps (pins,
  Group 2, Group 1 + generic builders), one PR each. Step 1 is pins-only (no S2-21 review: no product diff).
- R9-D-2 Silver S-0 runs as a third lane beside two Grok actor lanes: it is a reading unit with no product
  code (Home = one RePark ledger + an uncommitted-to-RePark fork probe clone), disjoint from both other
  Homes; its cargo invocations wait on the two-builder cap.
- R9-D-3 S-0's fork-side evidence (ten probe tests) is pushed to the fork as branch `probe/silver-s0`
  (`ac2d6ab8`), no PR, so the ledger's citations outlive the `/tmp` clone.
- R9-D-4 FACADE-2 step 2 starts from the step-1 commit in the same warm clone while step 1's PR merges
  (launch before 20:40 keeps it on Grok); the branch is rebased onto `main` after the squash.
- R9-D-5 S-1's module seat is `repark-core/src/silver/` (tier 2; DAG check), authoring format TOML (existing
  dependency), canonical bytes as identity with no digest (no dependency seed) — Q-R9-1/Q-R9-2.

- R9-D-6 FACADE-2 step 2 C-011 (the card's 5 % chain threshold, set by this orchestrator): a debug-build
  microbench is not a ranking measurement (perf-unit-briefs rule); the S2-21 reviewer re-measures both chains
  on release natives of `524e9edc` and `22c33fe1`; ≤ 5 % on release closes C-011, above it is a P2 for the actor.

- R9-D-7 Shared box from ~20:30: orchestrator B (run 9b: CFG-2, DYNCFG-1; `b-` lanes, Devin actors) and a
  third lane `dv-platform1` (PLATFORM-1, `.github/` Home, launched 20:34 by another session) run beside run 9.
  No Home overlaps any run-9 lane; run 9 never touches those clones; builder-idle waits and merge races are
  expected, map-row conflicts are union-resolved by the merge helper, non-map conflicts park.

- R9-D-8 PERF-UNPIVOT-1 step 2: the card said the PERF-DESCRIBE-1 suite passes unchanged, but one of its pins
  asserted the bridge exists (`_map_bridge is not None`) and the per-aggregate cast inventory — both contradicted
  by the card's own "no mapInArrow bridge" deliverable. Accepted the rewrite of those mechanism assertions only;
  every accepted-answer assertion is byte-identical (ledger C-009 note).

- R9-D-9 PERF-CAST-1 step 2 skips the S2-21 perf review: its only `crates/` file is an `#[ignore]`d measurement
  test (S2-21 applies to diffs beyond pins), and the verdict is upstream with no engine change.
- R9-D-10 The upstream DataFusion issue is filed by the orchestrator (scope item 5 "file upstream per the card"):
  duplicate search empty, text scanned for private paths and names, filed as apache/datafusion#25248; the perf
  document links it in an orchestrator commit before the PR.

- R9-D-11 PERF-UNPIVOT-1 step 2 remediation adds `row_labels` / `cell_indices` keyword parameters to the internal
  `_native.stack_dataframe` binding (not a Spark name, not in the freeze register, no new public Python name);
  accepted under G-2. The remediation's new Rust in the exec hot path gets a second S2-21 Rust re-check before the PR.

- R9-D-12 FACADE-3 opens at 23:26 rather than after the list is exhausted: scope items 2–5 are all at or past
  their last actor round (only reviews and merges remain), one worker lane was idle, and FACADE-3's Home (the
  `createDataFrame` modules) shares no file with FACADE-2 step 3's (`column.py`, `functions*`). Its card is assembled
  from audit §6–§8 in three steps (measure + goldens, the Rust move on the measured walls, the remainder), one PR each.

- R9-D-13 With three sessions merging, `main` can move during a 25-minute gate run (PERF-UNPIVOT-1 step 2 went
  BEHIND after its gates). The orchestrator pushes exactly the commit its local gates passed and brings the PR up to
  date with GitHub's update-branch inside the merge chain, where CI gates the merge commit before the squash; it does
  not loop merge-main → re-gate. The tree-equality check after the squash still binds.

- R9-D-14 FACADE-2 step 3's P2-1 remediation ships without a second S2-21 re-check: the change only removes an
  `Expr` clone and a list pass, goldens are byte-identical, and the reviewer had already confirmed the card's ≤ 5 % bar
  on the pre-fix commit; the actor's −17.5 % is reported as actor-measured.

- R9-D-15 PR #557 conflicted with `main` on `python/repark/tests/map.md`. A merge commit cannot land in this repo
  when `main` brought changes to other directories: the map-lockstep pre-commit hook rejects it
  (`python/repark-parity/tests/map.md was not updated in this commit`) and `--no-verify` is forbidden. The
  orchestrator rebases the unit branch onto `main` instead (replayed commits already passed the hook), union-resolves
  the map rows, re-runs the full gates, and updates the PR with `--force-with-lease` pinned to the exact remote head it
  had verified — its own unit branch, no other writer. Check after the rebase: the union lost no row (branch vs its
  rebase base `927fa4d3` is +9 / −1, the −1 being the extended `pins:` line); a CFG-2 block that looked missing against
  a later `main` came from #556 (orchestrator B), merged after the rebase base, and returns through the chain's
  server-side update-branch.

- R9-D-16 Memory-cap rule, refined: the INC-R9-1 address-space cap (`prlimit --as=8G`) aborts the polars control
  (polars/jemalloc and a session reserve ~13 GB of address space without using it). For that control only, the S2-21
  reviewer runs under a real-memory cgroup limit (`systemd-run --scope -p MemoryMax=8G -p MemorySwapMax=0`), which
  bounds what the rule exists to bound; every other worker keeps the address-space cap.

- R9-D-17 FACADE-3 step 2's remediation is split by the stop time: the P1 (a refusal-path regression against `main`)
  is mandatory before merge, with F-STRCOPY and F-DECIMAL if they fit; F-FUNNEL (pass `Row` / dict lists into native)
  and F-TIMETUPLE move to step 3's target list. The reviewer's F-TIMETUPLE suggestion (PyO3 `PyDateTime` getters) is
  not available under the abi3 limited API the wheel ships, so step 3 must find another route. If the round cannot be
  gated and merged by ~06:00 the PR opens as a draft.

- R9-D-18 A Devin ledger-only commit (`c0f48c07`) lacked the required `Authored-By` trailer. It was the unpushed tip of
  its branch, so the orchestrator amended the message only (trailer added, tree identical, a new commit id)
  rather than re-running the round; the running gates stayed valid for the unchanged tree.

## 4. SIL-1..SIL-10 recommendations and parked owner questions

Source: `task/ledgers/staging/silver-s0-ledger.md` (11 clauses PROVEN; ten fork probe tests on the memory
catalog at pin `3ebf7d36`, fork branch `probe/silver-s0`). These are RECOMMENDATIONS; every SIL decision is
the owner's.

**Headline for SIL-4 (the storage question): implementable at the current pin with no fork change, but not
through RePark's default overwrite.** Staging `DataFile`s without a commit works; one always-true overwrite
replaces the table in one snapshot; a run key rides that snapshot's summary and is findable from metadata.
Two traps measured: (1) `Transaction::commit` refreshes and re-applies on a moved head, so an overwrite
without `validate_no_conflicting_data` silently deletes a concurrent append (`deleted-data-files=2`), and
`commit.retry.num-retries=0` does not stop that first rebase; (2) RePark's default overwrite isolation is
`snapshot`, which does not refuse a concurrent append. With validation armed, the conflict refuses as
non-retryable `DataInvalid`. A lost response is `CommitStateUnknown` on Glue / S3 Tables / REST, and the
fork reconciles it by searching the reloaded snapshot set.

| ID | Recommendation (S-0) | Owner decides |
|---|---|---|
| SIL-1 | MVP types int32, int64, utf8, bool, date32, timestamp-µs UTC, decimal128 (all round-trip Iceberg↔Arrow at the pin); keys/order int32/int64/utf8 (bytewise); ops `Trim` (`btrim(col,' ')`), `EmptyToNull` (`nullif`), `RequireNonNull`, `CheckAllowedValues` (`in_list`) exist; `ParseTimestamp iso8601_seconds_offset_v1` is MISSING (no format parse with a success flag) → out of the first matrix until S-2 builds the kernel. | The bronze table; decimal cap; whether ParseTimestamp waits for S-2. |
| SIL-2 | Latest-source-version; an invalid latest quarantines, no fallback. | Accept the default. |
| SIL-3 | Fail on conflicting greatest-version ties and unorderable keyed history; identical repeats → lowest bronze record id. | Canonical payload equality. |
| SIL-4 | Publish in `crates/repark-iceberg/src/write/` beside `overwrite_commit.rs`: hold the expected-base `Table`, stage, gate, then `overwrite_by_row_filter(AlwaysTrue).add_files(staged).set_snapshot_properties(run_key).validate_no_conflicting_data().validate_from_snapshot(base)`; on `CommitStateUnknown` walk snapshots for the run key, never retry a replace. Fork cards F-SILVER-PIN-BASE / F-SILVER-NO-REBASE optional. | Opt-in validation enough vs a first-class expected-base in the fork; the run-key property name. |
| SIL-5 | One classified table; the committed snapshot id is the read handle. | How plain SQL consumers bind a run. |
| SIL-6 | A summary-only marker dies with its snapshot on expire → retention must cover the replay window, or dual-write the run key into table properties in the same transaction. | Windows; dual-write. |
| SIL-7 | Gates run on staged files before commit; a failed gate leaves the last accepted snapshot current. | Denominators, thresholds, empty input, evidence loss. |
| SIL-8 | Plan/policy/compile in `repark-core` (tier 2); publish adapter in `repark-iceberg` write; no new crate. (S-1 was placed in `repark-core/src/silver/` tonight on that basis.) | Module vs later crate split. |
| SIL-9 | No target asserted; set methodology first. | Budgets, corpus. |
| SIL-10 | Fixed output contract per accepted plan; breaking change = versioned target or reviewed migration. | Which. |

### Parked owner questions (§4)

- Q-R9-1 (S-1): a cryptographic plan-identity digest needs a dependency seed (`sha2` is already in
  `Cargo.lock` transitively; adding it to `repark-core` is still a dependency change) — S-1 ships canonical
  bytes as the identity, digest deferred.
- Q-R9-3 (OBS-R9-6, P1): open ABS-EXPR-1 now (a nested `F.abs` chain aborts the process at depth 14 and can
  OOM the machine) ahead of FACADE-3 steps 2–3, or after?
- Q-R9-2 (S-1): the S-1 authoring format is TOML (the crate's existing parser) with §8's field names; the
  owner may prefer YAML/JSON for S-6's public door.

## 5. Next

- **FACADE-3 step 3** — whatever step 2 leaves above the bar (step 2's table: `Row` and dicts still ~0.6–0.8 s at 1e5).
- **ABS-EXPR-1** (P1, Q-R9-3) — lower `F.abs` to one native call, audit facade-side `when(...)` rewrites that embed their
  child more than once, pin memory linear at depth 40.
- **FACADE-2 step 4** — the remaining Python display assembly inventory (facade-2 ledger C-018).
- **Silver S-2** — batch semantics over bounded Arrow fixtures, after the owner rules SIL-1..SIL-10.
- Residues: PERF-UNPIVOT-1-R-001 (ten round-robin repartitions, ~700 MB spill per 500-column describe).

## 6. Housekeeping

| Tier | Rows | Cost |
|---|---|---|
| Grok actor rounds (launched before 20:40) | 5 — S-0 r1, S-1 r1 + r2, FACADE-2 s1 r1, FACADE-2 s2 r1 | $22.93 |
| Grok S2-21 reviewers and re-checks (all night) | 8 — S-1; FACADE-2 s2 + re-check; PERF-UNPIVOT-1 s2 + re-check; FACADE-2 s3; FACADE-3 s2 + re-check | $4.44 |
| **Grok total** | **13** | **$27.37** |
| Devin SWE-2 rounds (launched at or after 20:40) | 10 rows, 623 agent steps (one launcher slip with 0 steps), plus the OOM-killed FACADE-2 s3 round that left no row | $0 (free tier) |
| Muse / GLM | 0 | — |

### Incidents

- INC-R9-3 (05:11) FACADE-3 step 2's ship pushed its gated tip, then `gh pr create` failed with a transient GitHub
  GraphQL error (`Something went wrong while executing your query`); the script stopped on its head check with no PR
  and nothing merged. A recovery script confirmed the remote head equals the gated tip, created the PR with
  backoff retries, verified the head, and ran the merge chain.
- INC-R9-2 (01:13) PR #555 (FACADE-3 step 1, tests/goldens/docs/ledger only) went red on the wheel smoke job:
  `test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool` — `Resources exhausted: Failed to
  allocate additional 768.0 KB for ExternalSorterMerge[0]` (1 failed, 5 891 passed). The diff cannot reach the spill
  path, the branch passed the same workflow before its update-branch, and `main`'s own wheels run for #553 (the commit
  the update-branch brought in) finished green at 01:18 — a flake, not a regression. The failed job was rerun and the
  merge chain retried. If it recurs on another PR it becomes a flake card (`test_t2_spill_reach` sort-merge-join
  spill under the small fair pool).
- INC-R9-1 (2026-09-13 00:11:35) **global OOM kill.** FACADE-2 step 3's C-017 measurement ran the step-1/main base
  tree on a depth-100 `F.sqrt(F.abs(c) + 1)` chain; that Python process reached 84 GB anonymous RSS (98 GB virtual),
  the kernel killed it (`global_oom`, task memcg `devin-grok-facade2-031217.service`), and systemd failed the whole
  Devin unit (`Failed with result 'oom-kill'`) — the round ended with two commits and uncommitted golden work in the
  lane and no hand-back. Lesson for every measurement brief: run base/branch workers under an address-space cap
  (`prlimit --as=…`) and grow depth stepwise on the base tree.

### Observations filed, not fixed

- **OBS-R9-6 (P1, found by INC-R9-1): nesting `F.abs` is exponential in native memory on `main`.** Measured under
  an 8 GB address-space cap on `main` (FACADE-3 lane, `024776a1` = main + tests) and on the FACADE-2 step-3 branch
  (release): an `F.abs` chain holds ~76 MB to depth 10, then 705 MB at depth 12, 1 963 MB at 13, 5 742 MB at 14 and an
  aborting allocation failure (core dump); `F.abs(c) + 1` the same; `+ 1`, `.cast("double")`, `F.sqrt` chains stay at
  76 MB through depth 40; rendered text stays linear (17 bytes per level). Uncapped, the same chain reached 84 GB and
  a global OOM kill. Cause read from source: `python/repark/src/repark/spark/functions.py:549` builds `abs` facade-side as
  `when(column < 0, lit(0) - column).otherwise(column)` — the child `Expr` is embedded three times per level, so a
  depth-n chain holds 3^n nodes (the measured ×3 per level). Candidate card **ABS-EXPR-1**: lower `F.abs` to one native
  `abs` call (DataFusion `abs`, Spark ANSI overflow semantics checked against the oracle), keep `spark_display`
  `abs(...)`, pin memory linear at depth 40 (red first), keep the `sum(abs(x))` live-oracle name pins; audit the other
  facade-side `when(...)` rewrites that embed their child more than once. FACADE-2 step 3's measurement adds
  `F.cbrt`: 444 MB and a 3.6 M-character display at depth 10 on `main` — same class, fold into the same card. Not fixed tonight: outside every carded scope; owner question Q-R9-3.
- OBS-R9-1 (FACADE-2 step 1): `df.select(col.cast(...)).columns` on a named attribute answers
  `datafusion.public.__repark_cdf_<id>.x` rather than `x`; the goldens store the trailing field. Candidate
  card: the cast output name drops the session-local qualifier (measure against the oracle first).
- OBS-R9-3 (FACADE-3 step 1): `createDataFrame(..., verifySchema=…)` / `samplingRatio=…` raise a bare `TypeError`
  because the facade signature is `(data, schema)` — a Spark signature gap (freeze row A2 names the method, not the
  keywords); pinned as-is. Candidate card: accept both keywords with Spark semantics.
- OBS-R9-4 (FACADE-3 step 1): inferred `bytes` / `bytearray` / `memoryview` columns build a binary Arrow column that
  `schema.simpleString()` shows as `string` (Spark: `binary`); pinned as-is. Candidate card after an oracle measure.
- OBS-R9-5 (FACADE-3 step 1): a `Decimal` at the envelope edge (10^20 − 1) raises a bare `decimal.InvalidOperation`
  from the quantize (Python's 28-digit context) instead of a PySpark error class; pinned as-is.
- OBS-R9-7 (FACADE-3 step 2 re-check): a `createDataFrame` whose FIRST row holds an `object()` cell does not refuse
  on the branch (the native screen declines in 0.02 ms, then Python infers from the first cell and succeeds); a later
  `object()` still raises `PySparkTypeError`. Not compared against `main`; first-row inference behaviour worth one
  oracle measurement (does Spark refuse?).
- OBS-R9-2 (S-0): RePark's default overwrite isolation (`snapshot`) does not refuse a concurrent append —
  correct for Spark parity on ordinary overwrites, a trap for silver publication (SIL-4).

## Pointers
- Up: [map.md](map.md) · Runbook: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- Ledgers: [facade-2](../../ledgers/staging/facade-2-ledger.md), [facade-3](../../ledgers/staging/facade-3-ledger.md), [silver-s0](../../ledgers/staging/silver-s0-ledger.md), [silver-s1](../../ledgers/staging/silver-s1-ledger.md), [perf-unpivot-1](../../ledgers/staging/perf-unpivot-1-ledger.md), [perf-cast-1](../../ledgers/staging/perf-cast-1-ledger.md)
