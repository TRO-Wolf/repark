# Charter ledger — RP-20 · fork pin edc38c6a consumer (F-GLUE-REPLACE-1)

**Date:** 2026-09-14 · **Branch:** `chore/repin-rp-20` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-19-ledger.md](../staging/rp-19-ledger.md) — repin consumer with a registry-truth clause.

**Retires:** the orchestrator moves this ledger to `../completed/` after the post-merge
live run; per the unit brief it stays in `staging/` until then.

**Why now.** The orchestrator bumped the iceberg-rust pin to
`edc38c6aa5cdbe132235f4fefc01d3066d6cff23` (bump commit `f1630f5a` on this branch). The
reason for the bump is `#282` F-GLUE-REPLACE-1: the Glue catalog had no replace-publish
path — `publish_replace_table` was the trait default `FeatureUnsupported` — so a second
`dbt run` on Glue (dbt-spark's `table` materialization emits `create or replace table
… as` when the relation already exists) died at publish after streaming the SELECT
into unreferenced data files (owner's gap G-1,
[docs/cutover/production-iceberg-status-2026-09-14.md](../../../docs/cutover/production-iceberg-status-2026-09-14.md)).
The fork's override is a version-id-checked `UpdateTable` of the metadata location
through the Glue commit transport: the staged metadata is read back and uuid-matched
before the send, a `Some` expected base that differs from the stored pointer is a
retryable `CatalogCommitConflicts` before any send, and a lost response is a typed
`CommitStateUnknown`. `begin_replace` continues the metadata-file version with a fresh
uuid so two staged replaces cannot collide on one `v(N+1)` name. With it, the gold dbt
path's second `dbt run` and the twice-`CREATE OR REPLACE` legs have a publish path on
Glue — the legs themselves are ICE-GOLD-TWICE-1, landed beside this ledger in the same
unit.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` (the orchestrator's bump commit
carries them); STATUS.md; product code — no RePark-side change is needed, the facade's
CTAS/`writeTo().createOrReplace()`/`writeTo().replace()` paths already reach
`begin_replace`/`publish_replace_table`, and the fork gained the Glue override.

## Take / skip — riders on `edc38c6a`

| Fork PR | Ask | Take or skip | What it means for RePark |
|---|---|---|---|
| `#282` | F-GLUE-REPLACE-1 — Glue `publish_replace_table` via `UpdateTable` | **take** (the reason for the bump, the whole bump) | Glue `CREATE OR REPLACE TABLE … AS`, `writeTo().createOrReplace()` and `writeTo().replace()` publish through the Glue commit transport — the second `dbt run` on Glue and the twice-replace leg have a publish path. `CommitStateUnknown` reaches Python typed (the ICE-COMMIT-UNKNOWN-1 contract). |

## PROPOSITION LEDGER — RP-20 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `pin_moved`: the five `rev` lines are byte-identical — `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u` prints exactly one line, `edc38c6aa5cdbe132235f4fefc01d3066d6cff23`; `docs/fork-sync.md` carries the RP-20 pin-history row and the root `map.md` pin sentence names the new sha. | The grep; five Cargo.toml hits; Cargo.lock sources; the fork-sync row; the map.md sentence. | **PROVEN** | `sort -u` prints one line `rev = "edc38c6aa5cdbe132235f4fefc01d3066d6cff23"`. Cargo.toml 5 hits, Cargo.lock 6 sources. Pin-history row 2026-09-14 names F-GLUE-REPLACE-1 `#282`, no riders. Root map.md sentence `**RP-20 (2026-09-14):** \`edc38c6a\`` landed in the orchestrator's bump commit `f1630f5a`. This unit does not edit the pin. |
| C-002 | `registry_inventory_truthful`: every row in `docs/spark-sql-iceberg-parity.md` or `docs/cutover/` that names the Glue replace refusal or `FeatureUnsupported` on a Glue `CREATE OR REPLACE` is stamped FIXED at `edc38c6a`. | The stamped rows; the grep proving the parity registry holds no such row. | **PROVEN** | The parity registry holds no row naming the Glue replace refusal — `grep -niE 'glue.*(or replace\|replace.*publish)\|or replace.*glue' docs/spark-sql-iceberg-parity.md` prints nothing; the `featureunsupported\|replace` grep finds only rows about views, `COMMENT`, `ReplacePartitions`, variant and `position_deletes`. The refusal lived only in `docs/cutover/`, all stamped FIXED 2026-09-14 at `edc38c6a` in this change: production-iceberg-status row C2 (evidence cell carries the FIXED stamp plus the kept pre-fix record), the §Recommendation update, gap-table row G-1, the G-1 section FIXED-at-pin note, canary row C6, row C3's "no live replace cell" qualification, §9 items 8–9 and the §10 addendum update; inventory §8 row 5 and the nightly-coverage note; the cutover `map.md` entry. Red evidence is the fork ledger's base-red quoted in §Red first. |
| C-003 | `gates_green`: the unit's gates pass on this branch — the offline replace pin plus the live-leg module, the dbt suite, the replace/catalog pytest selection, `make workflows-lint`, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, `make verify`, the whole parity suite, and the staged comment fence. | Counts pasted into §Gates. | **PROVEN** | Every listed gate green (counts in §Gates; the live legs skip without `REPARK_AWS_ACCEPTANCE`, counts asserted in the ICE-GOLD-TWICE-1 ledger). `make workflows-lint` is green on the orchestrator's online re-run of `46ddae52` — exit 0, zizmor "No findings to report. Good job! (19 suppressed)"; the sandbox's own run (workflows-parse + `zizmor --offline` clean, online `artipacked` unavailable — 401, tokens withheld) is recorded separately in §Gates. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

The pin consumption is a registry-and-workflow unit; the red it turns green was
measured on the fork at this branch's exact prior pin (`3ebf7d36`) — the fork ledger
(`task/f-glue-replace-1-ledger.md` at the pinned checkout, §Base-red evidence) records
`cargo test -p iceberg-catalog-glue --lib replace_publish` on the base tree (pins
added, production code untouched) exiting 101, **8 red out of 8 new pins**, every one
on the trait default `FeatureUnsupported => publish_replace_table is not supported by
this catalog`:

```
running 8 tests
test replace_publish_tests::replace_publish_access_denied_is_terminal ... FAILED
test replace_publish_tests::replace_publish_concurrent_modification_is_retryable_conflict ... FAILED
test replace_publish_tests::replace_publish_accept_then_lose_is_unknown_and_pointer_moved ... FAILED
test replace_publish_tests::replace_publish_unreadable_staged_metadata_refuses_before_send ... FAILED
test replace_publish_tests::replace_publish_foreign_uuid_staged_metadata_refuses_before_send ... FAILED
test replace_publish_tests::replace_publish_maybe_sent_lost_is_unknown_and_keeps_pointer ... FAILED
test replace_publish_tests::replace_publish_stale_base_conflicts_retryable_before_any_send ... FAILED
test replace_publish_tests::staged_replace_commit_swaps_glue_pointer_and_retains_uuid ... FAILED
test result: FAILED. 0 passed; 8 failed; 0 ignored; 0 measured; 41 filtered out
```

Green on the pin, from the same fork ledger: `cargo test -p iceberg-catalog-glue --lib`
49 passed; `iceberg-catalog-s3tables --lib` 39 passed; `iceberg --lib staged` 22
passed; `iceberg --lib` full 3676 passed, 0 failed, 8 ignored; fmt/clippy/taplo/
machete/file-size/comment-fence gates green.

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_acceptance_replace_offline.py python/repark/tests/test_aws_acceptance.py -q -rs` | 0 — 1 passed, 10 skipped in 0.39s (every live leg SKIPPED incl. both `replace2` legs at lines 510/537) |
| `make py-test-dbt` | 0 — 59 passed, 1 skipped in 35.46s (the gold leg is the skip) |
| `.venv/bin/python -m pytest python/repark/tests -q -k "replace or ctas or catalog"` | 0 — 245 passed, 18 skipped, 6185 deselected in 181.49s |
| `make workflows-lint` | 0 — orchestrator's online re-run on `46ddae52`: zizmor "No findings to report. Good job! (19 suppressed)". Sandbox run (this clone): `workflows-parse` green (13 workflows) + `uvx zizmor@1.26.1 --offline .` no findings; the online `artipacked` audit is unavailable here — GH_TOKEN/GITHUB_TOKEN are `disabled`, zizmor gets 401 listing `actions/checkout` tags. |
| `make check-docs-links` | 0 — 849 files, 5428 links checked — clean |
| `make check-ledger-grammar` | 0 — 131 live ledgers clean (904 clauses, 1523 pinned clause ids, 2 exception rows) |
| `make check-ledgers` | 0 — 349 ledgers in bins (218 archived), 962 ledger links resolve, frozen rule clean |
| `make verify` | 0 (fmt, clippy `-D warnings` + panic-ban, crate-dag, lib-rs, rust-file-size, lib-py, python-conventions, docstring-presence, manifest, ledger lifecycle + grammar, docs-compaction, docs-links, owner-ruling, parity-live dual-wire, matrix-test-liveness, cargo check, ruff check + format, all rust tests) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 757 passed, 2 skipped, 12 xfailed in 567.53s |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-20
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 greps all five Cargo.toml rev lines and the six Cargo.lock sources to one sha; C-002 greps the parity registry for the Glue replace refusal (none — the refusal lived only in docs/cutover/) and stamps every cutover row that named it FIXED at edc38c6a; the pinned fork source and ledger were read at the cargo checkout of rev edc38c6, not trusted from the card.
      artifacts: [docs/fork-sync.md, docs/cutover/production-iceberg-status-2026-09-14.md, docs/cutover/inventory.md, docs/cutover/map.md, map.md]
    - id: AT-2
      status: ATTACKED
      evidence: The whole bump is consumed (take/skip names #282 alone); the fork's base-red and post-fix green runs are quoted as evidence, and the consumed behavior is named — version-id-checked UpdateTable, staged metadata read-validated and uuid-matched before the send, retryable CatalogCommitConflicts, typed CommitStateUnknown, fresh-uuid metadata versioning.
      artifacts: [docs/fork-sync.md, task/ledgers/staging/rp-20-ledger.md]
    - id: AT-3
      status: N/A
      justification: No refusal path or error contract changes in RePark this unit; the publish path is a fork capability the repin consumes, and CommitStateUnknown's Python taxonomy is ICE-COMMIT-UNKNOWN-1's prior contract.
    - id: AT-4
      status: N/A
      justification: Registry, docs, maps and ledger only on this clause; no code, no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no network, no JVM; REPARK_PARITY_LIVE unset. The only external evidence is the fork's pinned ledger and source in the local cargo checkout.
      artifacts: [task/ledgers/staging/rp-20-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Pin claims come from grep over Cargo.toml/Cargo.lock and from reading the fork ledger at the pinned cargo checkout, not from prose; the registry sweep is a grep, not a recall.
      artifacts: [Cargo.toml, Cargo.lock, docs/cutover/production-iceberg-status-2026-09-14.md]
    - id: AT-7
      status: N/A
      justification: No engine change and no wall-clock claim.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs are one line edc38c6aa5cdbe132235f4fefc01d3066d6cff23; the bump is the orchestrator's commit f1630f5a and this unit does not touch Cargo.toml or Cargo.lock.
      artifacts: [docs/fork-sync.md, map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The stamps mark the refusal FIXED at this pin exactly — the live Glue proof is named as pending (the post-merge dispatch of aws-acceptance.yml), not claimed; pre-fix records are kept verbatim beside the stamps.
      artifacts: [docs/cutover/production-iceberg-status-2026-09-14.md, docs/cutover/inventory.md, docs/cutover/map.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state is the fork's base-red (8/8 replace pins red on the trait default, quoted in §Red first) plus the cutover rows' pre-fix record kept verbatim; after state is the stamped rows and the fork's green suite. No product code changed.
      artifacts: [task/ledgers/staging/rp-20-ledger.md, docs/cutover/production-iceberg-status-2026-09-14.md]
  complete: true
```

## Live proof (orchestrator, 2026-09-14)

Post-merge `workflow_dispatch` of `aws-acceptance.yml` on `main` `0b33f5b7`: run 34901483202, 21:55:59Z → 22:27:04Z,
conclusion success, no environment wait. Acceptance module: 10 passed in 308.55 s (the eight existing legs plus
`test_create_or_replace_twice_against_glue` and `test_create_or_replace_twice_against_s3tables`). dbt gold acceptance:
`test_gold_stage_on_glue` 1 passed in 36.19 s (two `dbt run` passes, `dbt test` 10 blocks). Recorded in
`docs/cutover/inventory.md` §8.
