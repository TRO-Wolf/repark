# Fork sync — keeping repark and TRO-Wolf/iceberg-rust aligned

RePark consumes the whole `iceberg*` family from the owned fork via `[patch.crates-io]`,
**rev-pinned**: five `rev = "…"` lines in the root [Cargo.toml](../Cargo.toml) that must stay
byte-identical (the **single-writer-per-pin** invariant). This page is the *sync contract* —
the structural decision that the fork stays a sibling repo is
[ADR-0001](adr/0001-own-iceberg-fork.md) and is not restated here.

## The three rules

1. **The pin moves only via its own PR.** `make bump-fork-pin REV=<sha|branch>` rewrites all
   five rev lines + `Cargo.lock` and prints the fork changelog URL for the PR body;
   `make preflight` gates the PR like any other. Never bump the pin as a side effect of an
   unrelated change.
2. **Fork main must be green before it is pinnable.** The fork's own CI decides; a red fork
   main is not a valid `REV`.
3. **Upstream flows through the fork, never directly.** `apache/iceberg-rust` improvements
   merge fork-side first, then reach repark as an ordinary pin bump (rule 1). RePark never
   patches Iceberg semantics locally — engine-agnostic table-format work lives in the fork
   (CLAUDE.md / AGENTS.md invariant).

## Drift visibility

The weekly **`fork-sync-drift`** workflow
([.github/workflows/fork-sync-drift.yml](../.github/workflows/fork-sync-drift.yml); also
manually dispatchable) reports three numbers in its run summary, so nobody has to remember
to check:

| Number | Meaning |
|---|---|
| Fork main ahead of pin | commits on the fork's `main` that repark does not consume yet |
| Upstream ahead of fork | commits on `apache/iceberg-rust` `main` past the fork's merge-base |
| Pin reachability | whether the pinned rev still exists on the fork (force-push guard) |

**Soft thresholds:** fork **>10** commits ahead of the pin → schedule a pin bump; upstream
**>50** ahead of the fork → schedule a fork-side upstream merge in the next planning pass.
**Hard stop:** the pin going unreachable — investigate before any other fork action.

## Pin history

The live pin is the five identical `rev` lines in the root [Cargo.toml](../Cargo.toml).
This table is append-only: one row per dedicated pin-bump PR (rule 1). Started 2026-08-15.

| Date | Old pin | New pin | Notes |
|---|---|---|---|
| 2026-09-03 | `ff4764d3eba037ecfa185be5de5f639cbffef80b` | `c1d6c9de1498cf04765893ef3f698d915766a6a7` | RP-8: `#261` F-19/F-20 (files-exist narrowed to the replacement blobs, the sibling rewrite deleted, `FanoutWriter::close` drains ascending), `#262` F-21 (the DataFusion delete exec merges legacy parquet position deletes; `validate_fresh_dvs_only` now blocks only file-scoped), `#263` F-22 (one delete-manifest pass returns `legacy_deletes` + `data_sequence_numbers`; `load_legacy_positions_by_path` is one projected read per delete file; the close takes an optional pre-loaded `ManifestList`). BREAKING: `DvContainerClose::retained_references` and `StampedDeleteFile` are gone. Family frozen. Take/skip in the RP-8 ledger. |
| 2026-09-02 | `fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a` | `ff4764d3eba037ecfa185be5de5f639cbffef80b` | RP-7: `#260` F-18 deletion-vector container close — only the touched blob is rewritten, the sibling entry stays byte-identical in its old container, removal is keyed by Java's `DeleteFileSet` triple, and the data-file walk is lazy (`close_touched_dv_containers_with_partitions`). Family frozen. Take/skip in the RP-7 ledger. |
| 2026-09-01 | `00cdde00685bbc94552b29fcf8ed6767fe051ce6` | `fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a` | RP-6: `#253` PR-1 REPLACE added>deleted refuse, `#254` PR-2 evolved-spec rewrite, `#252` PR-5A Glue/S3 Tables commit seams, `#255` PR-3 V3 MoR UPDATE lineage, `#256` PR-6B branch MoR UPDATE, `#257` PR-4 V2→V3 upgrade, `#250`/`#251` docs+PR-6A. Family frozen. Take/skip in the RP-6 ledger. |
| 2026-09-01 | `33be9a0f411c37cd8d7b38c4db81eec30c1344cc` | `00cdde00685bbc94552b29fcf8ed6767fe051ce6` | RP-5: `#245` F-6b `with_commit_branch`, `#246` R91 unknown-on-write, `#247` F-8 metadata projection/listing, `#248` F-16r delete-ratio bounds-only, `#249` F-6c branch-head scans. Family frozen. Take/skip in the RP-5 ledger. |
| 2026-08-31 | `d408da42fb91db2010662fe1da3783b82fa6e1ed` | `33be9a0f411c37cd8d7b38c4db81eec30c1344cc` | RP-4: `#241` test pin, `#243` F-7 slice 1 (v3 rewrite lineage), `#244` F-6 `to_branch` (carried). Family frozen. Take/skip in the RP-4 ledger. |
| 2026-08-30 | `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117` | `d408da42fb91db2010662fe1da3783b82fa6e1ed` | RP-3: F-17 `#237` (DV container close), F-14 `#235` (Hadoop `vN` pointer math), F-7 U3 `#227`, F-16 `#232`, F-9/F-15 `#233`, H7-P1/R114 `#239`. Family frozen. Take/skip in the RP-3 ledger. |
| 2026-08-27 | `5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6` | `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117` | RP-2: consume fork F-3 `#216` (remove-dangling-deletes compose), F-5 `#217` (ReplacePartitions scope, BEHAVIOR), F-13 `#219`/`#221`/`#222` (V3 MOR DV writes, BREAKING), F-7 U1+U2 `#225`/`#226` (lineage, BEHAVIOR). Family frozen; the fork's variant type adds `parquet-variant*` 58.4.0. |
| 2026-08-23 | `0c5fd58d4ab73a0113a8b28b717cf5d002b0f8f2` | `5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6` | RP-1: consume fork F-0 `#214`, F-1 (RPDF floor 5), F-2 `#215`. Family frozen. F-8a last-`$` filter. |
| 2026-08-15 | `b009ac158f7584a956fa9292c0e9675a411ecf0d` | `0c5fd58d4ab73a0113a8b28b717cf5d002b0f8f2` | AD-1 via `make bump-fork-pin` + `#182`/`#183` adapter. HALTED predecessor: repark `#102` (pin `1dae9b66`, never merged). `#186` is not a rider. |

Riders on the 2026-08-15 row (FW-0's nine main-side commits plus `#192`–`#195`, content-present on the target):

1. `TRO-Wolf/iceberg-rust#182 Feat/slate 2026 07 31 combined`
2. `TRO-Wolf/iceberg-rust#183 Feat/fk mor perf campaign`
3. `TRO-Wolf/iceberg-rust#184 Fix/qb posdelete bounds and partition stamp`
4. `TRO-Wolf/iceberg-rust#185 docs(task): DF 52→54 churn map (V0 recon)`
5. `TRO-Wolf/iceberg-rust#187 Chore/df54 family bump recut`
6. `TRO-Wolf/iceberg-rust#188 docs(ledger): #187 catch-up — queue reconciliation + 3 lessons`
7. `TRO-Wolf/iceberg-rust#189 docs(datafusion): H7-S2 review round 2 — peak-memory doc clause + ledger corrections`
8. `TRO-Wolf/iceberg-rust#190 U3 / hazard-1: Java MIDPOINT row-group selection over ranged splits`
9. `TRO-Wolf/iceberg-rust#191 2026-08 audit hardening — decimal parity, bounded recursion, typed metadata failures, namespaces, cache bytes, secret redaction`
10. `TRO-Wolf/iceberg-rust#192 inspect: project timestamptz/timestamptz_ns in data_file metadata tables`
11. `TRO-Wolf/iceberg-rust#193 Emit Arrow timestamptz as tz=UTC (Spark toArrow)`
12. `TRO-Wolf/iceberg-rust#194 inspect: drop empty partition column; tolerate $ in table names`
13. `TRO-Wolf/iceberg-rust#195 scope IcebergCatalogProvider namespace walk`

Equivalence (FW-0, copy-true): pin `b009ac15` (18-commit DF54 side branch) is content-equivalent on `1dae9b66` via `#182` (scan/`plan_tasks`/`with_file_prune_only`) + `#187` (DF54 family + `PageIndexPolicy` + nested-insert re-pin); six `docs(task)` tip-stamps are SHA-unique and not product content. This bump then carries `#192`–`#195` on top of that equivalent main.

## Debug

First checks: dispatch `fork-sync-drift` manually and read the run summary; locally,
`grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml | sort -u` must print exactly one line.
Escalate to: [map.md](map.md#debug).
