# Phase-1 PR-B ledger — repark-iceberg (declared-rename unit)

Status: ASSEMBLED 2026-08-06 (branch `phase-1/pr-b`).
Port-Source: `fc3f48102e437e2843ded460bc161edb434dac93`.
Fork rev audited/pinned: `b009ac158f7584a956fa9292c0e9675a411ecf0d`.

## Scope

Merge v1 `repark-catalog` + v1 `repark-write` into V2 `crates/repark-iceberg` as two independent
module trees (`src/catalog/`, `src/write/` — split `merge/` shape), copy-then-re-home,
byte-faithful except the docs/design/session-api.md §5 forced-edit classes. Arm the
`[patch.crates-io]` fork pin (five iceberg crates at the rev above) with an ADR-0001 fork-pin
proof test. Hoist `reregister_catalog_provider` from v1 `repark-sql/src/catalog_ops.rs` into
`src/catalog/catalog_ops.rs` (everything else in that v1 file stays behind for phase 2 — the
stay-behind table is in the module doc + `src/catalog/map.md`). Declared-rename unit under
docs/testing.md relocation discipline; the old→new map was generated from `--list` at the pin
(four prefix rules), never hand-written.

## Commit series (as landed)

1. `e3e8131` — chore(iceberg): repark-iceberg — v1 catalog+write re-homed as a
   declared-rename unit; fork pin. Root `Cargo.toml`: member + five iceberg 0.9.1 workspace
   deps + `[patch.crates-io]` fork pin + `tracing` workspace dep (orchestrator ruling R3,
   ratified — the write half's spans/warnings need it and PR-A had not added it).
   `Cargo.lock` regenerated. Forced-edit class 6 applied (below). deny/audit entries restored
   (below). map.md lockstep (root, crates/, crate, src/, catalog/, write/, merge/).
2. `af66747` — feat(iceberg): catalog_ops — `reregister_catalog_provider` hoisted MOVE-ONLY
   from v1 `repark-sql/src/catalog_ops.rs:182-193`; body byte-faithful modulo the declared
   prefix rewrite (`repark_catalog::` call → `crate::catalog::`) and imports narrowed to the
   direct set (`DataFusionError` not imported; the ported `# Errors` intra-doc link is kept
   verbatim and did not lint under the `make ci` surface). Re-exported at module + crate root.
3. `e757f92` — test(iceberg): fork-pin proof test; ledgers, deferred-test manifest, brief
   census correction.
4. `340211a` — ci: rust-cache restore + cache-warm pre-warmer (the PR-B pairing). Orchestrator
   carve-out commit (`.github/workflows/cache-warm.yml` new, `ci.yml` rust-cache restore steps,
   both `.github/` map.md files); deferred from PR-A, landed 2026-08-06 17:08 after assembly
   close.

Every commit: `make ci` green + pre-commit hook green.

## Forced-edit class 6 — shared test tracing harness (orchestrator ruling R1)

Merging the two v1 crates into one test binary broke the per-binary global-tracing-subscriber
invariant both v1 harnesses relied on (two `set_global_default` installers; first wins, the
other records nothing). Sanctioned fix, landed inside the declared-rename commit:

- **New file** `src/test_tracing.rs` (`#[cfg(test)] mod test_tracing;` in `lib.rs`,
  file-backed per check_lib_rs): ONE global subscriber carrying BOTH layers — the catalog
  span-field capture layer (thread-routed slot) and the merge `SpanNameRecorder`
  (root-descended `merge.*` spans) — installed once via `Once`, tolerant of repeat calls
  (`let _ = set_global_default`), with accessors `begin_catalog_capture` /
  `clear_catalog_capture_slot` / `merge_span_names`.
- **Edited span 1** — `src/catalog/tests.rs`, the v1 `capture_catalog_spans` harness region
  (v1 `repark-catalog/src/tests.rs` ~1696–1832: `SpanFieldCapture`/`FieldCollector`/
  `CAPTURE_SLOT`/`SpanFieldLayer` + the `Once` global install): layer + storage moved to the
  shared harness; `CaptureGuard`/`capture_catalog_spans` remain, rewritten over the accessors.
  Residual diff 134 lines, zero assertion lines.
- **Edited span 2** — `src/write/merge/streaming_scan_tests.rs`, the global-install site
  inside `mor_merge_emits_five_phase_spans_with_commit_last` (v1
  `repark-write/src/merge/streaming_scan_tests.rs` ~1513–1573: `SpanNameRecorder` + `RECORDED`
  `OnceLock` + `set_global_default` `.expect(...)`): moved to the shared harness; the test now
  takes `crate::test_tracing::merge_span_names()` and clears it. Residual diff 65 lines, zero
  assertion lines.
- **Every test assertion byte-unchanged** (fidelity guard: no `assert!`/`assert_eq!`/
  `assert_ne!` line appears in either residual diff). All 243 workspace tests pass.

## Fidelity check (pre-fmt, per orchestrator ruling R2)

`fidelity_check.sh` (WS1, extended with the two declared-edit files above) ran against the v1
pin BEFORE `cargo fmt`: **exit 0**. Residuals: the two declared-edit files (assertion-free, as
above) and the enumerated class-(c) doc lines in `catalog/mod.rs` (2 lines: crate→module
wording) and `write/mod.rs` (2 lines: same class).

Rustfmt reflow after the >100-col prefix rewrites — every reflowed site:

| File | Site | Reflow |
|---|---|---|
| `src/lib.rs` | write re-export block (lines ~33–44) | list re-wrapped |
| `src/write/merge/mod.rs` | ~2533 (`write_position_deletes` call) | binding split onto its own line |
| `src/write/merge/streaming_scan_tests.rs` | ~1101 (`with_file_scoped_rewrite` call) | args split one-per-line |

## Census reconciliation (recorded verbatim)

| v1 crate | v1 `--list` @ pin | V2 target | brief claim | delta | explanation |
|---|---|---|---|---|---|
| repark-catalog | 50 | `repark_iceberg::catalog::*` 50 | 51 | -1 | doc-comment `#[tokio::test]` at v1 `repark-catalog/src/tests.rs:1783` counted by the brief's grep; not a test |
| repark-write | 191 | `repark_iceberg::write::*` 191 | 192 | -1 | doc-comment `#[tokio::test]` at v1 `repark-write/src/merge/mod.rs:425`; not a test |
| **PR-B cohort** | **241** | **241** | 243 | -2 | `--list` is ground truth |

Assembly run (2026-08-06, at commit 1): sorted
`cargo test --locked --workspace -- --list` (per-package, crate-prefixed) = 243 lines
(241 `repark_iceberg::*` + 2 `repark_common::*`); `diff` against the generated rename map's
right-hand column: **EMPTY**. Zero deferred, zero `#[ignore]`. Commit 3 then adds one NEW
(non-ported) test, `repark_iceberg::fork_pin_tests::fork_pin_plan_commit_base_load_occ_contract`
— workspace `--list` total 244 from that commit on.

## Gate results

- [x] `make ci` per commit: green (commits 1–3); re-run green at the true branch head
      (2026-08-06 verification pass, includes `340211a`).
- [x] `make preflight` green at assembly close (commit 3, 2026-08-06 17:06 — BEFORE `340211a`
      landed at 17:08). Head coverage for the workflow-only commit 4: `make workflows-lint`
      (parse + zizmor over all workflows, incl. `cache-warm.yml` + edited `ci.yml`) re-run
      green at the verification pass, 2026-08-06.
- [x] Forbidden-literal sweep (tree + `git log -p` of the series): zero hits (narrowed ARN
      pattern per the 2026-08-06 ruling; the synthetic example-account fixture ARN in
      `catalog/tests.rs` is sanctioned and untouched).
- [x] Rename-map diff empty (census above).
- [x] Fork-pin proof: `fork_pin_tests` exercises `iceberg::plan_commit_base_load` +
      `CommitBaseLoadPlan` — verified present at fork rev `b009ac15` and **0 hits** in the
      registry `iceberg-0.9.1` source (compile-fails on silent registry fallback); asserts the
      OCC contract (stale base ⇒ `Conflict` even with a service-matching provided table), not
      `assert!(true)`. Belt-and-braces with the ported
      `catalog::tests::fork_patch_in_effect_deletefilter_is_public`.
- [x] crate-DAG + lib-rs gates pass with the new crate (tier map already carried
      `repark-iceberg: 1`).
- [x] deny/audit: gates flagged exactly RUSTSEC-2024-0436 (paste, unmaintained) +
      RUSTSEC-2026-0194/0195 (quick-xml DoS ×2 versions: 0.38.4 via object_store 0.13.1,
      0.37.5 via reqsign 0.16.5) — verified by a config-less `cargo audit` run — plus the
      `webpki-roots` (CDLA-Permissive-2.0) and `libbz2-rs-sys` (bzip2-1.0.6) license entries.
      v1's justified wording restored verbatim into `deny.toml` + `.cargo/audit.toml` (dated
      version note appended per the WS3 procedure); the rkyv entry (RUSTSEC-2026-0235) did NOT
      fire and was NOT restored.

## Fork-audit findings (pre-copy report, recorded per its "PR-B actions")

### Metadata-projection shim — NOT STALE; copied unchanged

Ported as-is (`ProjectingMetadataTableProvider` / `apply_projection_exec` /
`MetadataProjectionSchemaProvider` + provider.rs wiring). **Still required** at fork rev
`b009ac15`: `IcebergMetadataTableProvider::scan` ignores `projection` (fork
`crates/integrations/datafusion/src/table/metadata_table.rs:97-105`; `IcebergMetadataScan`
carries no projection field, `physical_plan/metadata_scan.rs:36-47,80-89`).

**Removal criterion:** delete the shim + the `$`-name-heuristic wrap only when a fork rev's
metadata-table `scan` honors `projection` (including the empty-projection count/show case);
re-verify at every fork repin.

**Fork-docs gap:** the fork's `docs/parity/GAP_MATRIX.md` / `docs/ENGINE_CONTRACT.md` carry no
metadata-projection entry at `b009ac15` — the fork-workstream seed the v1 shim header references
is ledger-only. Carried forward here so the seed is not lost in the port: *file the projection
gap into the fork's GAP_MATRIX and land the fork-side fix; then apply the removal criterion.*

### NamespaceScopedCatalog wrapper — copied unchanged; 16-method gap recorded

The wrapper forwards only the 14 required `Catalog` trait methods (1 intentionally overridden:
`list_namespaces`, its purpose); **all 16 defaulted methods fall to trait defaults with zero
omission comments** (trait-wrapping skill rules 2b/3 violated at the source). No signature
drift; all gaps latent today (the fork DF integration's only calls are forwarded methods).

Gap list with severities:

| Method | Severity | Why |
|---|---|---|
| `publish_replace_table` | **HIGH** (P5 shape) | trait default `FeatureUnsupported` swallows MemoryCatalog + S3TablesCatalog overrides |
| `name` | Medium | default reports `"unnamed"`; every in-tree catalog overrides |
| `properties` | Medium | default empty map; every in-tree catalog overrides |
| `list_views`, `create_view`, `load_view`, `drop_view`, `view_exists`, `rename_view`, `update_view` | Medium | `FeatureUnsupported` default swallows memory/rest/sql view support |
| `publish_create_table` | Low-medium | composes over forwarded `register_table` — currently equivalent; future inner override swallowed |
| `update_namespace_properties`, `set_namespace_properties`, `remove_namespace_properties` | Low | compose over forwarded primitives — currently equivalent |
| `invalidate_table`, `invalidate_view` | Low | no-op defaults; no in-tree overrides at this rev |

Also: the wrapper's banner ("All other methods fully delegate to `inner`") is inaccurate for the
16 defaulted methods.

**Follow-up seed (post-copy unit, NOT this phase):** add explicit forwards for all 16 defaulted
methods (or omission comments where the default is genuinely wanted); fix the "fully delegate"
banner; add the trait-wrapping checklist-item-4 test (`publish_replace_table` through the
wrapper against a memory inner). **Standing rule:** on every fork repin, re-enumerate the trait
surface — new defaulted methods reopen this bug class silently.

## Retrospective

_(fill at PR close per SEPMO)_
