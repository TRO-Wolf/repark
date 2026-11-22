# v1.5.7 qualification record (Master Plan Step 1.4)

**Status:** prepared 2026-11-22 on branch `release/v1.5.7` by the release-prep clerk (`claude-haiku-5-5`). This record tags nothing, publishes nothing and triggers no workflow. The owner's 2026-11-22 go covers the cut. The squash-merge, the tag and the `release` deployment approval are owner actions and are not delegated here (section 8).

## 1. Source revision

| Item | Value |
|---|---|
| Base | `origin/main` `913fbd535518e281202d635428ba065f5343a5cf` (CAST-STATS-SILENT-1, PR #1405, merge commit `913fbd535`) |
| Release-prep branch | `release/v1.5.7` on the base: `066d1102d` (version and lock), then the notes, record, STATUS and map commit |
| Version | 1.5.7 (`[workspace.package] version` in `Cargo.toml`) |
| Previous tag | `v1.5.6` at `ae60c359` (PyPI 1.5.6) |
| Range | `ae60c359..913fbd53`: 129 non-merge commits, of which 42 are fixes (one unprefixed), 1 feature, 2 chores, 64 docs, 16 tests, 3 harness fixes and 1 CI change; the notes carry one line per user-visible change |
| Units merged in the range that this record names | PLAIN-DYNAMIC-OVERWRITE-GUARD-1 (#1402, `1ba92f372`), UNIX-TIMESTAMP-TRUNCATE-1 (#1408, `809319dae`), CAST-STATS-SILENT-1 with CAST-STATS-INTERVAL-1 (#1405, `913fbd535`) |

## 2. Dependency lock

| File | sha256 at base `913fbd53` | sha256 on the release branch | Delta |
|---|---|---|---|
| `Cargo.lock` | `0476e83c1a9ae41fd02dad2019c7cd7137085d878700c43c5557742afc1b2ec7` | `cf40d89c6f43fac747f9e698803f7c1fe64eac9a127fc59885298c2157bdb0c6` | 22 lines: the 11 `repark-*` workspace member `version` lines, `1.5.6` to `1.5.7` (11 removed, 11 added); nothing else (`cargo update --workspace --offline`) |
| `uv.lock` | `d84e3889ac6fd119a692948e7db3124e97cbe8d3a031481353fd47559ab535ea` | unchanged | none |

## 3. Platform and profile

- Host: Linux 6.8.0-151-generic, x86_64. The lane sets `CARGO_HOME` to its own scratch cargo home, so the owner's `~/.cargo` was not touched.
- No Rust build ran in this lane (the lane's memory and core limits forbid a workspace build). `cargo metadata --locked --offline` ran on the lock.
- Facade profile: not built here. The release wheels are built by `release.yml` at the tag.

## 4. CI on the merged-main commit

Runs read with `gh run list --branch main`, read only, at the time this record was written.

| Head | Workflow | Run id | Conclusion |
|---|---|---|---|
| `913fbd53` (base) | CI | 45067315060 | in progress |
| `913fbd53` | smoke | 45067315134 | in progress |
| `913fbd53` | Cache warm | 45067315051 | in progress |
| `913fbd53` | typos | 45067315104 | success |
| `913fbd53` | wheels | 45067315153 | skipped (push to main) |
| `809319dae` (PR #1408) | CI, smoke, Cache warm, typos | | success (all four); wheels skipped |

The CI and smoke conclusions on `913fbd53` are not yet green. Re-read them before the squash-merge; a run cancelled at its time cap is not a pass.

## 5. Verifier verdicts for the three units

The verdict files are the lane's scratch records (`verify-1402`, `verify-1402-muse`, `recheck-1402`, `verify-1405`, `verify-1405-muse`, `recheck-1405`, `recheck2-1405`, `verify-1408`, `verify-1408-muse`).

**PR #1402, PLAIN-DYNAMIC-OVERWRITE-GUARD-1** (merged `1ba92f372`; reviewed heads `678e4acc`, then `e3e64e5c`, PR head `540f00f9`).

- `claude-opus-5-5` on `678e4acc`: **FAIL**, one finding F1 (medium, records only). The card, the registry row and the PR body named only the absent target as the cost, but an input covering every partition and an existing empty directory, both answered as Spark does on 1.5.6, are refused too. No code change; the records were corrected. Closed by delta 1 (`e3e64e5c`, the cost restated in the card, the registry row and the PR body; 59 new pin cells for the unpinned routes). Re-check of `e3e64e5c` by `claude-opus-5-5`: **PASS**, no class (a) or (b) finding. Its minor C1 (the attribution of the absent-target measurement) is closed in the card (lines 72 to 73). Its minor C2 (1.5.6 also wrote a root part file on an empty input) is not verified closed in this record.
- `muse-spark-1.3-contributor` on `678e4acc`: **PASS**, no class (a) or (b) finding. Two class (c) minors: four probe-verified routes without pins (closed by the 59 pin cells in delta 1) and a card seat path that omitted the module (closed by the re-check's N3).

**PR #1405, CAST-STATS-SILENT-1 with CAST-STATS-INTERVAL-1** (merged `913fbd535`; reviewed heads `2f1255fce`, `a836317e`, `27510476`, PR head `3fd27614`).

- `claude-opus-5-5` on `2f1255fce`: **FAIL**. F1 (class a, silent, pre-existing on main): footer pruning through a float-to-integer cast dropped a NaN row (`CAST(d AS INT) = 0` over a row group with bounds `[5.0, 10.0]` and a NaN). Closed: a float source keeps order only to a float target. F2 (class b): the claim that pruning was never affected was too broad. Closed: scoped to text casts, and the float path recorded. F3 (class c): an extreme-bound drop costs a footer fold. Recorded. F4 (class c): one optimizer rule runs after the barrier. Records corrected to "builds no casts".
- `muse-spark-1.3-contributor` on `a836317e`: **FAIL**, one record falsehood (class b). The caveat example stated `max` of `CAST(1 AS TINYINT)` and `CAST(300 AS TINYINT)` as 112; Spark 4.1.2 gives 44. Closed: 112 replaced with 44 in the card, the registry and the PR body. Two class (c) wording minors (`DecimalInRanges` builds no casts; "every RePark cell") closed the same way.
- `claude-opus-5-5` re-check 1 on `27510476`: **FAIL**, one new finding N1 (class a, silent, pre-existing on main, identical on main): a DOUBLE-to-FLOAT cast was kept by the float arm, so footer pruning through `CAST(d AS FLOAT) > 100` over `5.0, NaN, 10.0` dropped the NaN row. Closed by delta 2: a float source keeps order only to a float target at least as wide (`float_keeps_order`, `float_width`).
- `claude-opus-5-5` re-check 2 (delta 2, PR head `3fd27614`, product head `a8501ba5`): **PASS**. N1 closed: every DOUBLE-to-FLOAT shape measured equals Spark on the head, and every cell that differs from main changes from a silent drop to an answer equal to Spark. No class (a) or (b) finding. Minors: c-1 (the registry's pruning entry is narrower than its sibling, "incomplete, not wrong"), c-2 (two closed shapes not pinned), c-3 (overflowing narrowing casts refuse loudly where Spark wraps, the existing registry caveat). c-1 and c-2 are not verified closed in this record.

**PR #1408, UNIX-TIMESTAMP-TRUNCATE-1** (merged `809319dae`; reviewed head `8ca96767`).

- `claude-opus-5-5` on `8ca96767`: **PASS**. 11,904 probe cells across Spark 4.1.2, main and head: 0 regressions, 3,046 fixed, 2,148 still unlike Spark on both (outside the seat). Stored-value cells (Parquet and Iceberg, cold read) equal Spark. Findings, all class (c): F1, high product severity but outside the seat: `F.expr` types `CAST(x AS TIMESTAMP_NTZ)` as `timestamp`, so `F.expr("unix_timestamp(CAST(s AS TIMESTAMP_NTZ))")` answers a zone-shifted instant (LA, `1970-01-01 00:00:00.5`: 28800, Spark 0). It is not in the card's out-of-seat list, and no card for it was found in the tree; the verifier recommended one. F2 (the epoch vectors do not assert RePark's `typeof`), F3 (the generic NTZ refusal text for a non-column argument), F4 (map edits trimmed other units' entries to fit the size ceiling).
- `muse-spark-1.3-contributor` on `8ca96767`: **PASS**. 4,159 probe cells, 12 native and 11 micro cells, 39 stored cells: zero regressions. Minors c1 (refusal text for non-identifier column names) and c2 (`F.to_unix_timestamp` with a pattern string) are pre-existing or outside the seat and are not closed in this record.

## 6. Gates on the release branch

Exit codes are the real codes from the lane run on the tree that this record describes.

| Gate | Command | Exit |
|---|---|---|
| Lock consistency | `cargo metadata --locked --offline` | 0 |
| Crate DAG | `make check-crate-dag` | 0 |
| lib.rs roots | `make check-lib-rs` | 0 |
| Rust file size | `make check-rust-file-size` | 0 |
| Python file size | `make check-lib-py` | 0 |
| Python conventions | `make check-python-conventions` | 0 |
| Docstring presence | `make check-docstring-presence` | 0 |
| Example coverage | `make check-example-coverage` | 0 |
| Manifest | `make check-manifest` | 0 |
| Model ban | `make check-model-ban` | 0 |
| Owner ruling | `make check-owner-ruling` | 0 |
| Spell check | `make spell-check` | 0 |
| Comment ban, `913fbd53..HEAD` | the oc-worker comment-ban helper | 0 (hits=0) |
| Docs compaction | `make check-docs-compaction` | 0 |
| Docs links | `make check-docs-links` | 0 |
| Map size | `make check-map-size` | 0 (758 maps clean) |
| Map sync | `python3 scripts/sync_map_md.py --check` | 0 |
| Docs pins | `pytest` on `test_v1_gate_docs.py` and `test_live_v3_docs.py` (`uv run`, as `make py-test`) | 0 (26 passed) |
| Preflight | `make preflight` | not run: it builds the facade wheel and runs the facade suite, beyond this lane's limits; CI runs the suites on the release PR |
| Rust tests, clippy, fmt | `make verify` | not run: a workspace build is beyond this lane's limits |
| Security | `make audit` | not run in this lane |

## 7. Residues and open items carried into 1.5.7

- Known correctness issues on main, carried (STATUS.md, "Known correctness issues"): PARQUET-INT96-AS-NTZ-1 (S1, open), WRITE-ORDER-DROPPED-1 (S2, card, not dispatched; added to the STATUS list by this change), and the BACKLOG and DECLARED rows the notes list.
- ICE-HELD-FRAME-SNAPSHOT-1: unit 1b is not built, per its card (2026-11-20); the card's live cells are in #1393.
- PLAIN-DYNAMIC-OVERWRITE-GUARD-1: its card says it ships in v1.5.7 only on a separate publish approval. The owner's 2026-11-22 go on the brief names the cut, #1405 and #1408; whether it covers #1402 is an open question for the owner.
- UNIX-TIMESTAMP-TRUNCATE-1 F1: the `F.expr` NTZ-cast typing has no card in the tree. Filing one is an open question.
- The v1.5.6 notes' known issues that are not on the STATUS list (the empty-snapshot residue of a facade `DELETE` on a required column, and the `UPDATE` with `1 / 0` on a no-op format-v2 merge-on-read) are not carried by this release notes file. Whether they stay open is an open question for the owner.

## 8. Owner actions before the tag

1. Confirm the open question on PLAIN-DYNAMIC-OVERWRITE-GUARD-1's publish approval (section 7).
2. Let CI and smoke on `913fbd53` conclude success (section 4).
3. Squash-merge the release PR on green required checks.
4. Tree equality (runbook step 2.1): `git rev-parse release/v1.5.7^{tree}` must equal `gh api repos/TRO-Wolf/repark/commits/<squash-sha> --jq .commit.tree.sha`.
5. Tag (runbook step 2.2): `git tag -a v1.5.7 -m "RePark v1.5.7" <squash-sha>`, then `git push origin v1.5.7`.
6. Approve the `release` deployment when `release.yml` pauses.
7. Registry check (runbook step 4.1): `https://pypi.org/pypi/repark/json` reports 1.5.7, and the wheel filename is `repark-1.5.7-cp312-abi3-manylinux...`.
8. Close-out: run the compact-context-docs ritual so STATUS.md and the release-state documents show the shipped tag.
