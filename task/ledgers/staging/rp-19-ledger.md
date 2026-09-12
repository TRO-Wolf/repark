# Charter ledger — RP-19 · fork pin 3ebf7d36 consumer (F-S3ROOT-1; ORPHAN-S3TABLES-1 parser half)

**Date:** 2026-09-12 · **Branch:** `chore/repin-rp-19` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-18-ledger.md](rp-18-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The orchestrator bumped the iceberg-rust pin to
`3ebf7d36c2c1fe3664572e23ebc89a698ce2b55c` (bump commit `31c169b1`). The reason for the
bump is `#281` F-S3ROOT-1 (S2-28): a bare-bucket object-store location — `s3://bucket`,
every S3 Tables table's location shape — resolves to the bucket root like Java's
`S3URI`. The shared `scheme_relative_path` returns the empty key on an exact
`{scheme}://{bucket}` host match across the S3, GCS and OSS arms (`s3://bx` /
`s3://b-other/…` stay refused; azdls never had the defect), and `list` joins
`base` + entry with a `/` only when neither side carries one, so `list("s3://b")` and
`list("s3://b/")` answer byte-identical locations. The consumer record is the owner's
S3 Tables orphan bug: `CALL s3tables.system.remove_orphan_files` on the dev table bucket
died on the path parser before this pin and reaches the bucket's 405 listing refusal
after — the parser half closes here; the loud refusal is ORPHAN-S3TABLES-1's own card.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` (already on the branch); STATUS.md;
any product code (D-3); a live RePark pin (D-2 rules the pin is the registry row plus
the owner's verbatim reproduction — a `DESCRIBE TABLE EXTENDED` on a memory-catalog
table created with `LOCATION 's3://…'`-shaped metadata never reaches the fork's `FileIO`
parser, so no Python pin exists without AWS); the loud refusal itself
(ORPHAN-S3TABLES-1).

## Take / skip — riders on `3ebf7d36`

| Fork PR | Ask | Take or skip | What it means for RePark |
|---|---|---|---|
| `#281` | F-S3ROOT-1 | **take** (reason for the bump, the whole bump — D-1) | `s3://bucket` resolves to the bucket root like Java's `S3URI` on the S3, GCS and OSS arms, so a table whose location is a bare bucket no longer fails the path parser. The RePark consumer pin is the ORPHAN-S3TABLES-1 registry row plus the owner's verbatim reproduction (D-2): the parser error before, the 405 after. |

## PROPOSITION LEDGER — RP-19 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `pin_moved`: the five `rev` lines are byte-identical — `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u` prints exactly one line, `3ebf7d36c2c1fe3664572e23ebc89a698ce2b55c`; `docs/fork-sync.md` carries the RP-19 pin-history row and the root `map.md` pin sentence names the new sha. | The grep; five Cargo.toml hits; Cargo.lock sources; the fork-sync row; the map.md sentence. | **PROVEN** | `sort -u` prints one line `rev = "3ebf7d36c2c1fe3664572e23ebc89a698ce2b55c"`. Cargo.toml 5 hits, Cargo.lock 6 sources. Pin-history row 2026-09-12 names F-S3ROOT-1 `#281`, no riders. Root map.md sentence `**RP-19 (2026-09-12):** \`3ebf7d36\`` landed in the orchestrator's bump commit `31c169b1`. This unit does not edit the pin. |
| C-002 | `registry_row_opened`: `docs/spark-sql-iceberg-parity.md` §7 carries a new `### ORPHAN-S3TABLES-1` row beside ORPHAN-1/2 that marks the parser half FIXED at pin `3ebf7d36`, keeps the 405 listing refusal open for card ORPHAN-S3TABLES-1, and records the owner's 2026-09-12 dev-bucket reproduction verbatim — `Invalid s3 url: s3://<id>--table-s3, should start with one of [s3://<id>--table-s3/, s3a://…/, s3n://…/]` before, `405 MethodNotAllowed` from `ListObjectsV2` after — as the RePark-side pin (D-2, no live test). | The row text; the verbatim reproduction; the FIXED stamp at this pin. | **PROVEN** | Row opened after ORPHAN-2. Red evidence (the fork's base-red run on `9e3522e3`, this branch's exact prior pin — `task/f-s3root-1-ledger.md` §Base-red at rev `3ebf7d3`): `test s3::tests::test_s3_relative_path_bucket_root_is_empty_key ... FAILED` (`left: None`, `right: Some("")`), `test_create_operator_bucket_root_resolves_to_empty_key ... FAILED` with `Invalid s3 url: s3://my-bucket, should start with one of [s3://my-bucket/, s3a://my-bucket/, s3n://my-bucket/]` — byte-identical in shape to the owner's run-7 error — plus the GCS/OSS root pins red; 4 of 6 new pins red, controls green. Green on the pin: the fork's `cargo test -p iceberg-storage-opendal --all-features --lib` 55 passed, and the consumed source reads true — `crates/storage/opendal/src/utils.rs::scheme_relative_path` at the pinned checkout returns `Some("")` on an exact `{scheme}://{bucket}` host match and `None` on `bucketx` / `bucket-other`. |
| C-003 | `gates_green`: the listed gates pass on this branch — the orphan/catalog pytest selection, the whole parity suite, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, `make verify`. | Counts pasted into §Gates. | **PROVEN** | All listed gates exit 0 (counts in §Gates). |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Documentation-and-registry unit; no production pin exists to write (D-3 forbids product
code; D-2 rules the RePark pin is the registry row plus the owner's verbatim
reproduction — a `LOCATION 's3://…'` memory-catalog table cannot reach the fork's
`FileIO` parser from Python without AWS). The red this pin turns green was measured on
the fork against this branch's exact prior pin `9e3522e3` — the fork ledger
(`task/f-s3root-1-ledger.md` at the pinned checkout, §Base-red evidence) records
`cargo test -p iceberg-storage-opendal --all-features --lib` exiting 101 with 4 of 6 new
pins red:

```
test s3::tests::test_s3_relative_path_bucket_root_is_empty_key ... FAILED
test tests::s3_scheme_alias::test_create_operator_bucket_root_resolves_to_empty_key ... FAILED
test tests::test_opendal_gcs_bucket_root_relative_path_is_empty ... FAILED
test tests::test_opendal_oss_bucket_root_relative_path_is_empty ... FAILED

assertion `left == right` failed: s3://mybucket must resolve to the empty key
  left: None
 right: Some("")
```

and the owner-side red is the run-7 reproduction the registry row carries verbatim: the
parser error before (`Invalid s3 url: s3://<id>--table-s3 …`), the `405
MethodNotAllowed` to `ListObjectsV2` after. At this pin the parser half is green; the
405 stays red by design until ORPHAN-S3TABLES-1.

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests -q -k "orphan or catalog"` | 0 — 149 passed, 9 skipped, 6150 deselected in 171.10s |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 749 passed, 1 skipped, 12 xfailed in 556.41s |
| `make check-docs-links` | 0 — 791 files, 5092 links |
| `make check-ledger-grammar` | 0 — 113 live ledgers, 717 clauses, 1347 pinned clause ids |
| `make check-ledgers` | 0 — 331 ledgers in bins (218 archived), 910 ledger links |
| `make verify` | 0 |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-19
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 greps all five Cargo.toml rev lines and the six Cargo.lock sources to one sha; C-002 opens the registry row covering both halves of the owner bug (parser FIXED at this pin, 405 listing refusal open); the pinned fork source was read at the cargo checkout of rev 3ebf7d3, not trusted from the card.
      artifacts: [docs/fork-sync.md, docs/spark-sql-iceberg-parity.md, map.md]
    - id: AT-2
      status: ATTACKED
      evidence: The whole bump is consumed (D-1 take/skip names #281 alone); the registry row records the full surface of the fix relevant to RePark — the S3/GCS/OSS bare-bucket arms, the refused near-misses, and the list join — and the fork's base-red and post-fix green runs are quoted as evidence.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/fork-sync.md]
    - id: AT-3
      status: N/A
      justification: No refusal path or error contract changes in RePark this unit; the loud refusal is ORPHAN-S3TABLES-1's own card.
    - id: AT-4
      status: N/A
      justification: Docs, registry and ledger only; no code, no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no network, no JVM; REPARK_PARITY_LIVE unset. The only external evidence is the owner's recorded dev-bucket reproduction and the fork's pinned source in the local cargo checkout.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/rp-19-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Pin claims come from grep over Cargo.toml/Cargo.lock and from reading utils.rs/s3.rs at the pinned cargo checkout (Some("") on the bare host, None on bucketx/bucket-other), not from prose; the verbatim reproduction is recorded, not re-derived.
      artifacts: [Cargo.toml, Cargo.lock, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No engine change and no wall-clock claim.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs are one line 3ebf7d36c2c1fe3664572e23ebc89a698ce2b55c; the bump is the orchestrator's commit 31c169b1 and this unit does not touch Cargo.toml or Cargo.lock.
      artifacts: [docs/fork-sync.md, map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The registry row marks exactly the parser half FIXED at this pin and keeps the 405 listing refusal OPEN for ORPHAN-S3TABLES-1 — no verdict beyond the card's D-2; the open half is named in the row, the fork-sync note, the staging map entry and the call_orphan map note.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/fork-sync.md, task/ledgers/staging/map.md, crates/repark-spark/src/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state is the owner's verbatim parser error plus the fork's base-red run on the same prior pin (9e3522e3, 4/6 new pins red, quoted in §Red first); after state is the 405 verbatim plus the fork's 55-test green run and the pinned source read. No product code changed (D-3).
      artifacts: [task/ledgers/staging/rp-19-ledger.md, docs/spark-sql-iceberg-parity.md]
  complete: true
```
