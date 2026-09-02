# Charter ledger — RDF-1 · position-delete `file_path` bounds, re-homed from fork ask F-16

**Date:** 2026-09-02 · **Branch:** `fix/rdf-1-position-delete-bounds` · **Base:** `origin/main`
`cee8126` · **Model:** claude-opus-5 (medium) · **Registry:**
[../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md) row
`RDF-1` · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
ask F-16 residue 2 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Fork PR `#259` refuted half of F-16 residue 2 fork-side: the fork's own
position-delete writers set `position_delete_writer_properties()` and reclaim a probe of the
MW-7 shape at pin `fb0cacfa`. RePark's MW-7 pin was still green at the same pin, so the delete
file RePark's MERGE wrote had to carry absent, truncated or unequal `file_path` bounds. It did:
absent.

**Not in this unit:** the other half of residue 2 — a delete file naming two or more data files
has unequal bounds by construction and stays fork work; `remove-dangling-deletes` (F-3);
dependency changes; any Spark-visible choice not measured (HALT).

## PROPOSITION LEDGER — RDF-1 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The delete file RePark's MERGE writes for the MW-7 C-011 shape is measured — manifest `lower_bounds` / `upper_bounds` on field `2147483546`, `referenced_data_file`, record count, and the delete Parquet's own column statistics — and the SAME shape is measured on the live PySpark 4.1.2 + Iceberg 1.11.0 oracle. | Both bound sets recorded before any change; oracle transcript. | **PROVEN** | RePark before: field `2147483546` ABSENT from both bounds maps (only `2147483545` = `pos`, 0..2499); the delete Parquet's `file_path` statistic was truncated at 64 bytes (min `…/2dfaeaae-`, max `…/2dfaeaae.`); `referenced_data_file` null. Spark: `file_path` statistic exact and untruncated, min == max == the full 118-byte seeded path, at BOTH granularities. §6. Citation: `python/repark-parity/bench/mw7/map.md`. |
| C-002 | Every RePark-written position-delete file carries exact, untruncated `file_path` and `pos` bounds, taken from the fork's own writer configuration rather than a local restatement. Every RePark position-delete path is audited, not just the suspect. | Audit of every `ParquetWriterBuilder` construction site; the fix reads the fork's setting; rebuild + re-measure. | **PROVEN** | One position-delete writer site exists (`write/position_delete.rs`; `write/append.rs` and `write/merge/*` are data-file writers, and `merge/dv_close.rs` funnels v2 into the same call). It now builds properties with `writer_props::position_delete_writer_properties_for`, which adds `position_delete_writer_properties().statistics_truncate_length()` — the fork's value, read not restated — to the table's codec. After: lower == upper == the full 103-byte seeded path. SQL `DELETE`/`UPDATE` were never affected (iceberg-datafusion already used the fork's properties). Citation: `crates/repark-iceberg/src/write/map.md`. |
| C-003 | The MW-7 runbook pin states the reclaim, not the survival, and is named for what it proves; the MW-8 partitioned pin stays green; the incidental control (a partition-scoped delete naming two data files still survives, shadowing rows that do not resurrect) is pinned on the Spark door; the pair is mutation-proof. | Flipped pin; MW-8 green; two Spark-door pins; mutation `N` red of `M`. | **PROVEN** | `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies`: equal exact bounds, `removed_delete_files_count` 1, seeded path leaves the live set, 0 delete files / 0 delete records, `COUNT(*)` still 2,500. MW-7 + MW-8 modules 21/21 green. Spark door: `call_rewrite_data_files_drops_the_merge_delete_that_names_one_data_file` and its control `call_rewrite_data_files_keeps_a_partition_delete_that_names_two_data_files`. Mutation (drop `set_statistics_truncate_length`): 1 red of 21 Python, 1 red of 3 in `call_rewrite_dangling`; the control stays green by design. §7. Citation: `python/repark/tests/map.md`. |
| C-004 | Documents say what the pins prove: registry `RDF-1` FIXED with both engines' measured bounds and counts; the guide's runbook limit rewritten; north star §3b errata and `docs/design/map.md`; handoff F-16 residue 2 re-homed with the surviving ask stated in one line; every touched directory's `map.md`; this ledger `move`d to `completed/` last. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`. | **PROVEN** | Registry row rewritten with the three-row measurement table and the 4.1.2 oracle addendum. Guide "What the cycle cannot reclaim" + the retry paragraph. `docs/design/format-v3-track.md`, `docs/design/map.md`, `docs/guide/map.md`, `task/roadmap/mid-term/map.md`, `crates/repark-iceberg/map.md`, `crates/repark-iceberg/src/write/map.md`, `crates/repark-spark/src/tests/map.md`, `python/repark/tests/map.md`, `python/repark-parity/bench/mw7/map.md`. STATUS.md does not name `RDF-1`. Citation: `crates/repark-iceberg/map.md`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rdf-1-position-delete-bounds
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The reclaim is asserted on the manifest bound, the procedure counter, the live file set, the census and the row count — five independent readings of one commit.
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Both bound shapes are pinned — one referent (equal bounds, removed) and two referents (unequal bounds, kept) — plus the pre-existing remove-dangling-deletes path.
      artifacts: [crates/repark-spark/src/tests/call_rewrite_dangling.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No-resurrection is asserted on both sides. The flipped pin holds COUNT(*) at 2,500 after the delete file is gone; the control holds the shadowed rows shadowed while the delete file survives.
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state. The change is one builder call on a per-file writer.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, credential or catalog-transport surface is touched.
    - id: AT-6
      status: ATTACKED
      evidence: The truncation value is read from the fork's position_delete_writer_properties() rather than restated, so a fork policy change carries instead of drifting.
      artifacts: [crates/repark-iceberg/src/write/writer_props.rs, crates/repark-iceberg/src/write/map.md]
    - id: AT-7
      status: N/A
      justification: No recursion and no new allocation path; exact bounds cost the length of one path per delete file.
    - id: AT-8
      status: N/A
      justification: No dependency, lock or toolchain change.
    - id: AT-9
      status: ATTACKED
      evidence: Registry RDF-1 records both engines' measured bounds, RePark's before/after counts, and the 4.1.2 oracle divergence from the 2026-08-24 recorded reading.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses pinned; nine map.md files in lockstep; mutation 1 red of 21 (Python) and 1 red of 3 (Spark door), restored and re-run green.
      artifacts: [python/repark/tests/test_mw7_scale_smoke.py, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
  complete: true
```

## 6. Measured bounds (C-001)

Fixture: `python/repark-parity/bench/mw7/measure.py`, 2,500 rows, `mor`,
`write.delete.granularity = 'partition'`, `write.target-file-size-bytes` 64 KiB, one MERGE
deleting every seeded row. Oracle: live PySpark 4.1.2 + Iceberg 1.11.0,
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[1]`, Iceberg runtime
`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, Hadoop catalog. The oracle's CTAS uses an 8 MiB
target so the seed is ONE file, then `ALTER TABLE … SET TBLPROPERTIES` drops the target to
64 KiB — a matched layout, not a matched property sequence.

| Cell | seed data file | delete file | `file_path` lower | `file_path` upper | `referenced_data_file` |
|---|---|---|---|---|---|
| RePark before | 68,523 B, 2,500 records, in band | 2,500 records | ABSENT | ABSENT | null |
| RePark after | 68,523 B, 2,500 records, in band | 2,500 records | full 103-byte path | same value | null |
| Spark 4.1.2, granularity `partition` | 51,167 B, 2,500 records, in band | 2,500 records | full 118-byte path | same value | null |
| Spark 4.1.2, granularity `file` | 51,167 B, 2,500 records, in band | 2,500 records | full 118-byte path | same value | null |

RePark before, the delete Parquet's own `file_path` statistic: min
`…/part=0/2dfaeaae-` (64 bytes), max `…/part=0/2dfaeaae.` (64 bytes, last byte incremented).
parquet-rs `DEFAULT_STATISTICS_TRUNCATE_LENGTH` is `Some(64)`; a truncated statistic reports
`min_is_exact()` / `max_is_exact()` false; the fork's `MinMaxColAggregator` only records an
exact bound, so the bound was dropped rather than written unequal.

End state after the five-step maintenance sequence:

| Cell | `rewrite_data_files` | data files | delete files | delete records | `COUNT(*)` |
|---|---|---|---|---|---|
| RePark before | rewritten 4, added 2, removed_delete_files 0 | 3 | 1 | 2,500 | 2,500 |
| RePark after | rewritten 5, added 2, removed_delete_files 1 | 2 | 0 | 0 | 2,500 |
| Spark 4.1.2 (both granularities) | rewritten 3, added 1, removed_delete_files 0 | 1 | 1 | 2,500 | 2,500 |

**Finding F-RDF1-1 (S2, AT-9).** Spark reclaims the DATA file — the 100 %-dead in-band seed is
a candidate, so `tooHighDeleteRatio` fires on the exact equal bounds — but at Iceberg 1.11.0 the
delete file itself survives all five steps as a dangling delete with `removed_delete_files_count`
0, at both granularities. The registry's 2026-08-24 reading (PySpark 4.0.1 + Iceberg 1.10.0,
"zero delete files") does not reproduce at this version. RePark after the fix goes one step
further than this oracle: its rewrite attributes the file-scoped delete to the data file it
named and drops it. Disposition: RECORDED in the registry row's Apache Spark bullet; not
repaired, because reclaiming more is not a parity defect and the row's claim — dead rows are
retained forever — is closed either way.

## 7. Mutation (C-003)

Mutation: delete `.set_statistics_truncate_length(position_delete_writer_properties()
.statistics_truncate_length())` from `position_delete_writer_properties_for`, rebuild, re-run,
restore.

| Suite | N red / M | Cell |
|---|---|---|
| `test_mw7_scale_smoke.py` + `test_mw8_runbook.py` | 1 / 21 | `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies` — bounds `[(None, None)]` |
| `call_rewrite_dangling.rs` | 1 / 3 | `call_rewrite_data_files_drops_the_merge_delete_that_names_one_data_file` — `removed_delete_files_count` 0, expected 1 |

The incidental control and the pre-existing `remove-dangling-deletes` pin stay green under the
mutation, which is what makes them controls rather than duplicates of the flipped pin. A first
attempt wrote both Spark-door pins on `DELETE FROM`; they passed under the mutation, because
SQL `DELETE` runs through iceberg-datafusion's writer, which already sets the fork's
properties. Both were rewritten on `MERGE`, the RePark-owned path.

## 8. Scope decisions

- The brief asked for the incidental control "on the Spark door". `crates/repark-spark/src/tests/call.rs`
  sits at its exact file-size baseline (1,307), so the pair went to `call_rewrite_dangling.rs`,
  the leaf that already owns delete-file removal through the CALL.
- The brief's expected end state ("zero delete files and zero delete records") is what RePark
  now measures. The live 4.1.2 oracle does not reach it; §6's finding records that rather than
  the pin asserting an unmeasured number.
