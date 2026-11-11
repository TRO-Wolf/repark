# Re-runs at item 2's head (`e998bbb90`, release build)

## #1292's three tables

Each RePark leg ran from a scratch copy of the committed probes under
`task/wo/struct-compare-by-position-1/evidence/`, with a fresh warehouse, as `after/provenance.md` there records:
`drive_cmp.py --leg main` over pass 1 (`cells.jsonl` less its last four lines) and `pass2_cmp.py --leg main --mode
cells` over pass 2; `variant_probe.py --leg base --only P10,P10dml`; `probe_cmp_variant.py --leg base`.

| Table | Records | Byte-identical to `after/` there | Equal once random alias ids are masked |
|---|---|---|---|
| step 0 | 4393 | 4393 | 4393 |
| variant seat C | 218 | 195 | 218 |
| variant extension | 246 | 174 | 246 |

No outcome, label or class changes.

## Pins

| Pin | Debug | Release | ci-profile facade | Release facade |
|---|---|---|---|---|
| unit C: `struct_setop_reorder_refusal`, `struct_setop_reorder_refusal_delta2`, `test_struct_setop_reorder_refusal_1.py` | pass | pass | pass | pass |
| #1290: `struct_setop_type_family_1`, `struct_setop_recursive_kind`, `test_struct_setop_type_family_1.py` | pass | not run | pass | pass |
| #1292: `struct_compare_by_position`, `struct_pairing_doors`, `test_struct_compare_by_position_1.py` | pass | pass (`struct_compare_by_position`) | pass | pass |

`struct_pairing_doors.rs` is unchanged by this unit.
