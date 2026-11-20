# Card FROM-CSV-RESEAT-1: `from_csv` parses each token by its own rule and disagrees with Spark in 8 of 16 measured cells, silently

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §11 item 1, kept outside the repository.

**Status:** filed 2026-11-20; not dispatched. It waits for CSV-FIELD-KERNEL-1.

**Severity: S2, silent.** **Not label-blocking**, because `from_csv` is not the default read. This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M, by Fable]** `from_csv(text, 'x <type>')` on main. Of the 16 cells Fable measured, these 8 differ from Spark, all silently:

| text, type | Spark | main |
|---|---|---|
| `inf`, `nan`, `+Inf` as `DOUBLE` | NULL | `inf`, `nan`, `inf` |
| `1d`, ` 7 `, `0x1p3` as `DOUBLE` | `1.0`, `7.0`, `8.0` | NULL |
| `2024` as `DATE` | `2024-01-01` | NULL |
| `…T10:30:00Z` as `TIMESTAMP_NTZ` | NULL | `2024-01-15 10:30:00` |

**Where.** `crates/repark-functions/src/csv/from_csv.rs`, in `append_token` (Fable cites lines 342-436). It already parses a CSV token by its declared type, in Rust, and it disagrees with Spark. Spark's `from_csv` uses the same `UnivocityParser.makeConverter` rule. A new `spark_csv_field.rs` beside it would make two Rust homes for one Spark rule, so the kernel is the single home.

**Re-seat.** Re-seat `from_csv` on CSV-FIELD-KERNEL-1's kernel once that unit lands (Fable §11 item 1). It is not part of the kernel unit: Fable §10 item 1 says not to re-seat `from_csv` in that unit.

## First step

None beyond the re-seat, which waits for CSV-FIELD-KERNEL-1.

Up: [map.md](map.md).
