# Card UPDATE-SCHEMA-BATCH-MISMATCH-1: a two-file copy-on-write `UPDATE` refuses about half the time at planning

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the SIGNED-ZERO-NORMALIZE-1
evidence on main ([cards.md](../../wo/signed-zero-normalize-1/evidence/cards.md), where the name was given, and
[item3/moves.md](../../wo/signed-zero-normalize-1/evidence/item3/moves.md)); SIGNED-ZERO-NORMALIZE-1 part B builder,
2026-11-13 (the 50-repeat check on branch `fix/signed-zero-normalize-1-part-b`, not merged at filing); Fable guidance,
2026-11-13; the orchestrator's first probe, 2026-11-13.

**Status:** filed 2026-11-13, not dispatched. Severity **S3**, a loud refusal: no wrong answer was measured.
Priority **label-blocking**: the owner's ruling of 2026-11-13
([rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), item 6) ranks it now, and it must be
explained before the scoped label ICE-V2-FLAT-AWS-LABEL-1.

## Measured

- **The statement.** A format-v2 copy-on-write `UPDATE` whose predicate matches rows in two data files, for example
  `UPDATE … SET flag = 'upd' WHERE d <=> 0.0` on a two-file table that holds the `-0.0` row in one file and the
  `0.0` row in the other.
- **The refusal,** at planning: `AnalysisException: Error during planning: Mismatch between schema and batches`. The
  text is DataFusion's `MemTable::try_new` (`datafusion-catalog` 54.1.0, `memory/table.rs:95`, per Fable).
- **The rate on main**, 50 repeats per kind of that statement, each on a fresh two-file table (part B builder,
  main `276dc503`, #1307):

  | Kind | Refused |
  |---|---|
  | v2 copy-on-write | 26 of 50 |
  | v2 merge-on-read | 0 of 50 |
  | v3 | 25 of 50 |

  Part A's earlier run gave 27 and 29 of 50 on its base `68b09d18`, and 22 and 30 on its head; merge-on-read 0 on
  both ([item3/moves.md](../../wo/signed-zero-normalize-1/evidence/item3/moves.md)).
- **The first probe** (orchestrator, 2026-11-13, the same `276dc503` build): the Spark door, a memory Iceberg
  catalog, one table per attempt, two single-row `INSERT`s (two files), 30 repeats per cell, 27 cells, 810 attempts.
  Every attempt either refused with the same text or stored the truth rows. **No wrong answer in 810.** Refusals
  out of 30:

  | Statement | Layout | v2 copy-on-write | v2 merge-on-read | v3 (copy-on-write) |
  |---|---|---|---|---|
  | `WHERE d <=> 0.0` | mixed-nulls | **19** | 0 | **18** |
  | `WHERE i IS NULL OR i <= 7` | mixed-nulls | **12** | 0 | **18** |
  | `WHERE id IN (4, 6)` | mixed-nulls | 0 | 0 | 0 |
  | `WHERE flag IN ('f4', 'f6')` | mixed-nulls | 0 | 0 | 0 |
  | `WHERE id = 4` (one file touched) | mixed-nulls | 0 | 0 | 0 |
  | every statement | no-nulls | 0 | 0 | 0 |

  In the mixed-nulls layout one file's row has `i` NULL and `d` `-0.0`, and the other's has `i` -1 and `d` `0.0`. In
  the no-nulls layout neither file holds a NULL or a negative zero.

## What the probe shows

1. **It is not float-specific.** The integer predicate refuses as often as the float one, so the label's scalar scope
   reaches it.
2. **Copy-on-write only.** Merge-on-read never refused.
3. **It needs both files rewritten and a predicate over a column whose two files differ:** a NULL in one file and not
   the other (`i`), or `-0.0` against `0.0` (`d`). Predicates over `id` or `flag`, which also match both files,
   never refuse.

## First lead

Fable's first lead: the two files' rewritten batches may differ in schema, and the result depends on which batch reaches
the `MemTable` first; a rate near half on two files suggests that. The probe adds that each file may get its own
rewritten predicate (for example one simplified by its statistics), so the batches may differ in nullability or
type. Unmeasured.

## Likely cause, read on main (Fable guidance, 2026-11-13, checked by the orchestrator against main)

- `crates/repark-iceberg/src/write/predicate_dml/lineage.rs:162-185` (`project_update_data_batch`) builds each
  output field with `array.is_nullable()`, which is true only when that batch holds a NULL. Each batch's schema
  follows its data.
- `crates/repark-iceberg/src/write/predicate_dml/cow_commit.rs:97` (`register_update_values_table`) gives
  `MemTable::try_new` the first batch's schema. A NULL-free batch first and a batch with a NULL later fails the
  check; the other order answers. That fits copy-on-write only, never on the no-nulls layout, and a rate near one
  half.
- **Not explained yet:** why predicates over `id` or `flag` never refuse.

This is a likely cause, not a measured one. Fable's guidance puts the fix at high, not xhigh (not a ruling).

## Next step

A probe that prints each rewritten batch's schema and arrival order on the refusing cells, before any fix. The pin
is deterministic: two batches, the NULL-free one first. Then the owner picks a fix or a label sentence.

## Close

The refusal is explained by that probe, and then either fixed (both files' rows updated on every repeat, pinned on
v2 copy-on-write and v3) or named in the label's text, as the owner rules.
