# AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 — step 0 sketch (measured, no product code)

**Unit.** A grouped decimal `avg` answers differently from run to run on main, and with work stealing off it follows
the core count ([card](../../roadmap/mid-term/cards-2026-11-19/avg-decimal-grouped-run-variance-1-card-2026-11-19.md),
S2, not label-blocking). In scope with it: the ungrouped `avg` batch add that trusts the declared precision (#1360's
delta 3 path audit). Measured on main `6903fb48` (package `pkg-main-4c365814`), #1360's head `845766fd`
(`pkg-sum-845766fd`) for the `sum` side, the sizing's two prototypes, and Spark 4.1.2 at `local[1]` and `local[4]`.
`N` and `S` are the scratch roots of the brief; raw run files are under `N/avg-variance-out/`.

## 1. Spark's rule, from the jar

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` ([evidence/javap.md](evidence/javap.md)):

- **Buffer:** `sum` of type `decimal(min(p + 10, 38), s)` starting at zero, `count` a `long` starting at zero.
- **Update, per row:** `sum = DecimalAddNoOverflowCheck(sum, coalesce(cast(c as sum type), 0))`; `count + 1` unless
  `c` is NULL. The add itself checks nothing, and a NULL `sum` stays NULL (a binary operator on NULL).
- **The check is the buffer write.** In a grouped hash aggregate the buffer is an `UnsafeRow`, and
  `UnsafeRow.setDecimal` writes NULL when `changePrecision(p + 10, s)` fails. So the first row that takes a group's
  running sum past the buffer precision makes the group's sum NULL for good, whatever rows come later.
- **Merge:** `sum = DecimalAddNoOverflowCheck(left.sum, right.sum)`, written to the same kind of buffer, so a merge
  whose running total leaves the buffer is NULL too. A partial's output row goes through `UnsafeRowWriter.write`,
  which makes the same `changePrecision` check.
- **Evaluate:** `If(count = 0, NULL, DecimalDivideWithOverflowCheck(sum, count, result type, nullOnOverflow))`. A
  NULL sum raises `ARITHMETIC_OVERFLOW` (the `try_avg` hint) with ANSI on and answers NULL otherwise; `try_avg` and
  ANSI off answer NULL. A real sum is divided, rounded half up at `s + 4`, and a quotient past `p + 4` digits raises
  `NUMERIC_VALUE_OUT_OF_RANGE` (or NULL).
- **So Spark's answer follows the order rows are added in:** `M, 5, -M` overflows where `M, -M, 5` answers, and which
  rows a partial adds first, and in what order partials merge, is the split layout. The measurement below says how
  often `local[1]` and `local[4]` disagree.

**Measured** ([domain-table.md](domain-table.md), 4,720 answers, one answer on every run):

- **Grouped:** the per-row check holds. `t` g1 (`M, M, M / -M, -M, -M / 5 / M, -M`, exact `0.5556`), g7 and g8 are
  NULL / `ARITHMETIC_OVERFLOW` at both masters; g6 answers `0.2` at `local[1]` and NULL at `local[4]`.
- **Ungrouped:** no per-row check inside a partition (the buffer then is not an `UnsafeRow`); the partial's output
  row is checked once. So `local[1]` (one partial) answers the exact `0.5556`, `0.2`, `0`, `-0.2727`, and `local[4]`
  NULL or `ARITHMETIC_OVERFLOW`; `big` ungrouped `0.0167` against NULL.
- **Windows:** no buffer check at all: a whole-partition total past 38 digits reaches the divide and raises
  `NUMERIC_VALUE_OUT_OF_RANGE` (`t` g2, g3, g5 with ANSI on), where RePark raises `ARITHMETIC_OVERFLOW`.
- **Layout:** 48 of 1,396 `avg` (query, mode) pairs differ between `local[1]` and `local[4]` (20 ANSI off, 28 on), all
  at precision 38; the `sum` twins here, 20 and 23. At precision 28 and under, none.
- Spark refuses every over-precision file at the read (`NUMERIC_VALUE_OUT_OF_RANGE`).

## 2. Main, measured

- **96 of 16,940 `avg` cells vary** (31 query-mode pairs), every one at precision 38: `big` grouped (up to 4 answers
  of NULL-group counts), `big` ungrouped (2 cores and SET 2), `t` `c38_0` grouped all / `HAVING` / `FILTER` /
  `ROLLUP` / Iceberg CTAS (`ARITHMETIC_OVERFLOW` or `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on), `t` g1 ungrouped,
  `t4` g23 and g25. Stealing off: steady, but `big` grouped answers 3,000 / 398 / 437 / 220 NULL groups at 2 / 4 / 8
  / 10 cores; the exact average is `0.0167` in every group.
- **Steady but not exact:** the same families answer NULL or `ARITHMETIC_OVERFLOW` where the exact total fits
  (`big` ungrouped at every configuration but 2 cores and SET 2; `t` g1 / g6 / g7 / g8; `DISTINCT` at `c38_0` in 9
  cells). Nothing at precision 10 to 28.
- **Over-precision:** ungrouped `avg` / `try_avg` and the whole-partition window answer `1.25`, `1`, `-1.25`, `-1`
  on `wrap5` / `neg` at precision 19 and 20, every configuration, both modes (48 pairs); exact: past 38 digits.
- **`Decimal32`** (one probe): a `decimal32(9,2)` Parquet column averages to NULL / `ARITHMETIC_OVERFLOW` from
  `target_partitions` 2 where the exact answer is `0.625` (the `i32` partial wraps and ships the marker).
- #1360's head: its `sum` twins never vary in 4,360 cells and are exact; its `avg` varies as main's does.

## 3. The insertion inventory, in short

Full table: [inventory.md](inventory.md). Two families trust something they should not:

- **Row layout (the card's cause):** the grouped and ungrouped `state` (A8, A13) ship the partial's sum only when it
  is under `10^(p+10)` (`10^38` from precision 28) and a NULL marker otherwise; `merge_batch` (A9, A14) turns the
  marker into an overflow of the group or the query. Which rows meet in a partial follows DataFusion's file work
  stealing and `target_partitions`.
- **Declared precision:** the ungrouped and window batch add (A3–A7: `safe_rows`, `add_batch`, `sub_batch`) sums a
  batch with Arrow's wrapping `sum` and one carry whenever the declared precision says it cannot wrap. An
  over-precision Parquet value then wraps with no count.
- Exact already: the grouped row add (A12), `convert_to_state` (A16), and the one check in `average_parts` (A10, A11).
- Reuse: #1360's `exact_total` and `fold_batch` serve `avg`'s `Decimal128` path once five private helpers in
  `aggregate/sum_decimal.rs` become `pub(crate)` (or move unchanged to a shared file); they are `Decimal128`-only, so
  `avg`'s `Decimal32`, `Decimal64` and `Decimal256` natives keep the per-row carry.

## 4. Seats

**Seat A (leading; decision I for `avg`).** Exact sum in every phase, the state chosen by input precision, one check
at `evaluate`.

- **Product files (cap 4):** `crates/repark-functions/src/avg_decimal.rs` (the accumulators),
  `crates/repark-functions/src/aggregate.rs` (`state_fields`, `:248-285`), `crates/repark-functions/src/aggregate/sum_decimal.rs`
  (five helpers to `pub(crate)` and two error texts that name the function; or a pure move to a shared
  `aggregate/decimal_carry.rs`, the fifth file), and `avg_groups.rs` only if a test moves. New test file
  `aggregate/avg_precision_tests.rs` (`avg_decimal.rs` is at 961 lines, `aggregate.rs` at 959, ceiling 1,000).
- **Changes:** the ungrouped and window batch add uses #1360's `fold_batch` (`Decimal128`; checked four-lane total,
  per-row carry on overflow) in place of `safe_rows` / `add_batch` / `sub_batch`; `state` ships the raw low word (no
  `10^(p+10)` test, no NULL marker) plus the wrap count from precision 20; `merge_batch` adds both; the `overflowed`
  flag and its vector go; `evaluate` keeps `average_parts`' one check (`ARITHMETIC_OVERFLOW` past `10^(p+10)`,
  `NUMERIC_VALUE_OUT_OF_RANGE` past the result precision); `convert_to_state` ships raw values with zero wraps.
- **State schema** (`aggregate.rs:248-285`; DataFusion 54.1.0 passes `input_fields`,
  `datafusion-physical-expr-54.1.0/src/aggregate.rs:732-741`):

  | input | state fields |
  |---|---|
  | `Decimal128`, precision ≤ 19 | `count UInt64`, `sum decimal(p, s)` (raw `i128`; a counted wrap refuses, as #1360's narrow carrier) |
  | `Decimal128`, precision ≥ 20 | `count UInt64`, `sum decimal(p, s)` (low word), `wraps Int64` |
  | `Decimal32`, `Decimal64` | the wrap column at every precision (the narrow bound is an `i128` bound), or widen to `i128` before the add |
  | `Decimal256` | unchanged (`i256` does not wrap at these row counts) |

- **Cost cases** (the build measures; bars 1.05x unless noted):

  | row | today on main (AVG-DECIMAL-ROUND-1 cost) | prediction |
  |---|---|---|
  | K1 grouped `avg(d10)`, 200,000 groups | 0.0970 s | about 1.00x: same two columns; the `fits_sum` pass and the `overflowed` vector go |
  | K1 grouped `avg(d38)` (new held row) | not held today | **1.10x to 1.20x**: a third column on every shipped row (DataFusion skips partial aggregation at this cardinality; AVG-DECIMAL-ROUND-1 measured a third column at 1.19x on K1); **needs #1360's 1.12x exception at least** |
  | K3 ungrouped `avg(d10)`, `avg(d38)`, 10M rows | 0.0323 s, 0.0517 s | at most 1.02x: the four-lane checked add costs 3.41 ms against 2.91 ms per 10M rows (#1360 delta 3 bench) |
  | K2 window `avg(d10)`, `ROWS 3 PRECEDING` | 0.0759 s | about 1.00x to 1.03x |
  | C-012 `c012p6`, `c012rest` | 39.18 s, 42.85 s | about 1.00x (a reading, not measured here) |

- **Kept divergence:** the 81 pairs of [evidence/classes.md](evidence/classes.md): against `local[1]` 40 (24 exact
  value where Spark answers NULL, 8 where it raises, 8 another condition), against `local[4]` 75 (46, 16, 13). Every
  one at precision 38, none a different value, none where the exact answer is NULL or raises and Spark answers.
- **Answers it moves on main:** all 96 VARIES cells to one answer; the steady not-exact `c38_0` / `big` / `t4` cells
  to the exact answer; the 48 over-precision pairs to NULL / `ARITHMETIC_OVERFLOW` (or the narrow refusal in a
  multi-phase plan, as #1360's `sum`); the `Decimal32` cells to `0.625000` if that row is taken.

**Seat B (the cheap one; carrier (ii) for `avg`).** One sum column at every precision: ship the raw low word when
the partial did not wrap (no `10^(p+10)` test), the NULL marker only when its own total passed 128 bits; the same
`fold_batch` batch add.

- Files: `avg_decimal.rs` and `sum_decimal.rs` (visibility); no schema change. Cost: main's or less on every row.
- Not one answer: a partial holding two values near `10^38` (`2M ≈ 2 × 10^38 > 2^127`) still ships the marker. On
  this table that is `t` g1, g6, g7, g8 at `c38_0` (every crossing there is a multiple of `M`), so seat B fixes `big`
  and `t4` (`M + 5 < 2^127`) but not the four-file `t` cells; the sizing's carrier (ii) varied the same way on `sum`.

**Recommendation: seat A**, with the cost exception asked below; seat B only if the owner prefers main's cost on
the wide rows over one answer.

## 5. The unit split

- **One unit, one build round and one verifier** (the one-round cap). It edits #1360's `sum_decimal.rs`
  (visibility) and calls its `fold_batch`, so it **lands after #1360 merges**, on a branch from that main.
- **Pins:** a facade file over a four-file table at `target_partitions` 2, 4 and 8 set before the read, 20 runs,
  both modes, every precision of this table plus the 19 / 20 boundary, the over-precision files and the `Decimal32`
  column if Q4 takes it; Rust tests for the narrow refusal, the wide wrap column and a window retract.
- **If Q1 rules the over-precision add out:** part 1 is the marker (A8, A9, A13, A14, the schema); part 2, a small
  card, is A3–A7 with `fold_batch`.

## 6. Owner questions

1. **Does the ungrouped over-precision wrap belong here?** (a) yes, in this unit, by #1360's `fold_batch` (default:
   same file, same reuse, 48 measured pairs); (b) its own card. Cells: `over/{wrap5,neg}_p{19,20}[_two]/u/avg`,
   `/u/try_avg`, `/win/avg`, both modes, all 11 configurations (domain table, "Size").
2. **Is the kept divergence for `avg` accepted as `sum`'s was, and named in the label text?** (a) accept (default:
   81 pairs, all precision 38, no different value, and no steady rule matches Spark at both masters: 48 pairs differ
   between them); (b) mimic Spark's per-row grouped check (not steady across masters, and it keeps the race).
   Cells: [evidence/classes.md](evidence/classes.md).
3. **The cost bar for the wide grouped row (K1 `avg(d38)`).** (a) #1360's 1.12x for precision 20 and up, halt and
   report above it (default); (b) grant up to 1.20x now, from AVG-DECIMAL-ROUND-1's measured 1.19x for a third column;
   (c) seat B for precision 20 and up if the build measures past 1.12x. Rows: sketch §4, seat A cost table.
4. **`Decimal32` / `Decimal64` inputs:** (a) in this unit: the wrap column (or an `i128` widening) for those natives
   at every precision (default: the same seat, one probe shows a wrong NULL at `decimal(9,2)`); (b) its own card.
   Cells: [evidence/verdicts/narrow-main.jsonl](evidence/verdicts/narrow-main.jsonl).
5. **The window condition:** Spark's whole-partition window raises `NUMERIC_VALUE_OUT_OF_RANGE` past 38 digits where
   RePark raises `ARITHMETIC_OVERFLOW` (`t/c38_0/win/avg/g2`, `g3`, `g5`, ANSI on, steady, on main today). (a) leave
   it, a named condition class (default: not in this card's scope); (b) take it here, as #1360's `sum` windows do.

Up: [map.md](map.md).
