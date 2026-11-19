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
- **Merge:** `sum = DecimalAddNoOverflowCheck(left.sum, right.sum)`. A grouped merge writes it to an `UnsafeRow`,
  so a running total that leaves the buffer is NULL there too. **r1 correction:** the ungrouped final merge under
  whole-stage codegen keeps the sum in Java locals and never checks it, so `local[N]` can answer where the merged
  total passes 38 digits. A partial's output row goes through `UnsafeRowWriter.write`, which makes the same
  `changePrecision` check once per partial.
- **Which buffer decides the rule (r1, measured at `local[1]` and `local[4]`, [evidence/raw-r1/spark.md](evidence/raw-r1/spark.md),
  `javap` in [evidence/javap.md](evidence/javap.md)):**

  | operator | buffer | sum checked | measured (ANSI off; ANSI on reads AO for NULL) |
  |---|---|---|---|
  | grouped hash aggregate, codegen on or off | `UnsafeRow` | per row, a sticky NULL | `m_slide` g NULL at both masters (exact `0.5556`) |
  | ungrouped, whole-stage codegen (the default) | Java locals | once per partial, at its output row | `big` u `local[1]` `0.0167` |
  | ungrouped, **codegen off** (`TungstenAggregationIterator`) | `UnsafeRow` | **per row** | `big` u `local[1]` NULL; `m_w34` u `local[4]` NULL |
  | ungrouped **final merge**, codegen on | Java locals | **never** | `m_w34` u `local[4]` `9e33` (merged `1.8e38`); `local[1]` NULL |
  | `ObjectHashAggregate` (a `collect_list` beside it), under 128 groups | `SpecificInternalRow` | once per partial only | `m_slide` `0.5556`, `t4` g24 `1` at `local[1]`; ungrouped `m_w34` `9e33` at `local[4]` with codegen **off** too |
  | `ObjectHashAggregate` past 128 groups | sort-based, `UnsafeRow` | per row | `big` with `collect_list`: all 3,000 groups NULL |
  | window, whole partition, growing or sliding frame | `SpecificInternalRow` (`AggregateProcessor`) | **never**: the quotient only | `m_w34` `9e33` on every row, both masters, both modes; `t` g2, g3, g5 NVOOR |
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
  NULL or `ARITHMETIC_OVERFLOW`; `big` ungrouped `0.0167` against NULL. That holds with whole-stage codegen on; with
  it off the check is per row (`big` `local[1]` NULL). The merged total is never checked: five r1 tables whose
  partials fit and whose total does not answer the quotient at `local[4]` ([evidence/classes.md](evidence/classes.md)).
- **Windows:** no buffer check at all, and no check on the sum: only the quotient is checked. A total past 38
  digits answers when the average fits (`m_w34`, `9e33` at both masters, both modes, every frame shape) and raises
  `NUMERIC_VALUE_OUT_OF_RANGE` when it does not (`t` g2, g3, g5 with ANSI on), where RePark raises
  `ARITHMETIC_OVERFLOW` or is NULL in both. (Step 0 read the second case only; r1 corrects it.)
- **Layout:** 48 of 1,396 `avg` (query, mode) pairs differ between `local[1]` and `local[4]` (20 ANSI off, 28 on), all
  at precision 38; the `sum` twins here, 20 and 23. At precision 28 and under, none. An `ObjectHashAggregate` under
  128 groups drops the grouped per-row check (`t4` g24 `1` at `local[1]`, NULL from the plain hash aggregate), so
  Spark has no steady grouped rule either.
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

- **Product files (r1: cap 4 product files, two pure test moves, no codec change; §7 item 7):**
  `crates/repark-functions/src/avg_decimal.rs` (the accumulators), `crates/repark-functions/src/aggregate.rs`
  (`state_fields` at `:262-299`, the narrow return types at `:163-189`, `avg`'s window place in the rule at
  `:330-369`), `crates/repark-functions/src/avg_groups.rs` (the narrow arms of `create_groups`, `:44-65`) and
  `crates/repark-functions/src/aggregate/sum_decimal.rs` (five helpers to `pub(crate)` and two error texts that
  name the function). The build first moves `aggregate.rs`'s tests (`:454-989`) to `aggregate/avg_tests.rs` and
  `avg_decimal.rs`'s (`:689-961`) to `avg_decimal/tests.rs`, unchanged: `aggregate.rs` is at 989 of 1,000 lines at
  `d9eebe06`, not step 0's 959. New test file `aggregate/avg_precision_tests.rs`.
- **Changes:** the ungrouped and window batch add uses #1360's `fold_batch` (`Decimal128`; checked four-lane total,
  per-row carry on overflow) in place of `safe_rows` / `add_batch` / `sub_batch`; `state` ships the raw low word (no
  `10^(p+10)` test, no NULL marker) plus the wrap count from precision 20; `merge_batch` adds both; the `overflowed`
  flag and its vector go; `evaluate` keeps `average_parts`' one check (`ARITHMETIC_OVERFLOW` past `10^(p+10)`,
  `NUMERIC_VALUE_OUT_OF_RANGE` past the result precision); `convert_to_state` ships raw values with zero wraps.
  **r1:** a window (whole, growing or sliding frame) skips the sum check and checks only the quotient, in `i256`
  from the exact `low + wraps·2^128`, as Spark's window does (Q5 (b')); `Decimal32` / `Decimal64` widen to `i128`
  at update and take the `Decimal128` path and result type (Q4); `Decimal256` counts its wraps (Q6).
- **State schema, r1** (`aggregate.rs:262-299` at `d9eebe06`; DataFusion 54.1.0 passes `input_fields`,
  `datafusion-physical-expr-54.1.0/src/aggregate.rs:732-741`). The sum is declared `decimal(min(p+10, 38), s)`, as
  #1360's `SumCheck::data_type` (`aggregate/sum_decimal.rs:59-61`), not the input type. The schema depends only on
  the input type, which the distributed codec needs (it recomputes `state_fields` on the executor; §7 item 7):

  | input | state fields | result type |
  |---|---|---|
  | `Decimal32(p, s)`, `Decimal64(p, s)` | `count UInt64`, `sum Decimal128(p+10, s)` (raw `i128`, widened at update; no wrap column: rows × `10^18` < `2^127` up to about `1.7 × 10^20` rows) | `Decimal128(p+4, s+4)`, Spark's |
  | `Decimal128`, precision ≤ 19 | `count UInt64`, `sum Decimal128(p+10, s)` (raw `i128`; a counted wrap refuses, as #1360's narrow carrier) | `Decimal128(p+4, s+4)` |
  | `Decimal128`, precision ≥ 20 | layout I: `count UInt64`, `sum Decimal128(min(p+10, 38), s)` (low word), `wraps Int64`; layout D: `count UInt64` packing the count (low 40 bits) and the signed wraps (high 24 bits), `sum` (low word) | `Decimal128(min(p+4, 38), min(s+4, 38))` |
  | `Decimal256(p, s)` | `count UInt64`, `sum Decimal256(76, s)`, and the wrap count in the layout Q3 picks (Q6 (a)) | unchanged, `Decimal256(min(p+4, 76), …)` |

- **Cost cases** (the build measures; bars 1.05x unless noted):

  | row | today on main (AVG-DECIMAL-ROUND-1 cost) | prediction |
  |---|---|---|
  | K1 grouped `avg(d10)`, 200,000 groups | 0.0970 s | about 1.00x: same two columns; the `fits_sum` pass and the `overflowed` vector go |
  | K1 grouped `avg(d38)` (new held row) | not held today | **1.10x to 1.20x**: a third column on every shipped row (DataFusion skips partial aggregation at this cardinality; AVG-DECIMAL-ROUND-1 measured a third column at 1.19x on K1); **needs #1360's 1.12x exception at least.** r1: layout D (packed count, two columns) is measured first, beside layout I; §7 item 8 |
  | K3 ungrouped `avg(d10)`, `avg(d38)`, 10M rows | 0.0323 s, 0.0517 s | at most 1.02x: the four-lane checked add costs 3.41 ms against 2.91 ms per 10M rows (#1360 delta 3 bench) |
  | K2 window `avg(d10)`, `ROWS 3 PRECEDING` | 0.0759 s | about 1.00x to 1.03x |
  | C-012 `c012p6`, `c012rest` | 39.18 s, 42.85 s | about 1.00x (a reading, not measured here) |

- **Kept divergence (r1):** 98 pairs of [evidence/classes.md](evidence/classes.md): against `local[1]` 37 (24 exact
  value where Spark answers NULL, 8 where it raises, 5 another condition), against `local[4]` 92 (46, 16, 10, and
  **20 where the exact answer is NULL or raises and Spark answers**: the ungrouped final merge, sized on five
  tables). The three window pairs leave (Q5 (b')). Every pair at precision 38; none a different value.
- **Answers it moves on main:** all 96 VARIES cells to one answer; the steady not-exact `c38_0` / `big` / `t4` cells
  to the exact answer; the 48 over-precision pairs to NULL / `ARITHMETIC_OVERFLOW` (or the narrow refusal in a
  multi-phase plan, as #1360's `sum`); the `Decimal32` cells to `0.625000` if that row is taken. **r1:** the narrow
  cells to Spark's answers and types; the past-38-digit windows to the quotient; `m_d256` to NULL / AO. One residue
  stays: with ANSI on, a multi-group query whose groups raise different conditions (`t` all groups: g2, g3, g5
  `ARITHMETIC_OVERFLOW`, g9 `NUMERIC_VALUE_OUT_OF_RANGE`) reports the first group to fail, and that follows the
  evaluation order. Seat A makes every group's condition steady but not which one surfaces: 5 of step 0's 31
  VARIES pairs (`t` `c38_0` grouped all, `HAVING`, `FILTER`, `ROLLUP`, Iceberg CTAS; 43 cells) keep an
  AO-or-NVOOR answer (a reading, Q2).

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
- **r1 pins** (every cell measured on main, [evidence/raw-r1/pins.md](evidence/raw-r1/pins.md),
  [evidence/raw-r1/main.md](evidence/raw-r1/main.md), [evidence/raw-r1/frames.md](evidence/raw-r1/frames.md)):

  | pin | setting | main today | after the build |
  |---|---|---|---|
  | spill: one file, 200,000 groups (`M, M` then `-M, -M, 5`), `count(a)`, `min(a)`, `max(a)` | `target_partitions` 1, pool 16 MB (ANSI off) and 24 MB (`avg`, both modes); 32 MB does not spill | 0 of 200,000 non-NULL; AO | 200,000, every group `1.0000` |
  | skip-partial (`convert_to_state`), `big` | SET 4, `skip_partial_aggregation_probe_rows_threshold = 1`, `probe_ratio_threshold = 0.0` | 3,000 of 3,000 exact (default SET 4: 352 to 645 NULL groups, varies) | 3,000 exact, both settings |
  | `GROUPING SETS ((g), ())` on `big` | SET 1, 2, 4, 8 | VARIES (0 to 646 NULL rows; AO) | 3,001 rows, none NULL |
  | correlated subquery on `t4` | SET 1, 2, 4, 8 | VARIES at SET 2 | g23 `1.6667`, g24 `1`, g25 `1.6667` (ANSI off) |
  | DataFrame door, `spark.table("big").groupBy("g").agg(F.avg("c"))` | SET 1, 2, 4, 8 | VARIES at SET 4 and 8 | 3,000 exact |
  | Iceberg v2, RePark-written, a position-delete file | SET 1, 2, 4, 8 | exact at SET 1, g23 / g25 NULL from SET 2 | the exact answer at every SET |
  | narrow natives `m_d32`, `m_d64` (item 3) and `m_d256` (item 4) | SET 1, 2, 4, 8, both modes | see §7 | Spark's answer and type; `m_d256` never `0.4167` |
  | windows `m_w34` (whole, growing, 15,000-row sliding) and `t` g2, g3, g5 | SET 1, 4 | NULL / AO | `9e33` on every row; NVOOR |

  `try_avg` with ANSI on exhausts a 16 or 24 MB pool (`Resources exhausted`, 5 of 5) today; it is not a pin here.
- **#1360's `sum` pins re-run unchanged** (the analyzer rule routes both functions): `aggregate/sum_tests.rs` and
  `aggregate/sum_precision_tests.rs` whole, including the phrase pins "past its declared precision" at
  `sum_precision_tests.rs:73`, `sum_tests.rs:663` and `:672` and the window test `sum_tests.rs:198`
  (`an_ungrouped_total_is_checked_once_at_evaluate_and_a_window_at_every_row`), and
  `python/repark/tests/types/test_sum_dec_i128wrap_1.py` (the phrase at `:240`, its windows). The measured `sum`
  window cells stay put: `m_w34` and `t` g2, g3, g5 raise NVOOR (ANSI on) or are NULL on #1360's head, as in Spark.
- **If Q1 rules the over-precision add out:** part 1 is the marker (A8, A9, A13, A14, the schema); part 2, a small
  card, is A3–A7 with `fold_batch`.

## 6. Owner questions (restated in r1; each default re-checked against the r1 cells)

1. **Does the ungrouped over-precision wrap belong here?** (a) yes, in this unit, by #1360's `fold_batch` (default:
   same file, same reuse, 48 measured pairs); (b) its own card. Cells: `over/{wrap5,neg}_p{19,20}[_two]/u/avg`,
   `/u/try_avg`, `/win/avg`, both modes, all 11 configurations (domain table, "Size"). **Re-checked:** the critic
   re-measured the over-precision files (224 of 840 steady and not exact, the same families); no r1 cell bears on
   it. Default stands.
2. **Is the kept divergence for `avg` accepted as `sum`'s was, and named in the label text?** (a) accept (default);
   (b) mimic Spark's per-row grouped check. r1 classes ([evidence/classes.md](evidence/classes.md)): 98 pairs, all
   at precision 38, none a different value; against `local[1]` 37 (24 exact value / Spark NULL, 8 / Spark AO, 5
   other condition), against `local[4]` 92 (46, 16, 10, and **20 where the exact answer is NULL or AO and Spark
   answers**, the ungrouped final merge on five tables). (b) is rejected: Spark's grouped rule is per row in a hash
   aggregate and per partial in an `ObjectHashAggregate` under 128 groups (`t4` g24), so it has no steady rule to
   mimic. The label also names the residue of §4: a multi-group ANSI-on query whose groups raise different
   conditions reports either. **Re-checked:** the default holds; the new class is the only addition and Q7 decides
   it.
3. **The cost bar for the wide grouped row (K1 `avg(d38)`).** (a) #1360's 1.12x for precision 20 and up, halt and
   report above it; (b) grant up to 1.20x now, from AVG-DECIMAL-ROUND-1's measured 1.19x for a third column; (c)
   seat B for precision 20 and up if the build measures past 1.12x; **(d) the packed count first**: layout D (no
   third column; the count's high 24 bits hold the partial's signed wraps, a refusal when the count passes `2^40` or
   the wraps `2^23`), measured beside layout I (the `Int64` column). **Default: (d) then (a):** the build's first cost
   step (§7 item 8) takes the cheaper layout that keeps one answer within 1.12x on K1 `d38` and 1.05x elsewhere,
   and halts and reports if neither does. Seat B stays the last fallback (not one answer: `t` g1, g6, g7, g8).
   **Re-checked:** no r1 cell measures cost; the default moved from (a) to (d)+(a) on the critic's 1.19x reading.
4. **`Decimal32` / `Decimal64` inputs, and the return type.** (a) in this unit: widen to `i128` at update, ship
   `Decimal128(p+10, s)` with no wrap column, and **return Spark's `decimal(min(p+4, 38), s+4)` as `Decimal128`**
   (default); (b) a wrap column on the narrow natives, keeping today's capped `Decimal32(9, s+4)` /
   `Decimal64(18, s+4)` result (more code, and a steady wrong NULL / NVOOR at one partition); (c) its own card.
   Cells: `m_d32`, `m_d64` at SET 1, 2, 4, 8 ([evidence/raw-r1/main.md](evidence/raw-r1/main.md)): all rows exact at
   SET 1 only today; the `id < 3` / `id < 10` cells NULL, NVOOR or AO at every setting (the capped type); type
   `decimal(9,6)` / `decimal(18,4)` against Spark's `decimal(13,6)` / `decimal(22,4)`.
   **Re-checked:** the default changed to (a) as stated; four existing Rust pins of the capped type change with it
   (§7 item 3).
5. **The window condition.** (a) leave it, a named condition class; (b) take it here with one sum check at
   `evaluate`, as step 0 read #1360's `sum` windows; **(b') the window checks only the quotient, as Spark's window
   does** (default). Cells: `m_w34` window `9e33` at both masters, both modes, whole, growing and sliding frames;
   `t/c38_0/win/avg/g2`, `g3`, `g5` NVOOR (ANSI on) at both masters; main AO / NULL in all of them. (b') is
   Spark-equal and layout-free; (b) would add `m_w34`'s windows to the new class. **Re-checked:** (a)'s premise (a
   total past 38 digits always raises in Spark's window) is refuted by `m_w34`; default changed to (b').
6. **`Decimal256` (new).** Spark refuses the read (`DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`), so there is no
   Spark answer. (a) **count wraps** (`i256` `overflowing_add` into a wrap count, shipped as Q3's layout; a nonzero
   count puts the total past `2^255 > 10^76`, so `evaluate` answers NULL / AO in every place, windows included)
   (default: one answer for every input); (b) refuse past `i256` with a checked add (no new column, but not one
   answer: whether an intermediate sum passes `2^255` follows the order rows meet, as `M, M, -M, -M` shows); (c)
   refuse the read as Spark does, DECIMAL256-SUM-WRAP-1's scope (#1366), which would make this arm unreachable from
   Parquet but not from an Arrow input. Pin: `m_d256` (exact total `2^256 + 5`), today `0.4167` in every window and
   VARIES elsewhere; after (a) NULL / AO, the window NVOOR / NULL, never `0.4167`.
7. **Does the ungrouped `avg` follow Spark's unchecked final merge (new)?** (a) **no**: keep the exact total and its
   one check (default, the critic's lean). It equals Spark `local[1]` in every measured ungrouped cell and is
   steady; it keeps the new class against `local[N]` (20 pairs here). (b) mimic it: check each partial and never
   the merged total. That follows RePark's own partial layout, which is the card's cause, so it reintroduces the
   variance. (c) never check the ungrouped sum (quotient only, as the window): steady, equal to `local[4]` on the
   five tables, but different from `local[1]` there and from Spark wherever one partial passes 38 digits. Cells:
   the five tables' `u/avg` and `u/try_avg`, both modes ([evidence/classes.md](evidence/classes.md)).
   **Re-checked:** the default rests on r1's tables and holds.

## 7. Revision (critic round 1)

The critic ([evidence/raw-r1/critic-verdict.md](evidence/raw-r1/critic-verdict.md), Opus 5.5 medium) returned
UNSOUND with a narrow repair of eight items; seat A's core stands (an exact grouped sum, state by precision, one
check at `evaluate`). Each item is closed here with r1's measurement ([evidence/raw-r1/](evidence/raw-r1/map.md),
raw runs `N/avg-revision-out/`); §1, §4, §5 and §6 are amended in place.

1. **Spark's rule (§1).** Rewritten with the operator table. Measured at `local[1]` and `local[4]`, both modes,
   codegen on and off: the ungrouped final merge is unchecked under codegen (`m_w34` u `local[4]` `9e33`, NULL
   with codegen off); with codegen off the ungrouped sum is checked per row (`big` u `local[1]` NULL against
   `0.0167`); an `ObjectHashAggregate` under 128 groups drops the grouped per-row check (`m_slide` `0.5556`, `t4`
   g24 `1` at `local[1]`) and its ungrouped form answers at `local[4]` with codegen off too; past 128 groups it is
   per row again (`big`, all 3,000 NULL); the window checks only the quotient. `javap`: `AggregateProcessor.buffer`
   and `ObjectAggregationIterator`'s buffers are `SpecificInternalRow`, the fallback is `sipush 128`
   ([evidence/javap.md](evidence/javap.md)).
2. **Seat A's window path.** The quotient only, from the exact `i256` sum, in every frame shape. Pins: `m_w34`
   window `9e33` (whole partition; growing and 15,000-row sliding frames, all 20,000 rows; main NULL past row
   11,111) and `t` g2, g3, g5 NVOOR. **#1360's `sum` windows are not moved:** Spark's `sum` window checks the sum
   because the sum is its result (`m_w34` and `t` g2, g3, g5: NVOOR / NULL at both masters), and #1360's head
   answers the same at SET 1, SET 4 and 2 cores. The change is `avg`'s own arm in `legacy_avg_expr`
   (`aggregate.rs:341-369`): a place on `SparkAvgWithRetract`, set to the window in both modes; `spark_sum::placed`
   and `spark_sum::legacy` are untouched, and #1360's pin files re-run unchanged (§5).
3. **The narrow natives.** Widened as Q4 (a): state `Decimal128(p+10, s)`, no wrap column, result
   `Decimal128(p+4, s+4)`. Main today ([evidence/raw-r1/main.md](evidence/raw-r1/main.md)): `m_d32` all rows
   `0.625` at SET 1, VARIES at SET 2, NULL / AO at SET 4 and 8 (the window `0.625` everywhere), and `id < 3` NULL,
   NVOOR or AO at every setting and in the window (`9999999.99` does not fit the capped `decimal(9,6)`); `m_d64` the same
   with `0.2381` and `999999999999999999` (`decimal(18,4)`). Spark: `0.625`, `9999999.99`, `0.2381`, `999999999999999999` at both
   masters, types `decimal(13,6)` and `decimal(22,4)`. Pins at SET 1, 2, 4, 8, both modes, with `typeof`. Four
   existing pins of the capped type change on purpose: `group_avg_decimal32_stays_decimal_9_6_i32` and
   `group_avg_decimal64_stays_decimal_14_6_i64` (`aggregate.rs:662`, `:688`, moving to `aggregate/avg_tests.rs`),
   `decimal32_groups_update_then_evaluate` and `decimal64_groups_update_then_evaluate` (`avg_groups.rs:759`, `:780`).
4. **`Decimal256`.** Rule: Q6 (a), count wraps. Today `m_d256` answers `0.4167` in every window (steady) and
   `0.4167` / NULL / AO elsewhere (VARIES at SET 2, 4, 8 and 2 cores); Spark refuses the read. Pin: NULL / AO
   (`try_avg` NULL), the window NVOOR / NULL, never `0.4167`.
5. **The pins list.** §5's r1 table: spill (16 and 24 MB, `target_partitions` 1, 200,000 groups, every group
   `1.0000`), skip-partial (probe rows 1, ratio 0.0, SET 4), `GROUPING SETS`, the correlated subquery, the
   DataFrame door and the Iceberg v2 table with a position-delete file (`file_path,pos`), at SET 1, 2, 4 and 8.
   Each was measured on main; each varies or is not exact today except skip-partial, which is exact already and
   pins `convert_to_state`'s zero wraps.
6. **The kept divergence.** Restated in [evidence/classes.md](evidence/classes.md) and §4 with the new class, per
   master: `local[1]` 0, `local[4]` 20 on five tables. The VARIES count is a lower bound: `t` g6 (`try_avg`, 2 cores)
   and `t4` g21 (ANSI on, 2 cores; NVOOR×18 / AO×2 here) are added to the domain table.
7. **The file plan.** `aggregate.rs` is 989 lines at `d9eebe06` and `avg_decimal.rs` 961 (ceiling 1,000, no
   exception row). The build's first commit is a pure move: `aggregate.rs`'s `mod tests` (`:454-989`) to
   `aggregate/avg_tests.rs` and `avg_decimal.rs`'s (`:689-961`) to `avg_decimal/tests.rs`. That leaves about 455 and
   690 lines; the new cap is **every touched file under 1,000 with no new exception row** (seat A needs about 40,
   60 and 10 lines in `aggregate.rs`, `avg_decimal.rs` and `avg_groups.rs`). Product files: the four of §4; tests:
   the two moved files, `aggregate/avg_precision_tests.rs` and a facade file. **The codec does not change:**
   `crates/repark-distributed/src/codec.rs:121-127` ships a UDAF by name only, and the executor rebuilds the
   aggregate from its registry and recomputes `state_fields` ([inventory.md](inventory.md), C1, C2), so the new
   state round-trips as long as it depends only on the input type, which it does. The same path does not carry
   `avg`'s place, its ANSI-off `Legacy` mode or #1360's `sum` place (a reading, outside this unit). The state sum is
   declared `decimal(min(p+10, 38), s)`, as `SumCheck::data_type`.
8. **Cost: the build's first cost step.** Before the product code is final, the build makes both layouts and
   measures them with AVG-DECIMAL-ROUND-1's harness (`task/wo/avg-decimal-round-1/evidence/built/cost.md`: `k1`,
   200,000 groups, `d10 DECIMAL(10,2)`, `d38 DECIMAL(38,0)`): K1 grouped `avg(d38)` and `avg(d10)`, main against
   layout D (packed count) and layout I (`Int64` column), **5 alternating runs** (main, D, I, then main again as the
   control band), the per-query median. **Rule:** take the cheaper layout on K1 `d38` that keeps one answer, if it is
   within **1.12x on K1 `d38` and 1.05x on K1 `d10`**, then on K2, K3 and C-012 at 1.05x; a ratio over its bar but
   inside main's own control band is re-run (5 more alternations) before it counts. If neither layout fits, halt and
   report (Q3). `d10` has no wrap state in either layout, so K1 `d10` measures only the shared change.

## 8. Fable's guidance (2026-11-19)

Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC: "Go with your recommendations and tonight
you don’t even need to wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good
luck". This section is guidance, not a ruling. **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder
(file `Fable reply — avg grouped run variance step 0 2026-11-19.md`). Fable's marks are kept: **[M]** measurement,
**[R]** reading, **[Rec]** recommendation.

### §1: the revision and the critic's eight items

- **The eight items are closed.** **[R]** The revision closes all eight of the critic's items.
- **No second critic round.** **[Rec]** Skip it. The six additions below go into the build brief instead.
- **The six additions for the build brief** **[Rec]**, none needing a critic round:
  - (a) a DataFrame-door window pin, `F.avg("c").over(...)` on `m_w34`, expecting `9e33`; an `avg` window with no place
    set falls back to the sum check, which over-refuses but never answers a wrong value;
  - (b) the `Decimal256` window condition is NVOOR; with ANSI off and `try_avg` it is NULL;
  - (c) the spill pin asserts a nonzero spill count from the plan metrics, and a Rust test drives `state` into
    `merge_batch` directly as the primary pin for the wrap column;
  - (d) the packed layout's two refusals get Rust pins at their boundaries;
  - (e) the residue pin asserts SQLSTATE `22003` and a condition in the set of two;
  - (f) two of the critic's side findings, the `CUBE(g, id % 2)` planning failure and the `sum` at SET 4 under a 3 MB
    pool, need cards.

### Q1 to Q7

**[Rec]** Adopt every default, with one change: build layout D only, and drop layout I.

| Q | ruling | why |
|---|---|---|
| Q1, the over-precision wrap is fixed in this unit | (a) | Same file and same helper as the marker fix. The critic reproduced the cells (224 of 840). |
| Q2, accept the kept divergence (98 pairs, with the new ungrouped class) | (a), with the owner note below | **[R]** The 78 older pairs are the class the owner accepted for `sum`. **[R]** The 20 ungrouped pairs run the other way, and four facts make it safe to proceed: main already answers NULL / AO in all five tables; Spark disagrees with itself there (`local[1]`, and `local[4]` with codegen off, give NULL / AO); it is never a wrong value; and it needs a total past `10^38` with an average under `10^34`. |
| Q3, measure the packed count beside the `Int64` column | (d), changed: build D only | **[R]** Layout D ships the same two columns at the same widths as main, and removes the `fits_sum` pass and the `overflowed` vector; **[R]** about 1.00x on K1 `d38` expected. **[Rec]** Measure D against main with the same five alternating runs; within 1.12x on K1 `d38` and 1.05x elsewhere, take it; otherwise halt and report. Seat B stays the last fallback. Layout I returns only if a verifier shows the wraps refusal is reachable from declared-precision data at a plausible size. |
| Q4, widen the narrow natives to a `Decimal128` state and Spark's result type | (a) | The capped result type is a steady wrong NULL at one partition, so it cannot wait for a card. |
| Q5, the window checks the quotient only, as Spark does | (b') | Spark's window rule is steady at both masters and both modes, so equality is available here. |
| Q6, `Decimal256` counts wraps | (a) | Counting wraps is also what keeps a sliding frame exact after a retract. |
| Q7, the ungrouped `avg` keeps the exact total | (a) | It is the same rule as `sum` and as the grouped path, and it equals Spark `local[1]`. **[R]** If the owner later prefers (c), it is one check in the ungrouped `evaluate`; nothing built under (a) is lost. |

**For the owner's morning read:** the 20-pair ungrouped class (Spark answers where RePark raises or answers NULL; main
does the same today; no wrong value), which the `sum` acceptance does not cover.

### The residue: which condition surfaces

- **[R]** It can be named as a kept residue. The query raises on every run. Both conditions are correct errors of the
  same query, and both carry SQLSTATE `22003`. No run answers a value.
- **[R]** There is no cheap rule. The groups that raise different conditions sit in different final partitions after
  the hash repartition, and the first partition to fail wins. A precedence rule inside one `evaluate` call cannot order
  two partitions.
- **[R]** Collapsing the two conditions into one would be deterministic, but it would move cells that equal Spark today
  (a lone group whose quotient overflows raises NVOOR in both engines). That trade is worse.
- **[Rec]** Keep it and name it in the label line. The pin asserts "raises, SQLSTATE `22003`, condition in
  {`ARITHMETIC_OVERFLOW`, `NUMERIC_VALUE_OUT_OF_RANGE`}". Do not spend build time on a rule.
- The figures "5 pairs, 43 cells" are the orchestrator's, from the questions file. The reply does not restate them.

### The packed count

- **[R]** The packed count is sound. Nothing outside the accumulator reads the count column, and the schema still
  depends only on the input type, so spill, skip-partial and the codec are unaffected.
- **[R]** It has two refusal edges, and both are loud:
  - a count past `2^40`: about `1.1 × 10^12` rows in one group in one shipped state. A spill in the final stage ships
    merged groups, so this bounds the whole group, not just one partial;
  - net wraps past `2^23`: by Fable's arithmetic, about `2.85 × 10^7` same-signed rows near `10^38` in one group in one
    partial (`2^151 / 10^38`). The hand-back says about 14 million; either figure is far from real data.
- **[R]** The wraps refusal is not one answer at that edge. One partial holding 30 million `+M` rows and then 30 million
  `-M` rows nets to zero wraps and answers. Split across two partials, the first refuses. Layout I does not have this
  edge.
- **[R]** The 40 / 24 split is reasonable. A partial that passes the wraps limit has a total near `2.9 × 10^45`, so the
  answer is NULL / AO anyway unless another partial cancels it.

### The order

- **[Rec]** Do not build this unit tonight. Lane B takes the STRING-HOUR-MINUTE-SECOND-1 step 0 after the verifier.
- **The `avg` build runs in the next window, never alongside the SUM-BIGINT-ANSI-OVERFLOW-1 build.** **[R]** The two share
  the registration and the analyzer rule in `aggregate.rs`, which is at 989 of 1,000 lines. Whichever goes first carries
  the pure test move as its first commit.

### Clerk's notes (not Fable's guidance)

- **Filed:** the codec card of §5 as DISTRIBUTED-UDAF-INSTANCE-1, on the cards map (S2 pending its own step 0; does not
  touch the label).
- **Not filed: HADOOP-NEW-SESSION-READ-1.** Fable's §5 rating for the hadoop-catalog reading is S3 pending measurement, a
  loud refusal, not touching the label. The filing check found that the shape is already carded as HADOOP-FACADE-FRESH-SESSION-1
  (`task/roadmap/mid-term/hadoop-facade-fresh-session-1-card-2026-11-02.md`, appended 2026-11-13): a `type=hadoop`
  catalog opened in a new session over a warehouse another RePark process wrote reports `SCHEMA_NOT_FOUND`. The two cards
  the brief named do not carry it, so no card was filed and nothing was appended to the carded one. The owner or the
  orchestrator decides whether that card takes a dated paragraph.
- **§1 (f):** the two side findings already have cards from #1366: CUBE-GROUPING-EXPR-PLAN-1 and SPILL-SMALL-POOL-STALL-1
  (both on the cards map). No card is filed here for them.

Up: [map.md](map.md).
