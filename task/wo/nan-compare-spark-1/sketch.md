# NAN-COMPARE-SPARK-1: the design sketch

**Date:** 2026-11-13. **Author:** a design-sketch lane (`claude-opus-5-5`, xhigh), documents and probes only.
**Source:** the owner's ruling of 2026-11-13 ("Defaults", item 3: this unit first after the scoped label) and
Fable's guidance adopted the same day (an xhigh sketch before any build; a truth ruling owed on pruning).
**Card:** [NAN-COMPARE-SPARK-1](../../roadmap/mid-term/nan-compare-spark-1-card-2026-11-13.md). Evidence:
[domain-table.md](domain-table.md) (`§n` below), [inventory.md](inventory.md) (seats `A1`…`H5`).

## 1. The fault

Spark compares every NaN as one value, equal to itself and larger than `+Infinity`, whatever its sign bit or
payload. RePark compares floats by Arrow's totalOrder (inventory A1): a sign-set NaN (`fff8…`) is the smallest
value, a payload NaN (`7ff8dead…`) is above the canonical one, and two NaNs with different bits are unequal.
Both engines keep a sign-set NaN in memory: over a column, `sqrt(-1)`, `-x`, `Infinity - Infinity`,
`0 * Infinity`, `Infinity / Infinity` and `Infinity % 1` give `fff8…` on Spark too (§7, `jvm_raw`). The
difference is only in how the two compare it.

## 2. The headline

- **15,351 cells at ANSI on, 4,061 at ANSI off**, on four doors, 29 sources (in memory, computed, 18 Iceberg
  tables from two writers in three formats and two layouts plus six partitioned, six plain Parquet sets, a
  native-written table) and 53 operations (§1).
- **Wrong cells, ANSI on:** base 7,095, NAN-WRITE-CANONICAL-1 (1325) 7,001, part B (head3) 4,685. No NaN
  verdict moves with the ANSI mode (§8).
- **Part B already fixes** sort (`ORDER BY`, TopK, window `ORDER BY`), keys (`GROUP BY`, `DISTINCT`, joins) and
  column-to-column comparisons, on every NaN shape (§2, inventory H3).
- **Still wrong on head3:**
  - a sign-set NaN in `max`/`min` (ungrouped and every window form), `greatest`/`least`, every comparison with
    a literal (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `IN`, `BETWEEN`, a searched `CASE`), and the array
    functions;
  - a payload NaN in `= NaN`, `<> NaN`, `<=> NaN`, `IN (NaN)` and `> NaN`;
  - **grouped `max`/`min`, for every NaN shape, canonical included** (§3; new);
  - `max` over one plain Parquet file holding NaN, answered from statistics without the NaN (§6; new).
- **NAN-WRITE-CANONICAL-1 moves only the table RePark writes itself** (§5, `nat_parquet_l5`); Avro tables and
  tables written before it keep their bits, so every stored-NaN cell stays wrong (NWC verifier 2, F2).

## 3. What the folded cards turn out to be

- **PARQUET-NAN-EQUALITY-1:** not a RePark read fault. pyarrow and RePark write Parquet min/max without NaN;
  Spark's `parquet-mr` 1.16 writes NaN as the max. On a single RePark or pyarrow file, `d = NaN` and
  `d > Infinity` keep nothing **on Spark too**; on Spark's file both engines keep the rows (§6, `pq_*_l1`).
  The card compared RePark on RePark's file with Spark on Spark's. RePark's mechanism is DataFusion's
  `PruningPredicate` (inventory F2); Spark's is `parquet-mr`'s statistics filter. This is the pruning question
  (§6 below), not a compare seat.
- **ICEBERG-NAN-FILE-PRUNE-1:** the PRUNED-WRONG class. On the same five files, Spark-on-Iceberg and RePark both
  answer `d > 0.5` with `[1, 3]`, dropping the NaN rows that Spark's row semantics keep (`[1, 2, 3, 9]`):
  Iceberg's metrics evaluator reads `NaN > x` as false, in Java and in the fork's port alike (inventory G2).
  With a canonical NaN RePark's pruning **is** Spark-on-Iceberg's on every measured layout.
- **The Avro partition-column oddity:** Spark's `pk > Infinity` keeps nothing on its own Parquet, ORC and Avro
  tables alike. The manifest's partition summary has upper bound `Infinity` (NaN left out, `contains_nan`
  true) and the manifest evaluator skips the manifest without reading `contains_nan` (inventory G3). The
  non-partition `f` answers `[1..6]` because Avro data files carry no column metrics, on either writer.
- **The label exclusion understates the gap.** Its pending text ("`max`, `min`, `greatest`, `least` and
  non-key comparisons over a NaN computed in the same query") misses stored non-canonical NaNs (Avro, old
  tables, other writers), the array functions, the payload cases and grouped `max`/`min` over any NaN. A
  measured text is question Q6.

## 4. Seats, one family at a time

Each family has at most two proposals; **(a)** is the recommendation. "Cost cases" are the rows a build runs
at 1.05x on plans **without** NaN (release build, the C-012 harness rules, median of runs).

### 4.1 Comparisons with a literal (inventory E1–E4; F2, G1–G6 follow)

- **(a) Total-order bounds, in part A's literal seat.** Part A (`df_guards/signed_zero/literals.rs`) already
  rewrites float comparisons against zero literals on every door, as an analyzer rule and again after each
  simplification. Extend it to every float comparison with a literal `c`:
  - non-NaN `c`: `e > c` → `e > c OR e < -Infinity`; `e >= c` → `e >= c OR e < -Infinity`;
    `e < c` → `e >= -Infinity AND e < c`; `e <= c` → `e >= -Infinity AND e <= c`; `=`/`<>` unchanged. Under
    totalOrder every NaN lies above `+Infinity` or below `-Infinity`, so these are exactly Spark's answers for
    every NaN shape, and each added term is a plain comparison that DataFusion's `PruningPredicate` and the
    fork's converter push down as before;
  - NaN `c`: `= NaN`, `<=> NaN`, `>= NaN` → `isnan(e)` (null-preserving where the form needs it); `<> NaN`,
    `< NaN` → `NOT isnan(e)`; `<= NaN` → `e IS NOT NULL`-preserving true; `> NaN` → false; `IN (NaN, …)` →
    `isnan(e) OR e IN (…)`; `NOT IN` its complement. The fork already pushes `isnan` as `is_nan` (G1);
  - `BETWEEN` and a searched `CASE` expand first, as part A does for zeros.
  - **Trade-offs:** no new kernel, no UDF, no pushdown lost; one extra comparison and an `OR`/`AND` per float
    literal comparison. Pruning stays what it is today, so on the same files RePark answers as
    Spark-on-the-same-files (§6) — the "exact" reading of the pruning question. For the "sound" reading the
    `e < -Infinity` term becomes `isnan(e)` (same seat, one token), which keeps NaN files on Iceberg (NaN counts)
    but makes `>`/`>=` unprunable on plain Parquet (no NaN counts in parquet-rs 58).
  - **Risk to check in the build:** DataFusion's simplifier and interval analysis must leave `e < -Infinity`
    alone (no range fold to false). Measure it in step 0 with `EXPLAIN VERBOSE`.
  - **Cost cases:** `count(*) WHERE d > c`, `d < c`, `d BETWEEN a AND b` over 10 M DOUBLE rows in memory, from a
    100-row-group Parquet file and from a 100-file Iceberg table; a TPC-H Q6-shaped filter; and the pruning
    rows: files and row groups scanned, before and after, which must be equal.
  - **Fork or upstream:** neither.
- **(b) Canonical wrap.** Wrap the float operand in part B's `canonical_f64` call, as part B does for
  column-to-column comparisons. Uniform with part B, but a wrapped column is no longer `column op literal`, so
  every float predicate stops pushing down (Iceberg file pruning and Parquet row-group pruning both lost) unless
  the fork's converter and DataFusion's pruning learn the call. Rejected as the default on cost.

### 4.2 `max` and `min` (inventory B1–B6)

- **(a) RePark's own float `max`/`min`.** A UDAF in `repark-functions` registered under `max`/`min` on every
  door; for FLOAT and DOUBLE it answers Spark's way, and it delegates every other type to DataFusion's:
  - ungrouped: `max` is NaN if any input is NaN, else Arrow's `max`; `min` ignores NaN unless every value is NaN;
  - grouped: its own `GroupsAccumulator` with a NaN-greatest comparator, which fixes B2 for every shape;
  - sliding windows: `MovingMax`/`MovingMin` over a NaN-greatest key;
  - `value_from_stats`: none for FLOAT/DOUBLE, because Parquet's exact min/max leave NaN out (B5);
  - `array_min`/`array_max` reuse the batch kernel through RePark's dispatch (`function_dispatch.rs:444-451`).
  - **Trade-offs:** one owned kernel per accumulator kind; the result's NaN is `7ff8…` (Spark returns the
    input's bits; Q5). The statistics shortcut is given up for float columns of plain Parquet files only (the
    fork's Iceberg scan never offered it).
  - **Cost cases:** ungrouped `max`/`min(d)` over 10 M rows; grouped by 1 K and by 1 M keys; a running and a
    sliding window `max`; `array_max` over 1 M arrays of ten doubles; and, as a separate row not held to 1.05x,
    `SELECT max(d)` over a Parquet file with statistics (a metadata answer becomes a scan; Q4).
  - **Fork or upstream:** neither. B2 is a DataFusion bug worth reporting upstream (Q8), but this seat does
    not wait for it.
- **(b) Argument wrap.** Extend part B's `aggregate_call` to wrap `max`/`min` arguments in a NaN-only
  canonicaliser. It fixes B1, B3, B4 and removes the shortcut (a call is not a `Column`), but not B2 (a
  canonical NaN still makes `partial_cmp` return `None`), so it needs an upstream fix or a patched DataFusion,
  and it adds a per-row call to every float `max`/`min`.

### 4.3 `greatest` and `least` (inventory D1)

- **(a) RePark's own float `greatest`/`least`**, bound in `function_dispatch.rs:290-297` and on the SQL doors,
  delegating non-float types: NaN is greatest; NULLs skipped as today. Returns the NaN argument itself.
  **Cost case:** `greatest(a, b, c)` and `least` over 10 M DOUBLE rows. Neither fork nor upstream.
- **(b) Argument wrap** with the NaN-only canonicaliser in part B's projection seat: smaller, a per-argument
  pass, and the output's NaN becomes canonical.

### 4.4 The array functions (inventory C5, B6)

- **(a) Canonicalise in the shim.** RePark's `array_sort`/`sort_array` shim (`collection/array_sort.rs`)
  already rebuilds its arguments; for a FLOAT/DOUBLE element type it maps every NaN child to `7ff8…` before
  forwarding, so DataFusion's totalOrder sort puts NaN last ascending and first descending, as Spark does.
  **Cost case:** `array_sort` over 1 M arrays of ten doubles. Neither fork nor upstream.
- **(b) Own comparator:** sort the children with a NaN-greatest comparator in the shim. More code, keeps bits.

### 4.5 Sort and keys

Part B covers them (§2: every `ord_*`, `topk_*`, `win_rank`, `group_by`, `cnt_distinct`, `join_col` cell is
`=` on head3). No seat here. The native door's default null placement and `array_agg` keeping NULLs (§4) are
its dialect, outside this unit.

### 4.6 Pruning

No seat of its own: under 4.1(a) the pushed predicates keep today's pruning power (exact) or keep NaN files
(sound), per the ruling in §6. Two optional fork changes, not needed by either reading:

- `Datum`'s float order is `total_cmp` (inventory G4), unlike Java's `Double.compare`; it matters only for a
  sign-set or payload partition value, and the identity column reads back canonical on every build (§5). Leave
  it, or align with Java as a fork hygiene item.
- The manifest evaluator's `>`/`>=` could read `contains_nan` (G3); only the "sound" reading would want it, and
  Java does not.

## 5. The unit split and the order

| # | Unit | Seats | Retires (label exclusion, cards) | Needs |
|---|---|---|---|---|
| 1 | **NAN-MINMAX-1** | 4.2(a): `max`/`min` ungrouped, grouped, windows, statistics shortcut, `array_min`/`array_max` | "`max`, `min`"; the grouped case for every NaN shape (not in the text); `max` from Parquet statistics | nothing; independent of part B |
| 2 | **NAN-COMPARE-LITERAL-1** | 4.1(a) in part A's seat, the pruning reading from Q1 | "non-key comparisons"; NWC verifier 2 F2 (stored Avro and old tables); closes ICEBERG-NAN-FILE-PRUNE-1 into PRUNED-WRONG and re-scopes PARQUET-NAN-EQUALITY-1 (Q7) | part B merged (shares the seat module and its policy switch); the Q1 ruling |
| 3 | **NAN-GREATEST-ARRAY-1** | 4.3(a), 4.4(a) | "`greatest`, `least`"; the array functions (not in the text) | nothing |

- **Order 1 → 2 → 3** (Q2). Unit 1 first because grouped `max`/`min` is wrong for canonical NaN, so it reaches
  every table with a NaN from any writer; it needs no ruling and no part B. Unit 2 waits for part B and the
  pruning ruling. Unit 3 is the smallest.
- **Sort and keys** need no unit: part B retires them when it merges.
- Each unit: step 0 re-measures its slice of this table on its base; red pins from these cells (Spark door,
  DataFrame, `F.expr`, native) with the canonical column as the control; one cost table at 1.05x; one verifier.

## 6. The pruning truth question (Fable)

**The question.** Must file and statistics pruning return Spark's answer exactly, or only never drop a row
Spark keeps?

**What Spark does, measured (§6).**

- **Spark-on-Iceberg drops NaN rows whenever Iceberg's IEEE evaluators can:** a NaN-only data file for any
  `>`/`>=`; a file whose non-NaN upper bound is not above the literal (`{NaN, -0.0, NULL}` for `d > 0.5`); a
  manifest whose partition summary's upper bound is not above the literal (`pk > Infinity`).
- **It keeps them where there are no metrics:** Avro data files (no bounds on either writer), and any file
  whose other values pass.
- **On plain Parquet it depends on the writer:** `parquet-mr` stores NaN as the max, so Spark's own files keep
  the rows; pyarrow and RePark (arrow-rs) leave NaN out, so Spark drops them reading those files.
- **So Spark's answer is a function of the files, not of the rows.** The same rows answer differently after a
  compaction, a different writer, or another `local[n]`. RePark today matches it file for file for a
  canonical NaN (every pruning row in §6 with `dc`/`pk`), and differs only by its bit comparison for sign-set
  NaNs.

**The two readings.**

- **(E) Exact: Spark-on-the-same-files.** 4.1(a) as written. RePark keeps today's pruning, answers Spark's way
  at the row level, and returns Spark's answer on every table both engines read. Rows Spark itself drops stay
  dropped and are stated as the PRUNED-WRONG class (as AUD-024 and GD-10/GD-11 already are). No cost.
- **(S) Sound: never drop a row Spark's row semantics keep.** 4.1(a) with `isnan(e)` instead of
  `e < -Infinity`. Layout-independent, equal to Spark over `VALUES`, a superset of Spark-on-Iceberg's answer. On
  Iceberg it costs nothing when a file's NaN count is zero; on plain Parquet `>`/`>=` lose row-group pruning.
  RePark then differs from Spark-on-Iceberg in exactly the PRUNED-WRONG cells, which become RePark-right,
  Spark-wrong.
- **The AUD-024 link.** AUD-024's ruled divergence assumed RePark keeps the NaN row where Spark's pushed filter
  drops it. This table shows RePark drops it too whenever the fork's metrics evaluator can; (S) would make
  AUD-024's premise true on every layout, (E) would make RePark agree with Spark on AUD-024's SELECT side.

**Default: (E).** The Iceberg parity goal grades RePark against Spark-on-Iceberg cell for cell, (E) costs
nothing, and the class of rows Spark drops is already a stated, measured class. (S) is the alternative if the
owner prefers SQL truth over Spark's pruning, as the AUD-024 ruling did for DML.

## 7. Owner questions

| # | Question | Default | Rests on |
|---|---|---|---|
| Q1 | The pruning reading: exact (Spark-on-the-same-files) or sound (never drop a row Spark's row semantics keep)? | **(E) exact** | §6 pruning rows; stats.jsonl; AUD-024 |
| Q2 | The order of the three units. | **NAN-MINMAX-1, NAN-COMPARE-LITERAL-1, NAN-GREATEST-ARRAY-1** | §2 `grp_*` `WWW` on canonical; §3 |
| Q3 | Do the seats apply on the native door too? | **Yes**, as part B's policy does (`signed_zero_policy` on both doors) | §4 native matrix: the same NaN codes as the Spark door |
| Q4 | Give up the `max`/`min` statistics shortcut for FLOAT/DOUBLE on plain Parquet? | **Yes**; its cost row is recorded, not held to 1.05x | §6 `agg_max` on `pq_pa_l1`, `pq_rb_l1` (`Infinity`), `pq_sp_l5` (`stats`) |
| Q5 | The NaN that `max`, `min`, `greatest` and `least` return: canonical `7ff8…` or the input's bits? | **Canonical** (bits differ, values equal; Spark's writers store `7ff8…`) | §2 `nan_bits`; §5 |
| Q6 | The label exclusion's text, measured on head3 (part B) with 1325: "A NaN with the sign bit set (computed in the query, stored in Avro, or stored before NAN-WRITE-CANONICAL-1) answers unlike Spark in `max`, `min`, `greatest`, `least`, `array_sort`, `sort_array`, `array_min`, `array_max` and comparisons with a literal; a NaN with a payload answers unlike Spark in `=`, `<>`, `<=>`, `IN` and `>` against NaN; grouped `max` and `min` answer unlike Spark for any NaN; `max` over a Parquet file holding NaN can be answered without it." | **Adopt this text** for the label; each unit narrows it | §2, §3, §5, §6 |
| Q7 | PARQUET-NAN-EQUALITY-1: re-scope it to the writers' statistics (RePark leaves NaN out of Parquet min/max, as the format says; `parquet-mr` writes it as max), with the read side recorded as equal to Spark on the same file? | **Yes; no writer change** | §6 `pq_*_l1`; stats.jsonl footers |
| Q8 | Report B2 (the grouped `max`/`min` accumulator) to DataFusion upstream? | **Yes, after NAN-MINMAX-1 lands**; the unit does not wait for it | inventory B2; §3 |
| Q9 | Card the `UNION ALL` internal error ("Physical input schema should be the same…", four columns of one Iceberg table) and `approx_count_distinct` refusing FLOAT/DOUBLE? | **Card both, S3, outside this family** | §1 `R-ERR` |

## 8. Cost in one place

Hot kernels: comparisons and `max`/`min` run on every float filter and aggregate. Every cost row runs at
1.05x on plans without NaN; the unit fails its cost gate rather than trading a pruning loss for correctness
silently. The rows, by unit: 4.2(a)'s five plus the separate statistics row; 4.1(a)'s three filter shapes on
three sources plus the TPC-H Q6 shape and the files-and-row-groups-scanned equality; 4.3(a)'s and 4.4(a)'s one each.

## 9. Side observations (not acted on)

- `approx_count_distinct` refuses FLOAT and DOUBLE on every door and build; Spark answers (§1).
- A `UNION ALL` of four DOUBLE columns of one Iceberg table fails planning on the Spark door ("Physical input
  schema should be the same as the one converted from logical input schema"); not found carded (Q9).
- `ln(-1)` and `log10(-1)` are NaN on RePark, NULL on Spark (LOG-NONPOSITIVE-NULL-1, carded). The native door
  answers `0.0 / 0.0` with NaN in both ANSI modes (its dialect).
- Spark folds `sqrt(-1.0)` and the other producers to `7ff8…` and RePark to `fff8…` (§7); over a column both
  give `fff8…`. No comparison sees the difference once the seats above land.
