# NAN-COMPARE-SPARK-1: the design sketch

**Date:** 2026-11-13. **Author:** a design-sketch lane (`claude-opus-5-5`, xhigh), documents and probes only.
**Source:** the owner's ruling of 2026-11-13 ("Defaults", item 3: this unit first after the scoped label) and
Fable's guidance adopted the same day (an xhigh sketch before any build; a truth ruling owed on pruning).
**Card:** [NAN-COMPARE-SPARK-1](../../roadmap/mid-term/nan-compare-spark-1-card-2026-11-13.md). Evidence:
[domain-table.md](domain-table.md) (`§n` below), [inventory.md](inventory.md) (seats `A1`…`I8`).

**Revised after the xhigh inventory critic** (`VERDICT: GAPS`, 2026-11-13). The critic's doors M1–M8 are
measured in §9 of the domain table and inventoried in section I. This revision:

- corrects three claims: PARQUET-NAN-EQUALITY-1's "read side equal to Spark", "pruning equals Spark file for
  file" and "sort and keys need no unit";
- regroups the units;
- adds the label question (§7);
- rewrites the owner questions (§8).

## 1. The fault

Spark compares every NaN as one value, equal to itself and larger than `+Infinity`, whatever its sign bit or
payload. RePark compares floats by Arrow's totalOrder (inventory A1): a sign-set NaN (`fff8…`) is the smallest
value, a payload NaN (`7ff8dead…`) is above the canonical one, and two NaNs with different bits are unequal. Both
engines keep a sign-set NaN in memory (§7, `jvm_raw`); only the comparison differs.

Three faults the critic found are not about NaN's bits at all:

- **The grouped accumulator** replaces its running value on any NaN comparison, and it starts from
  `f64::MIN`/`f64::MAX`, not "no value" (B2, I1).
- **RePark consumes Parquet statistics Spark never uses:** the `max`/`min` shortcut, constant columns, and the
  TopK and aggregate dynamic filters (B5, I2, I3, I8). Parquet statistics leave NaN out.
- **Equality inside functions** (`nullif`, the array set functions) compares bits (I5).

## 2. The headline

- **The main table:** 15,351 cells at ANSI on and 4,061 at ANSI off, on four doors and 29 sources (§1).
  Wrong at ANSI on: base 7,095, 1325 7,001, head3 4,685.
- **§9, after the critic:** about 100 cells per build on the Spark and native doors. No NaN verdict moves with
  the ANSI mode.
- **Wrong on stored data with a canonical NaN, on every build:**
  - grouped `max`/`min` (every Iceberg format, both writers, DOUBLE and FLOAT);
  - every NaN in a plain-Parquet file whose other values are one value, read as that value; RePark then
    writes it into Parquet and into Iceberg tables (§9 M1).
- **Wrong with no NaN at all, on every build:** grouped `max` of a `-Infinity`-only group is
  `-1.7976931348623157e+308`, and grouped `min` of a `+Infinity`-only group is `1.7976931348623157e+308`.
- **Part B (head3) fixes:** sort, keys and column-to-column comparisons for top-level scalar columns, and the
  TopK dynamic filter by accident of its wrap.
- **Part B does not fix:**
  - literal comparisons;
  - `max`/`min` in every form;
  - `greatest`/`least`;
  - the array sort, set and search functions;
  - `nullif`;
  - `median`/`percentile_approx`;
  - struct leaves.
- **NAN-WRITE-CANONICAL-1 (1325) moves only the tables RePark writes itself** (§5). Grouped `max`/`min` stays
  wrong on them too.

## 3. What the folded cards turn out to be

- **PARQUET-NAN-EQUALITY-1 has two causes.**
  1. **Pruning by NaN-free statistics** (inventory F2). Spark's reader prunes the same pyarrow or RePark files:
     `= NaN` keeps nothing on Spark too (§6 `pq_*_l1`, §9 `pa_const48_eq_nan`). This half is the pruning
     question, and here RePark equals Spark.
  2. **The constant-column read** (I2). Where a file's non-NaN values are one value, RePark reads every NaN as
     that value and Spark reads NaN (§9 `pa_const_rows`, `pa_const48_isnan`). That is a RePark-only wrong
     read, and through any write a wrong write (§9 `roundtrip_*`, `ingest`).

  **Withdrawn:** the first draft's "read side equal to Spark".
- **ICEBERG-NAN-FILE-PRUNE-1:** the PRUNED-WRONG class. On the same files, Spark-on-Iceberg and RePark both
  answer `d > 0.5` with `[1, 3]` (§6); Iceberg's metrics evaluator reads `NaN > x` as false in Java and in the
  fork alike (G2).
- **The Avro partition-column oddity:** the manifest evaluator skips a manifest whose partition summary's upper
  bound (`Infinity`) is not above the literal, without reading `contains_nan` (G3). It happens on every format;
  the non-partition `f` survives on Avro because Avro data files carry no column metrics.
- **The label exclusion's pending text understates the gap:** §7 states what is wrong inside the label.

## 4. Seats, one family at a time

**(a)** is the recommendation in each family. "Cost cases" are run at 1.05x on plans **without** NaN.

### 4.1 Comparisons with a literal (E1–E4), function-internal equality (I5), the delete door (I7)

**(a) Total-order bounds, in part A's literal seat** (`df_guards/signed_zero/literals.rs`, analyzer rule and
after each simplification, every door).

- **Non-NaN literal `c`.** The rewrites:
  - `e > c` → `e > c OR e < -Infinity`;
  - `e >= c` → `e >= c OR e < -Infinity`;
  - `e < c` → `e >= -Infinity AND e < c`;
  - `e <= c` → `e >= -Infinity AND e <= c`;
  - `=` and `<>` are unchanged.

  The critic evaluated these forms over all nine shapes: 374 of 378 cells equal Spark on every build. No
  simplifier fold was seen (`EXPLAIN` keeps them as written), and pruning power is unchanged.
- **NaN literal, with NULL guards valid on both doors.** `isnan(NULL)` is `false` on the Spark door and NULL on
  the native door. So the forms avoid relying on either:
  - **in a filter:**
    - `= NaN` and `>= NaN` → `e IS NOT NULL AND isnan(e)`;
    - `<> NaN` and `< NaN` → `e IS NOT NULL AND NOT isnan(e)`, so **no NULL row is kept** (the critic's wrong-row
      case);
    - `<= NaN` → `e IS NOT NULL`;
    - `> NaN` → `false`;
    - `<=> NaN` → `e IS NOT NULL AND isnan(e)`;
  - **in a projection:** the same predicate under `CASE WHEN e IS NULL THEN NULL ELSE … END`, except `<=>`,
    which is never NULL;
  - **`IN (NaN, …)`** → `(e IS NOT NULL AND isnan(e)) OR e IN (…)`; `NOT IN` is its complement with Spark's NULL
    rule;
  - **`BETWEEN` and a searched `CASE`** expand first.

  The fork converts `isnan` and `IS NOT NULL` (G1), so pushdown is kept; step 0 must confirm it on the guarded
  forms.
- **Operands that cannot hold NaN are skipped:** a cast from an integer or decimal type, and literals.
  Cost row: an INT column against a DOUBLE literal.
- **Function-internal equality (I5, nine functions):** in RePark's shims (`array_contains.rs`, `array_position.rs`,
  `arrays_overlap.rs`) and in a `nullif` shim. Canonicalise every float NaN to `7ff8…` (NaN only, zeros
  untouched) before the inner call. The union, intersect, except, distinct and remove functions get the same
  pre-pass in the dispatch (`function_dispatch.rs:420-456`).
- **The delete door (I7).** It is not reached from either engine's writes (§9). Align the fork's `Datum` float
  order with Java's `Double.compare` (every NaN one value above `+Infinity`; `datum.rs:225-227`). That is one
  function and closes the door for every writer. The alternative, a writer census, cannot cover third-party
  writers.
- **Cost cases:**
  - `count(*) WHERE d > c`, `d < c`, `d BETWEEN a AND b` over 10 M DOUBLE rows, in memory, from a 100-row-group
    Parquet file and from a 100-file Iceberg table;
  - a TPC-H Q6 shape;
  - an INT column against a DOUBLE literal;
  - `array_contains` over 1 M arrays;
  - files and row groups scanned, before and after, which must be equal.
- **Fork or upstream:** the `Datum` alignment is a fork change. The rest is RePark.

**(b) The canonical wrap**, as part B does for column comparisons. It blocks every float predicate's pushdown, so
it is rejected on cost.

### 4.2 `max`/`min`, quantiles, and the statistics RePark alone consumes (B1–B7, I1–I4, I8)

**(a) RePark's own float `max`/`min`, plus a statistics seat.**

- **The UDAF.** In `repark-functions`, for FLOAT and DOUBLE, delegating other types:
  - **grouped:** its own accumulator that starts from **no value** (fixing I1 for `±Infinity`) and treats NaN
    as one value above `+Infinity` (fixing B2);
  - **ungrouped and sliding windows:** the same order;
  - **`array_min`/`array_max`:** the same kernel through the dispatch;
  - **its internal name is not `max`/`min`** (aliases keep the SQL names), so DataFusion's name-matched
    aggregate dynamic filter (I8) does not attach.
- **The statistics seat, which is the Inexact seat.** Wrap `ParquetFormat` in RePark's plain-Parquet read path
  (`crates/repark-core/src/spark_nullable.rs:27`, `:32`). The wrapper is a `FileFormat` whose
  `infer_stats_and_ordering` returns FLOAT/DOUBLE `min_value`/`max_value` as `Precision::Inexact`.
  - Both RePark-only consumers require `Exact`: the `max`/`min` shortcut (B5) and the constant-column
    replacement (I2), so one seat neutralises both.
  - File pruning (Inexact is enough) and row-group pruning (it reads the footers) keep working.
  - Iceberg is untouched: the fork reports no column statistics, and its reader is its own.
  - **Alternative:** turn `datafusion.execution.collect_statistics` off for the session. That is simpler, but it
    drops row counts and every column's statistics.
- **Quantiles (I4).** Canonicalise the argument's NaN (NaN only) before `median` and in RePark's
  `quantile_summaries.rs` insert path. Both then order NaN last, as Spark does.
- **Pins:**
  - every §9 grouped cell, the `±Infinity`-only groups included;
  - the constant-file read, the round trip, and the Iceberg ingestion;
  - `max` over `pa_dyn` (the shortcut);
  - a multi-file plain-Parquet ungrouped `max` with the UDAF in place, with dynamic filters on (I8);
  - the TopK cell `topk_desc_on`, which stays right only while part B wraps the sort key (I3).
- **Struct and list leaves** (`min_max_struct.rs`): **not in this unit.** They go to unit 4 with nested sort and
  keys (§5).
- **Cost cases:**
  - ungrouped and grouped `max`/`min` (1 K and 1 M keys);
  - a running and a sliding window;
  - `median`, `percentile_approx`;
  - `array_max`;
  - `SELECT count(*)` and a selective filter over 100 Parquet files, whose row groups scanned must not change;
  - a separate row, not held to 1.05x: `SELECT max(d)` over a Parquet file with statistics.
- **Fork or upstream:** neither. B2 and I1 are worth reporting upstream (Q9).

**(b) An argument wrap.** Part B's `aggregate_call` wraps the `max`/`min` argument. It fixes B1 and B3, but not
B2 or I1, which need an upstream fix; and it adds a per-row call.

### 4.3 `greatest`/`least` (D1) and 4.4 the array sorts (C5)

- **4.3(a):** RePark's own float `greatest`/`least`, NaN greatest.
  - **Alternative (b):** an argument wrap.
  - **Cost case:** `greatest(a, b, c)` over 10 M rows.
- **4.4(a):** in the `array_sort`/`sort_array` shim, canonicalise NaN children before DataFusion's sort.
  - **Alternative (b):** the shim's own comparator.
  - **Cost case:** `array_sort` over 1 M arrays.

### 4.5 Sort and keys

- **Top-level scalar keys:** part B covers them (§2), including the TopK dynamic filter (I3).
- **Nested leaves are not covered** (I6, §9 `struct_*`): `max(struct)`, `ORDER BY struct` and `DISTINCT` over
  structs are wrong on head3. **Corrected:** the first draft's "sort and keys need no unit" was wrong. Unit 4
  takes them; it sits outside the scoped label, which covers scalar columns only.

### 4.6 Pruning

See §6. The literal seat keeps the evaluators' pruning; the statistics seat in 4.2 removes RePark's own
consumers.

## 5. The unit split and the order

| # | Unit | Takes | Retires | Needs |
|---|---|---|---|---|
| 1 | **NAN-MINMAX-1** | 4.2(a): the float UDAF (grouped start from no value, NaN order, sliding, `array_min`/`array_max`), the Inexact statistics seat (B5 + I2), quantiles, the dynamic-filter pins (I3, I8) | grouped `max`/`min` on stored data and `±Infinity` groups; the constant-column wrong read and write; `max` from statistics; the quantiles | nothing; independent of part B |
| 2 | **NAN-COMPARE-LITERAL-1** | 4.1(a): the bounds rewrite with NULL guards on both doors and the integer-cast skip; function-internal equality (I5: `nullif` and eight array functions); the fork's `Datum` order (I7) | literal comparisons on stored non-canonical NaN (NWC F2); the delete door; ICEBERG-NAN-FILE-PRUNE-1 into PRUNED-WRONG | part B merged; the Q1 ruling; one fork PR |
| 3 | **NAN-GREATEST-ARRAY-1** | 4.3(a), 4.4(a) | `greatest`/`least`, `array_sort`, `sort_array` | nothing |
| 4 | **NESTED-NAN-ORDER-1** (to be carded; outside the scoped label) | struct and list leaves in sort, keys, `DISTINCT` and `max`/`min` | §9 `struct_*` | part B merged |

- **Order:** 1 → 2 → 3 → 4 (Q2).
- **Unit 1's first item is the statistics seat.** It is a wrong read that becomes a wrong write, so it may also
  run alone ahead of the rest of unit 1 (Q3).
- **Each unit's step 0** re-measures its §2 and §9 slice on its base. Red pins use canonical controls; there is
  one cost table at 1.05x and one verifier.

## 6. The pruning truth question (Fable), restated

**The question.** Must file and statistics pruning return Spark's answer exactly, or only never drop a row Spark
keeps?

**Measured.**

- **Spark-on-Iceberg drops NaN rows** wherever Iceberg's IEEE evaluators can: NaN-only files; files whose
  non-NaN upper bound is not above the literal; manifests whose summary's upper bound is not above it (§6).
- **Spark keeps them where there are no metrics** (Avro data files).
- **On plain Parquet, Spark's reader prunes the user's predicate by footers.** It drops `= NaN` on pyarrow and
  RePark files, as RePark does. It has **no** `max`/`min` shortcut, no constant columns and no dynamic
  filters: Spark answers NaN in every §9 cell where RePark's own statistics consumers lose it.
- **So Spark's answer is a function of the files**, and RePark matches it file for file only in the evaluators
  both engines share. **Corrected:** the first draft's "RePark's pruning equals Spark's file for file" holds
  for the Iceberg evaluators only.

**The two readings.**

- **(E) Exact: prune only where Spark's own reader prunes.**
  - **What stays:** the Iceberg metrics and manifest evaluators, and footer and page pruning of the user's
    predicate.
  - **What goes, for float columns:** every RePark-only statistics consumer, through 4.2's statistics seat,
    the UDAF's name, and part B's sort-key wrap.
  - **The literal rewrite** uses the `e < -Infinity` form, which keeps the evaluators' pruning unchanged.
  - **The rows Spark itself drops** stay dropped, as the stated PRUNED-WRONG class.
- **(S) Sound: never drop a row Spark's row semantics keep.**
  - **What changes:** the literal rewrite uses `isnan(e)` instead of `e < -Infinity`. That keeps NaN files on
    Iceberg (NaN counts), and makes `>`/`>=` unprunable on plain Parquet.
  - **What follows:** RePark then differs from Spark-on-Iceberg in exactly the PRUNED-WRONG cells, as the
    AUD-024 ruling already chose for DML.

**Default: (E),** restated as above. It is cheap rather than free: the statistics seat costs float `max`
metadata answers.

## 7. The label question, plainly

Scope: scalar columns, the Spark door, Glue and S3 Tables (Iceberg). Which wrong answers on **stored** data fall
inside it, on main and on both heads?

| # | Wrong answer inside the label | Cells | Fixed by |
|---|---|---|---|
| L1 | **Grouped `max`/`min` over any stored NaN, canonical included**, on every format and both writers: `{NaN, 0.0}` gives `max 0.0`; `{5.0, NaN}` gives `min NaN` | §9 `rb_grp_parquet`, `rb_grp_orc`, `rb_grp_avro`, `sp_grp_parquet`, `sp_grp_orc`, `sp_grp_avro`, DOUBLE and FLOAT | unit 1 |
| L2 | **Grouped `max`/`min` of a `±Infinity`-only group, no NaN at all:** `max(-Infinity) = -1.7976931348623157e+308` | the same six tables, groups `g3`, `g4`; `grp_mem_*` | unit 1 |
| L3 | **Ingesting a plain-Parquet file into a labelled table** (`spark.read.parquet(f).writeTo(t)`): a NaN in a file whose other values are one value is stored as that value | §9 `ingest` (Parquet and Avro tables), `roundtrip_*` | unit 1, statistics seat |
| L4 | **A stored non-canonical NaN.** These tables hold one: Avro tables from either engine when the NaN was sign-set, Parquet or ORC tables RePark wrote before NAN-WRITE-CANONICAL-1, and other engines' tables. Wrong on them: literal comparisons (`= NaN`, `> Infinity`, `> 0.5`, `< 0`), ungrouped and window `max`/`min`, `greatest`/`least`, the array functions, quantiles, `nullif` | §5 `rb_avro_*`, `sp_avro_*`, `rb_*` columns `ds`/`dq`; §6 `rb_avro_l5 ds f_eq_nan` (`[]` vs `[2, 9]`); §2 | units 1, 2, 3 |
| L5 | **A stored payload NaN:** `= NaN`, `<> NaN`, `<=> NaN`, `IN (NaN)`, `> NaN` | §5 `dp` columns | unit 2 |

- **Outside the label:**
  - struct leaves (unit 4);
  - the native door;
  - plain-Parquet reads as such: the shortcut, constant columns and TopK. L3 is the exception, because its
    destination is labelled.
- **The choice for the owner (Q1).**
  - **(i) NAN-MINMAX-1 before the label.** It removes L1, L2 and L3, the three that hit canonical data and data
    with no NaN at all. The exclusion text then covers only L4 and L5.
  - **(ii) A wider exclusion text now.** It must name L1–L5, including "grouped `max`/`min` over a group whose
    values are all `+Infinity` or all `-Infinity`" and "ingesting a Parquet file whose NaN shares a file with a
    single other value".
- **Default: (i).** L2 and L3 are not NaN-comparison semantics, and no NaN-scoped text describes them honestly.

## 8. Owner questions (rewritten after the critic)

| # | Question | Default | Rests on | Replaces |
|---|---|---|---|---|
| Q1 | NAN-MINMAX-1 before the label (i), or a wider exclusion (ii)? | **(i)** | §7 L1–L3; §9 grouped, `ingest`, `roundtrip_*` | new |
| Q2 | Unit order? | **1 NAN-MINMAX-1, 2 NAN-COMPARE-LITERAL-1, 3 NAN-GREATEST-ARRAY-1, 4 NESTED-NAN-ORDER-1** | §5; §9 | old Q2 |
| Q3 | Run unit 1's statistics seat (the constant-column wrong read and write) alone first, carded S1? | **Yes, as unit 1's first PR** | §9 `pa_const_rows`, `roundtrip_*`, `ingest` | new |
| Q4 | The statistics seat: an Inexact `FileFormat` wrapper for float columns, or `collect_statistics` off? | **The wrapper** | inventory B5, I2; §9 `pa_dyn` `agg_max_*` | old Q4 |
| Q5 | Pruning reading: (E) "prune only where Spark's own reader prunes", or (S) sound? | **(E)** | §6 pruning rows; §9 `topk_desc_*`, `pa_const48_eq_nan` | old Q1 |
| Q6 | The literal rewrite's NaN forms: one NULL-guarded form valid on both doors (`e IS NOT NULL AND …`)? | **Yes** | the critic's 378-cell evaluation; `isnan(NULL)` per door | new |
| Q7 | The delete door: align the fork's `Datum` float order with Java's `Double.compare`? | **Yes, one fork PR in unit 2** | inventory G4, I7; §9 `meta-delete` (not reached) | new |
| Q8 | The NaN that `max`, `min`, `greatest`, `least` return: canonical `7ff8…`? | **Yes** | §2 `nan_bits` | old Q5 |
| Q9 | Report B2 and I1 (the grouped accumulator) to DataFusion upstream? | **Yes, after unit 1 lands** | §3; §9 `grp_mem_*` | old Q8 |
| Q10 | Re-scope PARQUET-NAN-EQUALITY-1: its pruning half recorded as equal to Spark on the same file, and its constant-column half moved to unit 1? | **Yes; no writer change** | §6 `pq_*_l1`; §9 `pa_const_*` | old Q7 (withdrawn) |
| Q11 | Card NESTED-NAN-ORDER-1 (struct and list leaves), outside the scoped label? | **Yes** | §9 `struct_*` on head3 | new (old §4.5 claim withdrawn) |
| Q12 | Card the side observations: the `UNION ALL` internal error; `approx_count_distinct` refusing floats; Spark deleting the NULL-partition row for `pk < 100` where RePark keeps it? | **Card all three, S3** | §1 `R-ERR`; §9 `meta-delete after` | old Q9 |

- **Folded into §7 and Q1:** old Q6 (the exclusion text).
- **Kept in substance:** old Q3 (the seats apply on the native door too) is the default throughout, as part B's
  policy does.

## 9. Side observations (not acted on)

- `approx_count_distinct` refuses FLOAT and DOUBLE on every door and build; Spark answers.
- A `UNION ALL` of four DOUBLE columns of one Iceberg table fails planning on the Spark door ("Physical input
  schema should be the same…").
- `DELETE … WHERE pk < 100` on the partitioned table: Spark 4.1.2 with Iceberg 1.11.0 also deletes the NULL
  partition's row (`id 9`); RePark keeps it, as SQL's three-valued logic does (§9 `meta-delete after`). It was
  measured once, on one table, and is not NaN.
- `ln(-1)`, `log10(-1)` are NaN on RePark and NULL on Spark (LOG-NONPOSITIVE-NULL-1). The native door answers
  `0.0 / 0.0` with NaN in both ANSI modes.
- The native door has no `array_contains`, `array_position`, `array_distinct`, `arrays_overlap`, `array_remove`,
  `array_union`, `array_intersect`, `array_except`, `percentile_approx` or struct `max` (its dialect).
