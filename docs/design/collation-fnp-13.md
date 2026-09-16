# Design note — FNP-13 string collation (retiring registry G15)

**Status:** design note, no code (owner ruling **Q-15c-5**, 2026-09-15: "FNP-13 collation design note after FNP-4B lands") · **Author:** run 16c orchestrator, 2026-09-15 · **Home:** `docs/design/collation-fnp-13.md` · **Registry row it plans to retire:** G15 (§4, "string collation is refused at first evaluation") · **Oracle:** `fixtures-batch18-collation.json` (live PySpark 4.1.2, 55 cells, recorded 2026-09-15; cell ids `Q18-*` below).

## 1. Why a note before a unit

Collation changes the answer of comparisons, ordering, grouping, hashing and a dozen string functions, on both SQL doors and the facade. G15 refuses every such path loudly today (owner rulings A5/A10, 2026-08-12: "absence is loud"). Replacing a loud refusal with a partial implementation risks exactly the silent wrong-count G15 was filed against, so the order of slices, the type carrier and the dependency question are decided here first.

## 2. What Spark 4.1.2 does (measured)

| Area | Behaviour | Cells |
|---|---|---|
| Names | `collation(…)` answers `SYSTEM.BUILTIN.<name>` for `UTF8_BINARY`, `UTF8_LCASE`, `UNICODE`, `UNICODE_CI`, `UNICODE_AI`, `UNICODE_CI_AI`, ICU locales with modifiers (`de`, `de_CI`, `de_CI_AI`, `sr_Cyrl_SRB`), and `UTF8_BINARY_RTRIM` / `UNICODE_RTRIM`; `en_US_CI` and unknown names refuse | Q18-name-0…13 |
| Equality | `UTF8_LCASE` and `UNICODE_CI` fold case; `UNICODE_CI_AI` also folds accents (`'Émile' = 'emile'`); neither folds `ß`→`SS` (`'straße' ≠ 'STRASSE'`) | Q18-eq-0…4 |
| Ordering | `UNICODE` is ICU root order (`'a' < 'B'` true, binary false; lower before upper within a key); `UTF8_LCASE` / `UNICODE_CI` order case-insensitively | Q18-lt-0, Q18-order-0…2 |
| Grouping / distinct / hashing | `GROUP BY`, `count(DISTINCT …)`, hash join keys and window `PARTITION BY` follow the collation (one group for Alice/alice/ALICE) | Q18-group-0, Q18-distinct-0, Q18-hashjoin-0, Q18-partition-0 |
| Membership | `IN` follows the collation | Q18-in-0 |
| Precedence | explicit `COLLATE` beats implicit/default; two different explicit collations → `AnalysisException` `COLLATION_MISMATCH.EXPLICIT` (also inside `concat`) | Q18-implicit-0, Q18-mismatch-0/1 |
| String functions | `contains`, `startswith`, `instr`, `locate`, `replace`, `split`, `LIKE`, `RLIKE` are collation-aware; `upper` keeps the collated type; `ILIKE` refuses a collated input (`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`); result types carry the collation (`array<string collate UTF8_LCASE>`) | Q18-fn-* |
| Types | `typeof` / schema render `string collate X`; `CAST(… AS STRING COLLATE X)`; nested `map<string collate UTF8_LCASE,int>` | Q18-typeof-0, Q18-schema-0, Q18-cast-0, Q18-map-key-0 |
| Session default | `SET spark.sql.session.collation = UTF8_LCASE` does **not** change literal comparisons or `collation('x')` on 4.1.2 — object-level collation is off by default | Q18-session-0/1 |
| Python API | `createDataFrame(…, StringType("UNICODE_CI")).distinct().count() == 1`; `F.collation(F.collate(c, "UNICODE_CI"))` = `SYSTEM.BUILTIN.UNICODE_CI` | Q18-df-0, Q18-api-collate |

Not measured yet (a unit's first step records them): collation in Parquet/Iceberg column metadata round trips, `ORDER BY` null placement under a collation, `min`/`max`/`array_sort`/`array_distinct` over collated strings, collation-aware `hash`/`xxhash64` (`spark.sql.legacy.collationAwareHashFunctions`), RTRIM semantics beyond Q18-fn-trim, `spark.tvf.collations()`.

## 3. What RePark does today (G15)

Every compare/order-changing entry point refuses with `UnsupportedOperationException` / `NotImplemented` naming the collation (`crates/repark-spark/src/collation.rs`, `COLLATION_REFUSAL_NEEDLE`; the repark-sql valve; `filter_sql`; facade `_engine_type`): SQL `expr COLLATE`, `ORDER BY … COLLATE`, `CREATE TABLE … STRING COLLATE`, `CAST(… AS STRING COLLATE …)`, `SET spark.sql.collation.*`, `createDataFrame` / `fromJson` `__COLLATIONS` with a non-binary `StringType`, `Column.cast` to a collated string. `StringType(collation=…)` construction and `simpleString` stay. `F.collate` / `F.collation` / `Column.collate` are absent. The pins are listed in the G15 row (Python `test_collation_refuse.py`, Rust `crates/repark-spark/src/tests/collation.rs`, `crates/repark-sql/src/guards/tests.rs`).

## 4. Design

### 4.1 Carrier — collation rides on the Arrow field, resolved in Rust

- A collated string column is Arrow `Utf8`/`Utf8View`/`LargeUtf8` with field metadata `{"spark.collation": "<NAME>"}` (the same place the Spark JSON `__COLLATIONS` map lands). `repark-spark`'s type table (`SparkDataType::SparkString { collation }`, already present) maps both ways; `string collate X` renders from it.
- No new Arrow type and no DataFusion fork: DataFusion compares bytes, so collation is applied by **rewriting the plan**, not by teaching kernels about metadata.

### 4.2 Resolution — one Spark analyzer rule, before type coercion

A `SparkCollationResolution` analyzer rule in `crates/repark-spark` (Spark door and facade; the native ANSI door keeps G15 refusals per ADR-0002 until a separate ANSI decision):

1. Computes each string expression's collation and strength — explicit (`COLLATE`), implicit (column metadata), default (`UTF8_BINARY`; the session key stays inert, matching Q18-session-0).
2. Resolves operators and functions by Spark's precedence; two explicit collations → `COLLATION_MISMATCH.EXPLICIT` (Q18-mismatch-*), with Spark's other mismatch classes recorded when measured.
3. Rewrites collation-sensitive operations onto **sort/compare keys**: `a = b` → `key_X(a) = key_X(b)`; `ORDER BY a` → `ORDER BY key_X(a)` (value column unchanged); `GROUP BY` / `DISTINCT` / join keys / `PARTITION BY` → group on the key and carry the first value (Spark returns a representative value — measure which: Q18-group-0 shows `'Alice'`); `IN` → key comparison.
4. Routes collation-aware string functions (`contains`, `startswith`, `instr`, `locate`, `replace`, `split`, `LIKE`, `RLIKE`) to Rust kernels that take the collation as an argument; `ILIKE` over a collated input refuses (Q18-fn-ilike).

`key_X` is a Rust scalar UDF per collation family returning `Binary` (a sort key), so every DataFusion operator — sort, hash aggregate, hash join, window — works unchanged and spills correctly.

### 4.3 Collation families and the dependency question

| Family | Key function | Dependency |
|---|---|---|
| `UTF8_BINARY` | identity (no rewrite) | none |
| `UTF8_LCASE` | per-code-point lowercase fold (Spark's `UTF8String.toLowerCase` semantics, not full Unicode case folding — `ß` stays, Q18-eq-4) | none |
| `*_RTRIM` | strip trailing spaces, then the base family | none |
| `UNICODE`, `UNICODE_CI`, `UNICODE_AI`, `UNICODE_CI_AI`, ICU locales (`de`, `de_CI_AI`, `sr_Cyrl_SRB`, …) | ICU collator sort key at the right strength (primary/secondary/tertiary) and locale | **ICU4X `icu_collator`** (pure Rust, Unicode-consortium maintained) — **owner question Q-16c-2** |

Spark uses ICU4J; ICU4X implements the same CLDR root collation and tailorings, but byte-identical keys are not the contract — the **ordering and equality results** are, and every family gets an oracle matrix before it ships.

### 4.4 Facade

`StringType("UNICODE_CI")` in `createDataFrame` / `fromJson` / `Column.cast` carries the name into the field metadata instead of refusing; `F.collate`, `F.collation`, `Column.collate` become thin bindings over the Rust `COLLATE` expression and `collation()` UDF (Rust-first: no Python comparison logic).

## 5. Slices (each a PR; G15 pins retire only for the paths a slice makes correct)

1. **FNP-13a — carrier + `UTF8_LCASE` + `*_RTRIM` + precedence, no new dependency.** Field metadata both ways, `collation()` / `COLLATE` / `CAST … COLLATE` / `typeof`, the analyzer rule with `COLLATION_MISMATCH.*`, key rewrites for `=`/`<`/`ORDER BY`/`GROUP BY`/`DISTINCT`/join/`PARTITION BY`/`IN`, `createDataFrame` `StringType("UTF8_LCASE")`. Every other collation name keeps the G15 refusal. G15 narrows to the ICU families.
2. **FNP-13b — collation-aware string functions** for the slice-1 families (`contains` … `RLIKE`, `ILIKE` refusal).
3. **FNP-13c — ICU families** (after Q-16c-2): `UNICODE*` and locale collations through `icu_collator`; name validation (`en_US_CI` refuses, Q18-name-9). G15 retires.
4. **FNP-13d — storage:** collation in Parquet/Iceberg column metadata, schema evolution, `DESCRIBE`, and the ANSI door's decision (keep refusing or accept `COLLATE` natively) under ADR-0002.

## 6. Risks and guards

- **Silent wrong answers at the seam:** any operator the analyzer rule misses compares bytes. Guard: slice 1 adds a plan validator that refuses (G15 needle) when a collated field reaches a comparison, sort, hash or join that the rule did not rewrite — absence stays loud for unmapped paths.
- **Pushdown:** a key rewrite over an Iceberg/Parquet scan must not push a byte predicate for a collated comparison; the validator covers filter pushdown too.
- **Performance:** key UDFs allocate per row; the S2-21 reviewers measure `GROUP BY` / `ORDER BY` over 1M rows per family, and a dictionary-array fast path is in scope for slice 1.
- **Hash-function semantics:** `spark.sql.legacy.collationAwareHashFunctions` changes `hash`/`xxhash64` over collated strings — measure before slice 1 closes.

## 7. Owner question

**Q-16c-2 — ICU4X `icu_collator` for the `UNICODE*` and locale collations?** Recommendation: **approve for slice FNP-13c**, pure Rust, data compiled in (no runtime ICU data download), behind the existing crate-DAG and cargo-deny gates; slices 13a/13b ship `UTF8_LCASE` / `RTRIM` without it.
