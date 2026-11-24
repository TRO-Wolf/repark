# STRING-HOUR-MINUTE-SECOND-1 — the insertion inventory

The owner's rule of 2026-11-09: every place where a `STRING` argument to the datetime field extractors is resolved,
coerced or evaluated, with file:line on main `2fdab5c7` (the measured package `pkg-main-4c365814` has the same
`crates/` tree for these files: `git diff --name-only 4c365814 2fdab5c7 -- crates` lists only the `sum` files), and
what each one sees. DataFusion 54.1.0 and datafusion-spark 54.1.0 are read from the cargo registry copy, read only.
The verdict column points at [domain-table.md](domain-table.md).

## 1. Name resolution: which function a door reaches

| # | site | file:line | what it does with `hour` / `date_part` / `EXTRACT` |
|---|---|---|---|
| R1 | `register_all` | `crates/repark-functions/src/lib.rs:107-109`, `:126-128` | registers every datafusion-spark scalar first (its `hour`, `minute`, `second`, `date_part`, `date_trunc`, …), then `datetime::functions()`, which replaces `hour`, `minute`, `second` (the `time_family` wrappers) and `date_trunc`. Nothing replaces `date_part`: the registry's `date_part` / `datepart` is datafusion-spark's `SparkDatePart`. |
| R2 | `datetime::functions` | `crates/repark-functions/src/datetime.rs:40-61` | `year` … `weekday` are `part_udf` (`DatePartUdf`); `hour`, `minute`, `second` are `crate::time_family::{hour,minute,second}_udf()` (`:52-54`). |
| R3 | `time_family::{hour,minute,second}_udf` | `crates/repark-functions/src/time_family.rs:55-68` | `DatePartWithoutTime` around `datetime::{hour,minute,second}_udf()` (`datetime.rs:115-131`, `DatePart::Hour/Minute/Second`, offset 0). |
| R4 | SQL `EXTRACT(f FROM e)` | `datafusion-functions-54.1.0/src/planner.rs:37-41` | `plan_extract` builds DataFusion's own `DatePartFunc` directly, **not** the registry's `date_part`. |
| R5 | SQL `date_part('f', e)` | registry (R1); `datafusion-spark-54.1.0/src/function/datetime/date_part.rs:47-62`, `:94-138` | `SparkDatePart`: signature `(string, Timestamp)` or `(string, Date)`, so a `STRING` source fails to plan (`date_part(Utf8, Utf8)`); its `simplify` maps Spark names (`dayofweek` → `dow` + 1, `yearofweek` → `isoyear`, `dow_iso` → `isodow`) onto `DatePartFunc`. |
| R6 | facade `F.hour` / `F.minute` / `F.second` | `python/repark/src/repark/spark/functions_expr.py:1064-1076` → `crates/repark-python/src/column/function_dispatch.rs:521-524`, `:541-548` → `crates/repark-functions/src/expr_fn.rs:225-238` | `_scalar("hour")` builds `call(time_family::hour_udf(), …)`: the same UDF as SQL `hour`. |
| R7 | facade `F.date_part` / `F.extract` | `functions_expr.py:1079-1090` → `function_dispatch.rs:553-556` | `datafusion::functions::expr_fn::date_part`: DataFusion's `DatePartFunc` (as R4), **not** the registry's (R5). So `F.date_part(lit('HOUR'), c)` and SQL `date_part('HOUR', c)` reach different functions. |
| R8 | facade `F.year` … | `python/repark/src/repark/spark/functions.py:832-860` → `expr_fn.rs:122` … | `DatePartUdf` (R2). |
| R9 | `selectExpr`, `F.expr` | the session's SQL expression planner | the same registry and planners as SQL (R1, R4, R5). |

## 2. Coercion: where a `STRING` argument gets its type

| # | site | file:line | what it sees and does |
|---|---|---|---|
| C1 | DataFusion `TypeCoercion` | `datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs:1004-1024`; `datafusion-expr-54.1.0/src/type_coercion/functions.rs:530-536` | for a `user_defined` signature calls `func.coerce_types(arg types)` and wraps each argument in `Expr::Cast` to the answer (`expr.cast_to`). A plain arrow `Cast` unless a later rule rewrites it (C5). |
| C2 | **`DatePartWithoutTime::coerce_types`** | `time_family.rs:444-446` | delegates to the inner `DatePartUdf` (C3). **The seat.** It sees the argument's type (`Utf8`, `LargeUtf8`, `Utf8View`) and nothing else; it has no session settings. |
| C3 | **`DatePartUdf::coerce_types` → `coerce_date_arg`** | `datetime.rs:259-273` → `:167-182`, the arm at `:176-178` | `Utf8 \| LargeUtf8 \| Utf8View \| Null → Date32`, for **every** `part_udf`, `hour` / `minute` / `second` included. **This is the bug:** Spark types `hour`'s input `AnyTimestampType` (a `STRING` becomes `TIMESTAMP`), `year`'s `DateType` (a `STRING` becomes `DATE`). So `hour(c)` plans as `hour(CAST(c AS Date32))` ([evidence/raw/map.md](evidence/raw/), `main-explain.jsonl`). |
| C4 | the Spark door's rule order | `crates/repark-spark/src/extension.rs:160-172`, `:190-193`; `crates/repark-spark/src/spark_literal_typing.rs:50-55`; `crates/repark-functions/src/registration.rs:26-60` | DataFusion's default analyzer (its `type_coercion` included) runs first; `spark_door_post_coercion_rules()` (RePark's `analyzer_rules()` less `spark_integer_literal`) is appended **after** it, with `ltz_timestamp_cast_rule` at `registration.rs:43` and a second `TypeCoercion` at `:45`. So a cast that C1 inserts is seen by C5 in the same analysis. |
| C5 | `SparkLtzTimestampCast::rewrite_cast` | `crates/repark-functions/src/instant_ts.rs:578-641`, the string arm `:625-631`; `wall_kernel_by_target` `crates/repark-functions/src/timestamp_ns_cast.rs:63`, called at `instant_ts.rs:568` | a `Cast` of a string, `DATE` or naive µs value to a non-ns timestamp becomes `to_timestamp(x)` (or a folded literal, `spark_string_literal`); a cast to `Timestamp(ns, _)` becomes the wall kernel `__repark_cast_timestamp_ns__`. It decides by the cast's types, whoever wrote the cast: `hour(CAST(c AS TIMESTAMP))` plans as `hour(to_timestamp(c))`, and `EXTRACT(HOUR FROM c)` as `date_part('HOUR', __repark_cast_timestamp_ns__(c))` (`main-explain.jsonl`). |
| C6 | DataFusion `DatePartFunc` signature | `datafusion-functions-54.1.0/src/datetime/date_part.rs:118-150` | a string source is coerced implicitly to `Timestamp(ns, None)` (a naive wall); C5 turns that cast into the ns wall kernel. No session zone anywhere on this path. |
| C7 | `date_trunc`, `date_format` | `datetime.rs:859-873` (`DateTrunc`), `:952-966` (`DateFormat`) → `coerce_to_timestamp_micros` `:393-404` | a `STRING` stays `Utf8`: no cast in the plan; the text is parsed at invoke (E4). |
| C8 | `trunc`, `add_months` | `datetime.rs:375-390` (`coerce_to_date32`) | `STRING → Date32` (Spark: `DateType`, right type, same arrow parse as C3). |
| C9 | `unix_timestamp` | `crates/repark-functions/src/timestamp_cast.rs:487-566` | the text is parsed with the format (Spark's rule too, `ToTimestamp.inputTypes`, [evidence/javap.md](evidence/javap.md) §7). |
| C10 | `time_cast_guard_rule` | `time_family.rs:462-511` (`TimeCastGuard`) | refuses a non-literal cast to `TIME`; does not touch strings. |

## 3. Evaluation: where the field is read

| # | site | file:line | what it computes for a `STRING` argument today |
|---|---|---|---|
| E1 | **`DatePartUdf::invoke_with_args`** | `datetime.rs:275-298` | an instant (`Timestamp(_, Some(_))`) goes to `instant_part` in the session zone (`:278-281`, `datetime/session_wall.rs:141`); anything else to arrow `date_part` (`:284`). After C3 the input is `Date32`, and arrow's `date_part(Date32, Hour)` is `0` for every row: **the silent `0`**. `Minute` and `Second` likewise. |
| E2 | `DatePartWithoutTime::invoke_with_args` | `time_family.rs:448-459` | refuses `TIME` (`refuse_time`, `:395-404`), then E1. |
| E3 | the Utf8 → Date32 cast (C3, C8) | arrow-cast `Date32Type::parse` | arrow's date parse: no trim, a text longer than 10 characters is parsed as a datetime **in UTC** and its UTC date taken (so `+05:30` moves the date), no `yyyy` / `yyyy-MM` forms, no out-of-range time tail; a failure raises (`Cast error`) in both modes. Spark's `stringToDate` trims, reads only the date segment and ignores the rest. This is why `year` is right on `full`, `tsep`, `frac`, `zulu` and wrong or loud on `off`, `lead`, `trail`, `nosec`, `badtime`, `yonly` (domain table §4). |
| E4 | `invoke_local_micros` | `datetime.rs:418-431`, the cast at `:428` | `date_trunc` / `date_format` cast the text with arrow (`cast` with safe options: an unparsable text is NULL in both modes), read it as a zone-free wall (`LocalSource::ZoneFree`), so an offset or `Z` text is shifted to UTC and not localized. |
| E5 | DataFusion `DatePartFunc::invoke_with_args` | `date_part.rs:195-260` | arrow `date_part` on the naive wall the ns kernel produced; `SECOND` is `seconds_as_i32` (`:402`): an integer, where Spark's `SecondWithFraction` is `decimal(8,6)`. |
| E6 | `to_timestamp` (one argument) | `instant_ts.rs:239-275` → `crates/repark-functions/src/spark_string_timestamp.rs:93-112` (`cast_columnar_strings_to_ltz`; grammar `spark_string_timestamp/grammar.rs:67`, zones `zone.rs:47`, `instant.rs:83`) | Spark's string grammar in the session zone; ANSI on raises `CAST_INVALID_INPUT`, off answers NULL (`StringCastFailure`, `spark_string_timestamp.rs:19`). This is what `hour(CAST(c AS TIMESTAMP))` runs, and the measured control `cast_*` in the domain table. |

## 4. Why `year` is right and `hour` is not

Both reach `coerce_date_arg` (C3) and are cast to `Date32`. For `year`, `DATE` **is** Spark's input type
(`GetDateField.inputTypes = DateType`, [evidence/javap.md](evidence/javap.md) §2), so the plan has Spark's shape and
the answer is right wherever arrow's date parse agrees with Spark's `stringToDate` (E3). For `hour`, Spark's input type
is `AnyTimestampType`, whose implicit cast from `STRING` is `TimestampType` (§3 there): the text's time of day
survives. Main casts it to a `DATE` instead, the time of day is gone, and E1 reads the hour of a midnight: `0`, in
both ANSI modes and every zone, with no error. `minute` and `second` are the same. `date_format(c, 'HH')`, `to_date`,
`unix_timestamp` and `hour(CAST(c AS TIMESTAMP))` are right on the card's two texts because they do not pass C3: C7,
C8 (as a date), C9 and C5 / E6.

## 5. Seen at the seat

| seat | sees | does not see |
|---|---|---|
| C2 / C3 (`coerce_types`) | the argument type only | the session zone, ANSI mode, the text |
| C5 (`rewrite_cast`) | the cast's source and target types, the session zone and `spark.sql.timestampType` (`instant_ts.rs:526-546`) | which function the cast feeds |
| E1 / E2 (invoke) | the arrays, `config_options` (session zone through `session_time_zone_from_options`, ANSI through `crate::ansi::spark_ansi_enabled_from_options`, `ansi.rs:163`) | the plan |

Up: [map.md](map.md).
