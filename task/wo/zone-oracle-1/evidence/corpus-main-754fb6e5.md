# ZONE-ORACLE-1 corpus on main 754fb6e5

**Date:** 2026-10-20. **Filed by:** Claude (claude-sonnet-5-5), the ZONE-ORACLE-1 builder lane.

**What this is.** The first replay of the ZONE-ORACLE-1 corpus on main `754fb6e5`: 1,640 cells,
328 expressions, each run as a plain `SELECT` through `ReparkSession.sql` in five session
zones and compared with the Spark 4.1.2 answers recorded in `zone_oracle_1_spark_oracle.json`
(`python/repark/tests/timestamps/`). RePark was the `ci`-profile build (`maturin develop --profile
ci`) of main with only this unit's test files added; `spark.sql.ansi.enabled` true on
both sides. The corpus hash is
`72bef6e69102fd6932b0cfffb0acc5b9434830ecc03149afbe69e8e788749a17`. The replay of all cells takes about 4 s.

**Statuses.** EQUAL: the same type and value, or both raise and RePark's text names Spark's error
condition. DIFFERENT: another type or value, Spark raises and RePark answers, or both raise with
another condition. REFUSED: Spark answers and RePark raises a deliberate refusal (the text says
`refus`, `unsupported` or `not supported`). ERROR: Spark answers and RePark raises anything else,
a caught Rust panic included.

## Totals

| status | cells |
|---|---:|
| EQUAL | 1,150 |
| DIFFERENT | 400 |
| REFUSED | 20 |
| ERROR | 70 |
| all | 1,640 |

The 490 cells that are not EQUAL are `zone_oracle_1_main_difference.json`, each with its status and
the measured RePark answer. `test_zone_oracle_1.py` asserts that the non-EQUAL set is exactly that
list.

## Counts per axis

Source: `inst` is the instant literal `TIMESTAMP '2026-01-02 03:04:05.123456+00:00'`, `wall` the
`TIMESTAMP_NTZ` literal, `str` the string, `date` the date, `epoch` the `BIGINT` 1767323045,
`col_*` a column of that type from the one-row temp view. Level: `leaf` is a conversion of a
source with no container, `inner` a container whose leaf is converted inside it, `container` a
cast of the whole container to its own shape (or `to_json` of it), `through` two chained
container casts or a `from_json(to_json(..))` round trip.

### Container

| container | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| bare | 725 | 588 | 107 | 20 | 10 |
| struct | 270 | 179 | 91 | 0 | 0 |
| array | 245 | 167 | 78 | 0 | 0 |
| map | 235 | 117 | 58 | 0 | 60 |
| struct2 | 55 | 33 | 22 | 0 | 0 |
| array2 | 55 | 33 | 22 | 0 | 0 |
| array_struct | 55 | 33 | 22 | 0 | 0 |

### Level

| level | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| leaf | 725 | 588 | 107 | 20 | 10 |
| inner | 255 | 215 | 0 | 0 | 40 |
| container | 600 | 317 | 263 | 0 | 20 |
| through | 60 | 30 | 30 | 0 | 0 |

### Value source

| source | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| inst | 525 | 354 | 146 | 5 | 20 |
| wall | 285 | 159 | 116 | 5 | 5 |
| str | 205 | 176 | 29 | 0 | 0 |
| date | 190 | 148 | 32 | 0 | 10 |
| epoch | 85 | 50 | 35 | 0 | 0 |
| null | 60 | 50 | 0 | 10 | 0 |
| col_inst | 120 | 79 | 16 | 0 | 25 |
| col_wall | 75 | 49 | 16 | 0 | 10 |
| col_str | 45 | 35 | 10 | 0 | 0 |
| col_date | 25 | 25 | 0 | 0 | 0 |
| col_epoch | 25 | 25 | 0 | 0 | 0 |

### Conversion

| conversion | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| `CAST` on the leaf | 455 | 415 | 10 | 0 | 30 |
| `TRY_CAST` on the leaf | 125 | 98 | 27 | 0 | 0 |
| `::` on the leaf | 45 | 45 | 0 | 0 | 0 |
| `to_timestamp_ntz` | 30 | 25 | 5 | 0 | 0 |
| `to_timestamp_ltz` | 30 | 30 | 0 | 0 | 0 |
| `to_utc_timestamp` | 60 | 35 | 15 | 5 | 5 |
| `from_utc_timestamp` | 45 | 25 | 10 | 5 | 5 |
| `date_format` | 60 | 50 | 5 | 0 | 5 |
| `date_format_micros` | 10 | 0 | 0 | 10 | 0 |
| `to_date` | 60 | 50 | 5 | 0 | 5 |
| `unix_micros` | 60 | 30 | 30 | 0 | 0 |
| `CAST` of the container | 435 | 223 | 197 | 0 | 15 |
| `TRY_CAST` of the container | 60 | 24 | 36 | 0 | 0 |
| `to_json` | 105 | 70 | 30 | 0 | 5 |
| chained container casts, `from_json(to_json(..))` | 60 | 30 | 30 | 0 | 0 |

### Target type

| target | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| TIMESTAMP_NTZ | 305 | 238 | 57 | 0 | 10 |
| TIMESTAMP | 275 | 197 | 68 | 0 | 10 |
| STRING | 260 | 160 | 80 | 0 | 20 |
| DATE | 185 | 170 | 10 | 0 | 5 |
| BIGINT | 155 | 70 | 85 | 0 | 0 |
| (no target type) | 460 | 315 | 100 | 20 | 25 |

### Zone

| zone | cells | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|---:|
| UTC | 328 | 254 | 56 | 4 | 14 |
| America/New_York | 328 | 206 | 104 | 4 | 14 |
| Asia/Kolkata | 328 | 218 | 92 | 4 | 14 |
| +05:30 | 328 | 218 | 92 | 4 | 14 |
| Etc/UTC | 328 | 254 | 56 | 4 | 14 |

`UTC` and `Etc/UTC` are identical expression by expression, and so are `Asia/Kolkata` and
`+05:30`: RePark treats the offset spelling like the region zone. The 56 DIFFERENT cells that `UTC` shares with every other
zone are not zone defects: they are type, text and error-condition differences that hold
whatever the zone. The zone seam is the rest: 48 more DIFFERENT cells in `America/New_York`
and 36 more in `Asia/Kolkata` and in `+05:30`.

## The ten most common difference shapes

Each non-EQUAL cell is counted once, under the first shape it matches.

| # | shape | cells | expressions | example cell |
|--:|---|---:|---:|---|
| 1 | A container CAST, TRY_CAST or chained cast whose leaf cast reads the session zone (instant to NTZ or DATE, wall, string or date to TIMESTAMP) answers the UTC reading in every zone | 113 | 45 | `struct.inst.ccast_ntz.new_york` |
| 2 | Container CAST or TRY_CAST to STRING renders Arrow's ISO text `2026-01-02T03:04:05.123456Z`, never Spark's session-wall text | 70 | 14 | `struct.inst.ccast_string.utc` |
| 3 | `map('k', <value>)` panics `list array` at the Python boundary when the value is a column or a function result (MAP-COLUMN-PANIC-1), in a cast, an inner conversion or a read | 60 | 12 | `map.col_inst.to_json.utc` |
| 4 | An error Spark raises with a class (CAST_INVALID_INPUT, UNEXPECTED_INPUT_TYPE, CAST_WITHOUT_SUGGESTION) is raised by RePark as a DataFusion cast or planner error without the class | 45 | 9 | `bare.str.cast_bigint.utc` |
| 5 | Spark raises (a container cast of a date or wall to BIGINT, an NTZ from BIGINT, `to_timestamp_ntz` of an integer) and RePark answers | 35 | 7 | `bare.epoch.to_timestamp_ntz.utc` |
| 6 | Container CAST or TRY_CAST to BIGINT returns microseconds where Spark returns seconds | 30 | 6 | `struct.inst.ccast_bigint.utc` |
| 7 | `to_json` of a TIMESTAMP_NTZ leaf appends `Z`, as if the wall were an instant | 30 | 6 | `struct.wall.to_json.utc` |
| 8 | `unix_micros` of a string, date, wall or integer: Spark raises UNEXPECTED_INPUT_TYPE, RePark answers (wall) or raises a planner error without the condition | 30 | 6 | `bare.wall.unix_micros.utc` |
| 9 | `date_format` refuses the `S` pattern letter; `to_utc_timestamp` / `from_utc_timestamp` refuse an untyped NULL or a date | 30 | 6 | `bare.date.to_utc_timestamp.utc` |
| 10 | Leaf TRY_CAST is not session-aware: STRING renders ISO `T…Z`, BIGINT returns microseconds, TIMESTAMP of a wall, date or epoch is wrong | 27 | 7 | `bare.inst.try_string.utc` |
| | everything else: `to_utc_timestamp` / `from_utc_timestamp` of a wall return TIMESTAMP_NTZ where Spark returns TIMESTAMP (15); `from_json(to_json(instant))` into TIMESTAMP_NTZ answers the millisecond wall where Spark answers NULL (5) | 20 | | |

Three readings of the table that go beyond the card. A leaf conversion inside a
container (`inner`) is EQUAL in every zone wherever the engine answers at all: the lowering of
the leaf cast reaches it, and the defect sits in the cast of the whole container. `to_json`
of an instant leaf is EQUAL in all five zones (`struct.inst.to_json` and its five siblings); it
is `to_json` of a `TIMESTAMP_NTZ` leaf that differs, by a trailing `Z`, in all five. The
container casts differ even in `UTC`: the text of a `STRING` target is Arrow's ISO form, and a
`BIGINT` target is microseconds, so a fix of the zone alone does not turn those cells EQUAL.

## Cells of card CONTAINER-CAST-ZONE-1

The card's rows by cell id. `E` is EQUAL, `D` DIFFERENT, `R` REFUSED, `X` ERROR, one letter
per zone. The control `bare.inst.cast_ntz` is the leaf cast the card says matches Spark; the
card's three plain `SELECT` rows are `array.inst.ccast_ntz`, `struct.inst.ccast_ntz` and
`map.inst.ccast_ntz`; its
"through another type" rows are the `through.*` ids; `to_json` is `*.to_json`.

| expression | UTC | New York | Kolkata | +05:30 | Etc/UTC |
|---|:-:|:-:|:-:|:-:|:-:|
| `bare.inst.cast_ntz` | E | E | E | E | E |
| `bare.inst.cast_ltz` | E | E | E | E | E |
| `bare.inst.cast_string` | E | E | E | E | E |
| `bare.inst.cast_date` | E | E | E | E | E |
| `bare.inst.cast_bigint` | E | E | E | E | E |
| `struct.inst.ccast_ntz` | E | D | D | D | E |
| `struct.inst.ccast_ltz` | E | E | E | E | E |
| `struct.inst.ccast_string` | D | D | D | D | D |
| `struct.inst.ccast_date` | E | D | E | E | E |
| `struct.inst.ccast_bigint` | D | D | D | D | D |
| `array.inst.ccast_ntz` | E | D | D | D | E |
| `array.inst.ccast_ltz` | E | E | E | E | E |
| `array.inst.ccast_string` | D | D | D | D | D |
| `array.inst.ccast_date` | E | D | E | E | E |
| `array.inst.ccast_bigint` | D | D | D | D | D |
| `map.inst.ccast_ntz` | E | D | D | D | E |
| `map.inst.ccast_ltz` | E | E | E | E | E |
| `map.inst.ccast_string` | D | D | D | D | D |
| `map.inst.ccast_date` | E | D | E | E | E |
| `map.inst.ccast_bigint` | D | D | D | D | D |
| `struct2.inst.ccast_ntz` | E | D | D | D | E |
| `struct2.inst.ccast_ltz` | E | E | E | E | E |
| `struct2.inst.ccast_string` | D | D | D | D | D |
| `struct2.inst.ccast_date` | E | D | E | E | E |
| `struct2.inst.ccast_bigint` | D | D | D | D | D |
| `array2.inst.ccast_ntz` | E | D | D | D | E |
| `array2.inst.ccast_ltz` | E | E | E | E | E |
| `array2.inst.ccast_string` | D | D | D | D | D |
| `array2.inst.ccast_date` | E | D | E | E | E |
| `array2.inst.ccast_bigint` | D | D | D | D | D |
| `array_struct.inst.ccast_ntz` | E | D | D | D | E |
| `array_struct.inst.ccast_ltz` | E | E | E | E | E |
| `array_struct.inst.ccast_string` | D | D | D | D | D |
| `array_struct.inst.ccast_date` | E | D | E | E | E |
| `array_struct.inst.ccast_bigint` | D | D | D | D | D |
| `struct.inst.ctry_ntz` | E | D | D | D | E |
| `struct.inst.ctry_string` | D | D | D | D | D |
| `struct.inst.ctry_date` | E | D | E | E | E |
| `array.inst.ctry_ntz` | E | D | D | D | E |
| `array.inst.ctry_string` | D | D | D | D | D |
| `array.inst.ctry_date` | E | D | E | E | E |
| `map.inst.ctry_ntz` | E | D | D | D | E |
| `map.inst.ctry_string` | D | D | D | D | D |
| `map.inst.ctry_date` | E | D | E | E | E |
| `struct.inst.to_json` | E | E | E | E | E |
| `array.inst.to_json` | E | E | E | E | E |
| `map.inst.to_json` | E | E | E | E | E |
| `struct2.inst.to_json` | E | E | E | E | E |
| `array2.inst.to_json` | E | E | E | E | E |
| `array_struct.inst.to_json` | E | E | E | E | E |
| `through.struct_inst_string_ntz` | E | D | D | D | E |
| `through.struct_inst_date_ntz` | E | D | E | E | E |
| `through.struct_inst_bigint_ntz` | D | D | D | D | D |
| `through.struct_wall_string_ltz` | E | D | D | D | E |
| `through.array_inst_string_ntz` | E | D | D | D | E |
| `through.array_inst_date_ntz` | E | D | E | E | E |
| `through.map_inst_string_ntz` | E | D | D | D | E |
| `through.leaf_over_array_inst` | E | D | D | D | E |
| `through.json_inst_ntz` | D | D | D | D | D |
| `through.json_wall_ntz` | E | E | E | E | E |
| `through.json_inst_ltz` | E | E | E | E | E |
| `through.json_wall_ltz` | E | D | D | D | E |


Reading the table. Every `ccast_ntz`, `ctry_ntz` and `through.*_ntz` cell with an `inst` leaf is
`D` in `America/New_York`, `Asia/Kolkata` and `+05:30` and `E` in the two UTC spellings, which is
the card's defect: the UTC wall instead of the session wall. `ccast_date` differs only in
`America/New_York`, where the New York date is not the UTC date. `ccast_ltz` is `E`: a container
cast from an instant to `TIMESTAMP` is the identity. `ccast_string` and `ccast_bigint` are `D`
in every zone including `UTC` (see the shapes above), `through.struct_inst_bigint_ntz` is `D`
everywhere because Spark raises `CAST_WITHOUT_SUGGESTION` and RePark answers, and
`through.json_inst_ntz` is `D` everywhere because Spark answers NULL (an ISO text with a `Z` is
not a `TIMESTAMP_NTZ`) and RePark answers the millisecond wall.

## Other findings the corpus made on the way

- **MAP-COLUMN-PANIC-1 is wider than a column.** The panic `list array` surfaces on a plain
  `SELECT` for `map('k', <column>)` of the one-row view (7 expressions: the cast of the map and
  its `to_json` in four, the inner leaf conversions in three) and also for
  `map('k', <function result>)` where the leaf conversion is lowered to a session-aware function
  (`map.inst.cast_string.inner`, `map.inst.cast_date.inner`, `map.inst.date_format.inner`,
  `map.inst.to_date.inner`, `map.wall.cast_string.inner`: 5 expressions). 60 ERROR cells in all,
  12 expressions in 5 zones. `map('k', <literal>)` and the leaf casts that fold to a literal
  (`map.inst.cast_ntz.inner`) answer.
- **The facade refuses `s[0].a` and `s.a.b` in one expression** (`Dot access not supported
  for non-string expr`). The corpus reads those leaves one subquery layer per step instead; a
  first draft that read them in one expression had 50 REFUSED cells that hid the zone behaviour.
  That refusal is a facade-grammar gap, not a zone defect, and is not in the list.
- **`date_format` refuses the `S` pattern letter** (`unsupported pattern letter 'S'`): the
  10 `date_format_micros` cells are REFUSED. The other `date_format` cells use
  `yyyy-MM-dd HH:mm:ss` so the zone shows.
- **Leaf `TRY_CAST` is not session-aware** the way leaf `CAST` is (`bare.inst.try_string`,
  `bare.inst.try_bigint`, `bare.wall.try_ltz`, `bare.epoch.try_ltz`, `bare.date.try_ltz`).
- **`to_utc_timestamp` and `from_utc_timestamp` of a wall literal or column return
  `TIMESTAMP_NTZ`** with a shifted wall, where Spark casts the wall to `TIMESTAMP` in the
  session zone first and returns a `TIMESTAMP` (`bare.wall.to_utc_timestamp`,
  `bare.wall.from_utc_timestamp`, `bare.col_wall.to_utc_timestamp`).

## What the corpus does not cover

Axes kept as ordered: every source, leaf conversion, container, target type and zone of the
order's §1. Not in the corpus, by design: write doors (the point of the oracle is that a
write stores what the `SELECT` yields), `timestamp_ns`, the ANSI-off mode (one mode, Spark 4's
default), and a `MAP` with a non-literal key. Cell count: 1,640, about 9 % above the order's
target of about 1,500; the container-cast, `TRY_CAST` and `to_json` families are the bulk.

## Reproducing

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 /tmp/sparkenv/bin/python python/repark/tests/timestamps/_record_zone_oracle_1.py --check
PYTHONPATH=python/repark-parity/src python python/repark/tests/timestamps/_record_zone_oracle_1.py --main --check
```

The first re-derives the Spark fixture and exits non-zero on drift; the second replays the corpus
on the installed build and compares the result with `zone_oracle_1_main_difference.json`.
