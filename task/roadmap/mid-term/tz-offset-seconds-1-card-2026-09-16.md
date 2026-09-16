# TZ-OFFSET-SECONDS-1 — fixed session offsets carried as seconds east of UTC (1.6 card)

**Filed:** 2026-09-16 by run 18c under owner ruling **Q-17c-1** ("sub-minute fixed offsets stay a dated DECLARED divergence
in 1.5; a 1.6 card carries offsets as seconds east of UTC"). **Target:** 1.6. **Severity:** P3 — a spelling almost nobody
uses, but the root of a Java/Arrow grammar mismatch. **No code in 1.5.**

## The divergence it closes

Registry row **SET-ANSI-RUNTIME-4** (`docs/spark-sql-iceberg-parity.md`): `spark.sql.session.timeZone = "+05:30:30"` is
accepted by PySpark 4.1.2 and answers values in +5h30m30s (`from_unixtime(0)` → `1970-01-01 05:30:30`); repark refuses
with `[INVALID_CONF_VALUE.TIME_ZONE]` and keeps the previous zone. Zero-second forms (`+18:00:00`, `+053000`, `+05:30:00`)
are accepted and canonicalise to `±HH:MM`.

Measured by run 17c (`fixtures-batch17-zone-spellings.json`, 15 cells, live PySpark 4.1.2, 2026-09-16): five of fifteen
cells diverged after the first fix, four closed under ruling R-17c-6 ("Spark's acceptance set is the specification in both
directions"), and this fifth shipped as the dated declaration. Report: `overnight-report-2026-09-16-17c.md` §3–§4.

## Why it is not a 1.5 fix

Arrow's `Tz::from_str` is the only zone parse on every value-bearing path, and `Tz` has no seconds-offset representation.
The three parse sites are `timestamp_cast.rs::parse_session_zone`, `datetime.rs::extraction_time_zone` and
`spark_from_unixtime.rs` (through `parse_session_zone`). The frozen builder parse is `Tz::from_str` verbatim and refuses
these spellings at build time.

## The design

1. The runtime zone carrier (the SET-ANSI-RUNTIME-1 snapshot: the Spark-visible text plus a canonical companion) gains a
   typed zone: `Region(chrono_tz::Tz)` or `Fixed(i32 seconds east of UTC)`, with the range Java's `ZoneOffset` accepts
   (±18:00:00).
2. The three parse sites take the typed zone instead of a string. A `Fixed` offset converts with integer arithmetic on the
   epoch value; it never goes through `Tz`.
3. Arrow timestamp metadata still needs a zone string. For a `Fixed` offset with non-zero seconds, the Arrow field carries
   the nearest representation Arrow accepts, and the values are shifted by the typed offset. Measure first whether
   Arrow's `+HH:MM:SS` form is accepted by the arrow-rs version on main at the time the card opens.
4. The Python `tzinfo` bridge (`session_time_zone.py`) builds `datetime.timezone(timedelta(seconds=n))`, which does carry
   seconds.
5. Keep the case-sensitive and no-trim acceptance rules of R-17c-6.

## Oracle cells to re-measure on the day

`fixtures-batch17-zone-spellings.json` (all 15), plus: `from_unixtime`, `date_format`, `hour`/`minute`/`second`,
`to_utc_timestamp`/`from_utc_timestamp`, `current_timestamp()` display, `CAST(ts AS STRING)`, `createDataFrame` of a naive
`datetime` and `collect()` of a timestamp, all under `+05:30:30`, `-00:00:01` and `+18:00:00`.

## Clauses (draft)

- **C-001** The carrier holds `Fixed(seconds)`; the gate accepts every seconds spelling Spark accepts and refuses the rest.
- **C-002** The three engine parse sites answer Spark's values under a sub-minute offset.
- **C-003** The Python door's `collect()` / `createDataFrame` round-trip under a sub-minute offset.
- **C-004** SET-ANSI-RUNTIME-4 → FIXED; the declared-divergence pin flips to a parity pin.

## Pointers

- Up: [map.md](map.md) · Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
