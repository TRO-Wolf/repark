# FACADE-3 createDataFrame step-3 baseline (2026-09-13)

Step-3 baseline for the [step-2 state](facade-3-cdf-step2-2026-09-13.md), measured
on the release native before any step-3 product change (`__debug_assertions__` is
`False`). Two tables: the cProfile split that shows where the `rows` and `dicts`
shapes still spend their time, and the F-TIMETUPLE candidate micro-benchmark the
extraction-route decision is built on. The step-2 table above this doc stays the
comparison baseline for the step-3 re-measure.

pins: facade-3/C-018

## cProfile split — `rows` and `dicts` at 1e5 (release native)

One `session.createDataFrame(data)` per shape under `cProfile`, after a warm
call. Shares are cumulative-time against the profiled wall (profiling inflates
the absolute wall; shares are the signal). The step-1 runner fixtures
(`build_shape("rows"|"dicts")`) built the data.

| segment | rows (wall 1 260.3 ms) | dicts (wall 1 280.9 ms) |
|---|---:|---:|
| `_rows_from_mapping_list` (Python funnel, total) | 750 ms — 59.5 % | 779 ms — 60.8 % |
| &nbsp;&nbsp;`_bind_named_row` | 270 ms | 379 ms |
| &nbsp;&nbsp;`_apply_permutation` (+genexpr) | 166 ms | 167 ms |
| &nbsp;&nbsp;`asDict` (+lambda) | 138 ms | — |
| &nbsp;&nbsp;`_spark_dict_key_union_order` | — | 84 ms |
| &nbsp;&nbsp;`dict.get` / mapping genexpr | 78 ms | 350 ms |
| `_arrow_table_from_raw_tuples` → `_rust_cdf_arrow_table` | 493 ms — 39.1 % | 495 ms — 38.6 % |
| &nbsp;&nbsp;`{repark._native.cdf_arrow_export}` | 492 ms | 494 ms |
| `pa.table(export)` capsule drain + MemTable register | ≈1 ms — <0.1 % | ≈1 ms — <0.1 % |

The Python named-row funnel — homogeneity walk, `as_mapping`/`asDict`,
`_bind_named_row`, `_apply_permutation` with an identity permutation — is ~60 %
of both walls on the current tree; the native call is ~39 %; the capsule drain
and MemTable registration are noise. That is the F-FUNNEL wall step 3 removes:
`Row` and dict lists go into the native export directly, the bind/permutation
walk happens in Rust, and any refusal the native screen cannot reproduce returns
`None` before extraction so Python owns the pinned exception (the C-014 rule).

## F-TIMETUPLE candidate micro-benchmark (release native)

The wheel is abi3 (`abi3-py312`): PyO3's `PyDateTime`/`PyDate` getters and the
`PyDateTime_CAPI` macros are unavailable under `Py_LIMITED_API`, so every
candidate is ordinary Python-level attribute/method access from Rust. A
temporary probe (`cdf_temporal_probe`) ran each candidate over 1e5 cells of
`date`, naive `datetime`, and UTC-aware `datetime` (`timezone.utc`), median of
4 reps after 1 warmup. The `tzinfo` attribute read per cell is common to all
datetime modes. Checksums were identical across modes inside each fixture — the
candidates agree on values, not only on speed.

| mode | route | date | naive `datetime` | aware `datetime` |
|---|---|---:|---:|---:|
| 0 | `timetuple()` + `microsecond` getattr (current) | 98.39 | 125.26 | 194.69 |
| 1 | interned `getattr` year/month/day/hour/minute/second/microsecond | 11.79 | **25.58** | 58.55 |
| 2 | `toordinal()` + interned time attrs | 13.20 | 27.80 | 56.79 |
| 3 | `obj - epoch` timedelta + `days`/`seconds`/`microseconds` | **10.16** | 38.98 | 65.72 |
| 4 | mode 3 + `utcoffset` cache (exact `datetime.timezone` only) | — | 38.64 | 40.60 |
| 5 | mode 1 wall + `utcoffset` cache (exact `datetime.timezone` only) | 9.12* | 26.77 | **31.87** |

\* mode 5 hits the same `obj - epoch` route as mode 3 for `date`; 9.12 vs 10.16
is the same code inside the noise band.

**Chosen routes** (each >5 % under the current path, so no HALT): `date` —
subtract a cached `date(1970,1,1)` and read `.days` (−90 %); naive `datetime` —
seven interned `getattr`s (−80 %); aware `datetime` — the same seven getattrs
for the wall clock plus `utcoffset` cached by `tzinfo` object identity, armed
only when `type(tzinfo) is datetime.timezone` (a fixed-offset tz whose offset
cannot vary by instant; `zoneinfo` and friends still pay the per-cell call —
−84 %). Wall-clock fields stay the source of truth, so `fold`, subclass
`__sub__` overrides, and pandas `Timestamp` nanoseconds cannot leak through the
subtract path for datetimes.

## Pointers

- Step-2 table + polars-cap finding:
  [facade-3-cdf-step2-2026-09-13.md](facade-3-cdf-step2-2026-09-13.md)
- Step-1 baseline + runner:
  [facade-3-cdf-baseline-2026-09-13.md](facade-3-cdf-baseline-2026-09-13.md)
- Ledger: [../../task/ledgers/staging/facade-3-ledger.md](../../task/ledgers/staging/facade-3-ledger.md)
