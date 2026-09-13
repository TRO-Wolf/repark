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

## Step-3 re-measure — step-2 table vs this branch (median ms)

Same runner method (warmup + 3 reps, medians, `wait_for_idle` per cell, one
fresh process per shape) on a release native (`__debug_assertions__` False).
This run used the card-mandated `systemd-run --user --scope -p MemoryMax=8G`
cap rather than step 2's `prlimit --as=8589934592`; see the nested/polars notes.
Bar: `rows` and `dicts` faster; every other shape not slower than 5 %.

| cell | 1e4 create | | 1e5 create | | 1e5 +count | |
|---|---:|---:|---:|---:|---:|---:|
| | s2 → s3 | Δ | s2 → s3 | Δ | s2 → s3 | Δ |
| tuples | 42.08 → 22.31 | −47.0 % | 437.50 → 230.00 | −47.4 % | 429.26 → 233.18 | −45.7 % |
| rows † | 73.99 → 23.10 | −68.8 % | 793.48 → 248.54 | −68.7 % | 768.00 → 251.98 | −67.2 % |
| dicts † | 61.20 → 24.41 | −60.1 % | 641.20 → 255.72 | −60.1 % | 633.92 → 250.64 | −60.5 % |
| tuples + DDL | 42.71 → 22.93 | −46.3 % | 448.31 → 242.15 | −46.0 % | 443.44 → 234.95 | −47.0 % |
| tuples + StructType | 43.28 → 22.66 | −47.6 % | 450.21 → 239.57 | −46.8 % | 455.28 → 234.22 | −48.6 % |
| nested | 16.91 → 18.84 | +11.4 % | 206.96 → 236.54 | +14.3 % | 205.85 → 234.14 | +13.7 % |
| pandas (control) | 46.49 → 47.04 | +1.2 % | 445.99 → 447.51 | +0.3 % | 449.26 → 442.99 | −1.4 % |
| polars (control) | n/a → 42.84 | +0.2 %* | n/a → 436.18 | +2.9 %* | n/a → 440.30 | +5.2 %* |

\* polars has no s2 value (the step-2 prlimit finding); deltas are vs the s1
baseline (42.74 / 424.07 / 418.59). It is measurable under `MemoryMax` because
an RSS cap does not trip jemalloc's ≈7.7 GiB address-space reservation the way
`prlimit --as` did.

**Nested is not a regression — cap-method artefact, proven by A/B.** The nested
path (tuple rows; no named-funnel, no temporal cells) carries no step-3 source
change at all. An s2 wheel built from `4f121ab9` was extracted and its
`_native.abi3.so` swapped into this tree: on today's box under `MemoryMax` the
*s2 binary* measures **243.04 ms** at 1e5 create while the s3 binary measures
**236.54 ms** — the branch is ~3 % *faster* than step 2's code. The s2 table's
206.96 was recorded under `prlimit --as=8G`, which constrains mimalloc's arena
reservation and systematically lowered allocation-heavy cells (nested
allocates a small container per cell; pandas/polars use bulk Arrow buffers and
moved only ~0–3 %).

**Bar: met.** `rows` −69 % and `dicts` −60 % at 1e5 create — the Python
named-row funnel (~60 % of both walls in the baseline split), the per-row
`asDict` dict allocation, the per-row union sort, and the `timetuple` per-cell
cost are gone. Tuples and the explicit-schema pair drop a further ~46–48 % on
top of step 2 (their date/timestamp columns rode the F-TIMETUPLE route).
Controls are flat to +2.9 %.

† `rows`/`dicts` re-measured after the S2-21 review remediation (C-026):
`1d039a38` reads `_Row__field_values` by index through per-distinct-fields
maps instead of paying `asDict()` per row; `8608cb5c` skips the union
extract/sort for mappings whose keys are all already seen; `4996f6fd` interns
the `timedelta` attribute names. Same runner, same cap, same box.

## Fallback shapes @ row 90 000 — real-base bar (release native)

The S2-21 Python-review re-measure (C-026 follow-up) — this table is the
bar. Poison seeded at index 90 000 of 100 000 rows; `base` is
`/tmp/f-rev3/base` (release, `main` @ `9efb6a65`, read-only); `branch` is
this tree. Interleaved cells per shape, warmup + 5, medians,
`systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0`.

| fallback shape | base ms | branch ms | Δ | pinned outcome |
|---|---:|---:|---:|---|
| `object()` cell in a dict list | 570.05 | 578.63 | +1.5 % | PySparkTypeError |
| int→float in a tuple list | 358.25 | 360.01 | +0.5 % | PySparkTypeError |
| non-`Row` element in a `Row` list | 85.24 | 71.74 | −15.8 % | PySparkTypeError |
| non-dict element in a dict list | 9.20 | 4.48 | −51.3 % | PySparkTypeError |
| strict key-set mismatch in a `Row` list | 209.00 | 197.61 | −5.4 % | PySparkValueError |
| `object()` cell in a `Row` list | 637.95 | 635.38 | −0.4 % | PySparkTypeError |

Every shape is within +5 % of the real base; refusal class, message and
index are unchanged (Python owns each raise). F-PY-2's fix (`b1c8b90f`)
skips the tuple-door native export once `cdf_arrow_export_named` has
declined — `named_declined` routes the rebuilt tuples straight to
`_arrow_table_from_raw_tuples_fast`/`_legacy`, and a spy pin proves
`cdf_arrow_export` stays at 0 calls on declined dict and Row lists. The
negative deltas come from cheapening the doomed path, not weakening a
refusal: `row_field_tuples` len-checks mid-list field tuples (set coverage
still enforced per distinct tuple in `build_row_cells`) and
`_rows_from_mapping_list` no longer pays a per-iteration `import Row` or
an identity `as_mapping` copy on dict rows.

### Simulated-export history (C-014 method)

Recorded before the real-base re-measure; kept for provenance only — the
real-base table above is the bar. `literal-sim` patches only
`_native.cdf_arrow_export` to `None` in the same process; `main-sim`
patches `cdf_arrow_export_named` too (the pre-step-3 path on these
inputs). Medians of 3 after a warm call.

| fallback shape | branch ms | literal-sim Δ | main-sim Δ | pinned outcome |
|---|---:|---:|---:|---|
| `object()` cell in a dict list | 607.12 | +1.6 % | +7.3 % | PySparkTypeError |
| int→float in a tuple list | 355.29 | +4.1 % | +4.0 % | PySparkTypeError |
| non-`Row` element in a `Row` list | 89.81 | −0.1 % | +3.4 % | PySparkTypeError |
| non-dict element in a dict list | 10.33 | −0.5 % | +8.1 % | PySparkTypeError |
| strict key-set mismatch in a `Row` list | 220.36 | +2.8 % | +10.0 % | PySparkValueError |

The main-sim column records the honest cost of the native attempt itself:
a doomed input still pays one cheap whole-list probe (pointer type-check
≈0.8 ms; `_Row__field_names` validation ≈19 ms; dict union+tag ≈40 ms)
before Python's own refusal walk — the probe is what lets the export
decline *before* any `asDict()` call or cell extraction.

## Pointers

- Step-2 table + polars-cap finding:
  [facade-3-cdf-step2-2026-09-13.md](facade-3-cdf-step2-2026-09-13.md)
- Step-1 baseline + runner:
  [facade-3-cdf-baseline-2026-09-13.md](facade-3-cdf-baseline-2026-09-13.md)
- Ledger: [../../task/ledgers/completed/facade-3-ledger.md](../../task/ledgers/completed/facade-3-ledger.md)
