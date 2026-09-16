# BL11-ENCODER-PERF-1 — the numeric → BINARY encoder writes fixed-width output without a per-row builder

**Filed:** 2026-09-16 by run 18c under owner ruling **Q-17c-5** ("the BL-11 encoder cost is a perf card; no code today").
**Severity:** P3 perf, not a 1.5 blocker. **Kernel:** `crates/repark-functions/src/int_to_binary.rs`
(`IntToBinaryCast`, `__repark_int_to_binary__`), merged in #641 (BL-11, `0ef060af`).

## The measurement it starts from

Run 17c's Rust-perf reviewer (Grok 4.6, read-only, 2026-09-16, head `44bd26ef`), release native, a cached 1 000 000-row
`INT` column, ANSI off, `to_arrow()`:

| Op | min | median | output bytes |
|---|---|---|---|
| identity `v` | 0.84 ms | 0.96 ms | 4 MB |
| `CAST(v AS BIGINT)` | 3.42 ms | 5.65 ms | 8 MB |
| `CAST(v AS BINARY)` | 5.39 ms | 8.58 ms | 8 MB |
| 8× `CAST(v+i AS BINARY)` | **26.81 ms** | 46.16 ms | 64 MB |
| 8× `CAST(v+i AS BIGINT)` | **18.35 ms** | 21.21 ms | 64 MB |
| literal `CAST(1 AS BINARY)` over 1e6 | 1.76 ms | 1.78 ms | 8 MB |
| all-null `CAST INT→BINARY` | 2.59 ms | 2.66 ms | 4.1 MB |
| 50 % null `CAST INT→BINARY` | 2.28 ms | 2.49 ms | 6.1 MB |

About **1.5×** the wall clock of the width-equivalent integer cast on the minimum, roughly one millisecond per million
rows per cast. There is no per-row heap allocation; the cost is the per-row `BinaryBuilder::append_value` call, the offset
push and the endian swap. The all-null path reserves `len × W` value bytes it never writes. Spark's JVM encoder was not
measured.

## The change

1. Fixed width is known per source type (1/2/4/8 bytes). Build the value buffer in one pass: `values.iter().flat_map(to_be_bytes)`
   into a `Vec<u8>` of `len × W`, or a bulk byteswap of the native buffer.
2. Offsets are arithmetic (`0, W, 2W, …`): build them as a `ScalarBuffer<i32>` from a range, not per row.
3. Clone the input's `NullBuffer` into the output instead of rebuilding validity bit by bit.
4. Null slots keep `W` value bytes (zeroed) so the offsets stay arithmetic. This changes the output buffer's byte size for
   null-heavy columns, not any visible value. Record that choice in the ledger.
5. All-null input: short-circuit to `new_null_array`.

## Clauses (draft)

- **C-001** The same bytes as today for every width, sign, null pattern and batch offset (a sliced input array). Pinned by
  a Rust property test against the current per-row encoder kept as the test oracle.
- **C-002** A criterion or `to_arrow` timing on the same 1e6 cached shape shows 8× `CAST AS BINARY` within 1.15× of
  8× `CAST AS BIGINT` on a release native, with numbers in the ledger.
- **C-003** BL-11's oracle pins stay green on both doors.

## Pointers

- Up: [map.md](map.md) · Kernel map: [../../../crates/repark-functions/src/map.md](../../../crates/repark-functions/src/map.md)
- Source report: [overnight-report-2026-09-16-17c.md](overnight-report-2026-09-16-17c.md) §9 Q-17c-5
