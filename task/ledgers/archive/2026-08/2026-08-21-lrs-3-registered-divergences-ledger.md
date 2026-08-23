# Unit ledger — LRS-3 · write down what was already decided

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](2026-08-21-lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §3 LRS-3 ·
**Source finding:** round 2 F-R3-9, plus one gap this campaign measured

## Two registry rows, because a promise is not an artifact

The parity branch shipped six S2 divergences described as *"registry row handed to FNP-Z"*. The
registry's own rule (§6) is blunt about that: **a row lands with its pin, in the same change, or it
does not land** — "an unpinned divergence is prose, and prose is what this registry exists to
replace". Two of those promises are now rows.

**`RAND-1` (DECLARED)** — repark caps `randstr` at 1,000,000 characters. Spark has no cap:
`SELECT length(randstr(5000000, 1))` returns **5000000** (oracle, live). The cap is a *safety
limit, not a parity claim*, and nothing said so anywhere a user would look.

**`BL-8` (BACKLOG)** — the SQL door hands back `UInt64` for count-like aggregates where Spark and
the repark facade both give `bigint`. Registered rather than left in STATUS because **the cost of
this gap is on disk**: a `UInt64` column written to Parquet or Iceberg is read back by Spark as
`decimal(20,0)` and does not round-trip. Its pin is the FNP-5 ratchet, which asserts the door is
*still* unsigned — so closing the row turns it red on purpose.

Per §6 "nothing is stated in both places", both descriptions were **removed** from STATUS.md, which
now keeps one line of state and a link. The same pass truth-ed up two neighbouring claims that LRS
had already made false — the `#[path]` conversion is done, and FNP-6a's residual is decided.

## The batch bound, because a caught panic is not a contract

Round 2 showed the per-row cap was not sufficient on its own: a **legal** length times a large batch
still overflows the i32 offsets of an Arrow `StringArray` and panics inside arrow-rs. It was caught
at the PyO3 boundary rather than aborting, so it was never unsafe — but the user saw a panic, not a
limit. `randstr` now refuses `length x rows` past `i32::MAX` and names both numbers.

## The door did not know Spark's own name

Measured while writing `BL-8`: `SELECT approx_count_distinct(g)` fails on the SQL door with
`Invalid function 'approx_count_distinct'`. Spark SQL has that name; DataFusion calls it
`approx_distinct`. The facade resolved both from its own dispatch table, which is exactly why no
test could see it — **the facade never went through the door for that name**, so the two-door parity
tests had no way to notice.

Fixed by registering the UDAF under both spellings in `aggregate::functions()`. It went there rather
than in `register_all` because the crate root sits at its 175-line ceiling, and `functions()` is
already the list `register_all` installs — so the alias arrives by the same route as every other
repark aggregate overwrite rather than by a special case.

## Evidence

- 6 pins in `python/repark/tests/test_lrs3_registered_divergences.py`. Both new refusals measured
  failing the old way first: the batch case panicked inside arrow-rs, the door name returned
  `Invalid function`.
- Oracle: Spark returns 5,000,000 characters for `randstr(5000000, 1)`, and `bigint` for
  `approx_count_distinct` through SQL — both transcribed into the rows.
- Registry IDs checked unique (`FN-1` was taken; the row is `RAND-1`).
- `cargo test --workspace` — 45 binaries, **1,990 passed, 0 failed**.
- facade — **3,588 passed, 70 skipped, 0 failed** (3,582 before, +6). `make ci` exit 0.

## Disposition

**DELIVERED.** Charter C-001 held: the batch bound replaces a panic, and the alias makes a name
resolve that did not resolve at all.
