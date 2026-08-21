# Campaign retrospective — the low-risk sweep (LRS)

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` off `feat/spark-function-parity` @
`8a28057` · **Charter:** [lrs-0-charter-ledger.md](lrs-0-charter-ledger.md)

Seven units, seven commits, and the invariant held: **no query that worked before this branch
returns a different value after it.**

## What shipped

| Unit | What |
|---|---|
| LRS-5 | canonical Rust module layout — all six `#[path]` inclusions gone |
| LRS-1 | four facade paths refuse a higher-order column instead of leaking a DataFusion internal |
| LRS-2 | `xxhash64()` and the lambda parameter-kind gate matched to Spark |
| LRS-7 | a window with no `ORDER BY` frames the whole partition, as Spark documents |
| LRS-3 | registry rows `RAND-1` / `BL-8` landed with pins; `randstr` batch bound; the SQL door learned `approx_count_distinct` |
| LRS-6 | regexp divergences measured and registered (`RE-1`, `RE-2`) — not forced |
| LRS-4 | the C-012 guard walks the session registry, 20 names → **341** |

**53 new pins.** Rust 45 binaries / 1,991 passed / 0 failed. Facade 3,596 passed / 70 skipped / 0
failed, from 3,548 at the base.

## The finding that matters most is not on that list

Two **silently wrong answers on common functions**, both found while doing something else, both
registered rather than fixed because closing either changes what a working query returns:

- **`RE-1`** — `regexp_extract_all('a1b2', '([a-z])([0-9])')` returns `['a1','b2']`; Spark returns
  `['a','b']`. The two-argument form defaults the capture group to 0 instead of 1, on both doors.
- **`LOG-1`** — `SELECT log(8)` through the SQL door returns `0.903`, DataFusion's base 10; Spark
  and the repark facade both return `2.079`, the natural log.

Neither is exotic. Both look plausible. Neither was reachable by any existing test.

## Four things worth remembering

**The oracle beat the review.** A live PySpark 4.1.2 runs on this machine. Using it to scope the
campaign *before writing any unit* refuted **three** of the Critic round's suggested fixes —
accepting `xxhash64()`, rejecting positional-only lambda parameters, and guarding a non-callable
that already matched Spark. Each would have shipped a new divergence. It also refuted one of my own:
the first LRS-7 fix made five ordering-requiring window functions answer where Spark refuses. An
adversarial reviewer with no oracle produces plausible fixes, and plausible is exactly the failure
mode this campaign exists to catch.

**Two of the seven units were found by pins whose only job was to bound a different fix.** LRS-1's
"an ordinary column still passes this path" went red and became LRS-7. That pin was written out of
suspicion of my own change, not out of a finding. The habit of pinning *what must not change* is
what turned a one-unit sweep into a two-unit one.

**Widening a guard's domain found more than writing new tests would have.** LRS-4 changed no
behavior. It replaced a hand-maintained list of 20 names with the session's own 341 and found four
kernel divergences in a single run — including `LOG-1`, which had been wrong for as long as the door
has existed. A guard that checks a list is a guard someone has to remember to extend.

**Deciding the right answer is the cheap half.** LRS-6 was scoped wrong twice. The oracle settled
*what* the answer should be (5, not 4) and that made the unit look ready; implementing it showed the
fix needs the collector to run in UTF-16 space, because a mid-surrogate offset is not a byte
boundary and Rust's `&str` cannot address one. Charter C-001 was amended to allow that unit, and
then amended back. Recording both amendments is the point — a charter that only ever gets looser is
not a charter.

## What is carried forward

Registered with pins that codify today's behavior, so the fix reds them: `RE-1`, `RE-2`, `LOG-1`,
`UNIX-1`, `BL-8`, `RAND-1`. Left in the design's excluded table with reasons: the unsigned-count
analyzer rule, group-by expression-key naming, `_sort_specs`' truncating `zip`, and the parity
campaign's own remaining units.

Two names the SQL door still does not know: `regexp_extract` (also a declared facade stub) and
`approx_count_distinct` — the latter **closed** by LRS-3.

## Disk

Checked at open and at every unit boundary: **446G → 444G free** across the campaign, `target/`
63G → 64G. Nothing reclaimed, nothing retained beyond the shared `target/` and the cargo registry
that were already there. No worktree was created — the units are sequential and none of them
conflict, so a worktree would have duplicated ~63G of build artifacts for no isolation benefit.
