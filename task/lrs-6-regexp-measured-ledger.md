# Unit ledger — LRS-6 · the regexp divergences, measured and registered

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../docs/design/low-risk-sweep.md) §5 ·
**Source finding:** round 2 F-R3-4

## This unit was scoped wrong, twice, and the second correction is the useful one

F-R3-4 came in as "the empty-pattern collector agrees with `regexp_count` only on BMP text". The
design first excluded it as *medium risk — needs a live Java `Matcher` oracle*. Finding the oracle
appeared to settle it: Spark gives **5** for both `regexp_count('🎉ab','')` and
`size(regexp_extract_all('🎉ab','',0))`, repark's count already gives 5, so only the collector is
wrong. It was scheduled as the campaign's one value-changing unit, and charter C-001 was amended to
say so.

Implementing it showed why that was still wrong. Java finds an empty match at every UTF-16
code-unit index **including the one inside a surrogate pair**. That offset is not a byte boundary,
so Rust's `&str` cannot address it and there is no `regex::Match` to construct there — the existing
doc comment on `collect_matches` says exactly this, and it is right. Closing the gap means running
the collector in UTF-16 space and mapping back, which is a restructure of a hot path, not an
edge-case patch.

**So the unit shipped as measurement and registration**, C-001's exception was withdrawn, and the
campaign's invariant is intact. Deciding *what the right answer is* turned out to be the cheap half.

## What the measurement found instead — and it is worth more than the row it came from

Probing the neighbourhood of F-R3-4 turned up a divergence nobody had filed:

```
regexp_extract_all('a1b2', '([a-z])([0-9])')
  repark  ['a1', 'b2']     facade and SQL door alike
  Spark   ['a',  'b']      the two-argument form defaults idx to 1, not 0
```

That is a **silently wrong answer on ordinary input** — no astral text, no zero-width pattern, just
a capture group. Spark also raises `[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX]` where repark
returns matches for a pattern with no group. The explicit three-argument form already agrees on both
sides, so the fix is a one-line default change — but it changes what **every** two-argument caller
gets back, which this campaign's invariant does not allow. Registered as `RE-1`, pinned to today's
behavior, and flagged to the owner as the highest-value item the sweep found.

## Rows landed

| Row | Kind | What |
|---|---|---|
| `RE-1` | BACKLOG | `regexp_extract_all` two-argument form returns group 0; Spark returns group 1 |
| `RE-2` | BACKLOG | a zero-width match at a mid-surrogate position is missed — 4 where Spark gives 5, and `regexp_substr` gives `''` where Spark gives NULL |

Both pins **codify today's behavior**, per the registry's rule for BACKLOG rows: the unit that fixes
each one reds its pin on purpose.

## Also measured, not registered

`regexp_extract` — the singular — is a declared facade stub (`not supported yet`) and is not a SQL
door function at all, while `regexp_extract_all` works on both. That is a roadmap gap for the parity
campaign, not a divergence, so it stays in STATUS rather than becoming a row.

## Evidence

- 5 pins in `python/repark/tests/test_lrs6_regexp_divergences.py`, all green because they codify
  current behavior. One of them exists to **bound** RE-2: on BMP text every case matches Spark
  exactly, so the row is narrow rather than a general claim that repark's regex engine differs.
- Oracle transcripts for all eight probed shapes are in the rows themselves.
- facade — **3,593 passed, 70 skipped, 0 failed** (3,588 before, +5). `make ci` exit 0.

## Disposition

**DELIVERED as measurement.** No code changed. Charter C-001 restored to its original form.
