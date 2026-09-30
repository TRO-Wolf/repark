# Card SORT-PARENT-COLUMN-1 — `orderBy(parent Column)` on a case-twin frame sorts by the wrong column

**Date:** 2026-09-30 · **Filed by:** a Claude session (claude-fable-5-1) on the owner's ruling of the same day (ATTR-ID-1 OD-3) · **Source:** the fourth re-verify of PR #881, finding RC5-7 (S3, `new_since_base: false`), cells `p4 T_so casetw{,_filt,_lim} parent{,_desc}` (6 cells); the R-CS2-18 family in `task/ledgers/staging/casesens-2-ledger.md` on the PR #881 branch (`feat/casesens-2-s4`, not yet on main).

## What was measured

`caseSensitive=true`; `dd` has `id`, `v`:

```
dd.select((dd.v + 1).alias('V'), (dd.v * -1).alias('v'), dd.id).orderBy(dd.v)
```

Spark sorts by the hidden parent attribute `v` (the pre-projection column) and answers
`[[null,null,2],[null,null,5],[11,-10,3],[21,-20,4],[31,-30,1]]`. RePark on main and on the #881 head sorts by the output twin `v * -1`, so the rows come back reversed. The same holds after `filter`, after `limit`, and with `desc`.

The verifier's cause: the Column sort key rebinds by display name to the output `v` instead of to the parent attribute it was created from.

## Why it is a card and not a fold

Pre-existing on main and outside #881's charter. It is the parent-reference case of the attribute-identity problem: once a Column carries its attribute id (ATTR-ID-1 §3.6), the sort key binds to the parent attribute by id, or to a pass-through projection of it, without any display-name lookup. This card should therefore be scheduled after ATTR-ID-1 S2, not before.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Depends on ATTR-ID-1 S2 (Columns carry `_attr_id`). Do not attempt a display-name fix first. |
| D-2 | A Column sort key whose id is not among the frame's outputs but is among the child's (Spark's "missing attribute" rule) sorts through a pass-through projection of the child attribute, then projects it away. Under both settings. |
| D-3 | Pin the six measured cells, plus the same shape under `caseSensitive=false`, plus `orderBy(dd.v)` where `v` was dropped by the projection (Spark answers; today RePark refuses). |

## Done condition

All D-3 cells EQUAL; the R-CS2-18 rows in the CASESENS-2 ledger point at this card's pins.
