# ICE-HELD-FRAME-SNAPSHOT-1 unit 1a — cost

Main is `S/ddu-work/pkg-main-9a48932f` (main's product tree is unchanged through `a4d8857f`). The round-0 head is
`pkg-ice1a-58c7e307`; the fix-round head is `pkg-ice1a-eced2285` (product commit `eced2285`). Every sample ran under the
exclusive measure lock, base against head against base again: 10 held rounds and 5 C-012 rounds per build. Medians;
"head / mean" is the head over the mean of the two base runs.

## C-012 (the hard 1.05x bar)

| case | round-0 head / mean | fix-round base | head | base again | head / mean |
|---|---|---|---|---|---|
| `c012p6` (20,386 cells, s) | **2.456** | 39.26 | 39.42 | 39.30 | **1.004** |
| `c012rest` (s) | 1.015 | 43.04 | 43.10 | 42.82 | **1.004** |

**C-012 passes on the fix-round head.**

## Why the round-0 head missed (F0): one more schema layer per statement

Measured before any code changed, on the round-0 head against main:

- **The cost grew with the session, not per reference.** A replay of only the 801 `F|ice|` cells of `c012p6` cost
  0.9 ms more per cell at the start and 4.5 ms more at the end. In the full run, after about 9,700 non-Iceberg cells,
  the same cells started at 20 ms more and ended at 27 ms more; the `T|ice|` cells, later still, 43 to 52 ms. Within
  a block the excess climbed in the order the cells ran (by constructor, 18.4 ms for the first to 26.1 ms for the
  last), whatever the action. The `T` cells are the case-sensitive half, which runs later; they do not reference two
  frames.
- **The profile** (`perf record -e cycles:u --call-graph dwarf --user-callchains` over the `F` cells) put
  `ViewSchemaProvider::table` at 5.2% inclusive, `CapturingSchema::table` at 1.8% and `GrownStack::poll` at 3.1%,
  with two thirds of the stacks cut at the grown-stack segments.
- **The cause.** `ReparkCatalogProvider::schema` returned a new `capturing_schema` `Arc` on every call. The Spark door
  runs `ensure_view_wrappers` on every statement (`guard_statement`, `execute_view_body_query`); it compares the
  installed view wrapper with `schema()` by `Arc::ptr_eq`, so the check always failed, and each statement registered
  one more `ViewSchemaProvider` over one more `CapturingSchema` over the previous chain. Every Iceberg table
  reference then resolved through every layer, each on its own grown stack. Main's check holds, so main installs the
  wrapper once.
- **Each table reference now** costs one view wrapper and one capturing layer, as main costs one view wrapper: the
  same `F` replay on the fix-round head runs its 801 Iceberg cells at 1.66 ms each, flat from first to last, against
  23.8 ms on the round-0 head. The fix is F1's (each Iceberg schema is wrapped once, where it is registered).

## The short rows

| row (ms) | round-0 head / mean | fix base | head | base again | head / mean |
|---|---|---|---|---|---|
| `held_nowrite` | 1.075 | 11.05 | 10.89 | 9.95 | 1.037 |
| `held_write` | 1.195 | 10.96 | 11.91 | 11.42 | **1.064 (flag)** |
| `fresh_write` | 0.935 | 14.54 | 14.30 | 13.88 | 1.007 |
| `plan_100` | 1.036 | 0.55 | 0.52 | 0.51 | 0.967 |
| `held_filter_count` | 1.141 | 11.45 | 13.74 | 11.64 | **1.190 (flag, over 1.15x: the owner's call)** |

**Attribution by instruction counts** (`perf stat -e instructions:u`, a 2,000,000-row table, one insert after the
frame is held, the process at 40 actions less the process at 0, per action):

| action | main | fix-round head | head / main |
|---|---|---|---|
| held read (`toArrow`) | 262.70 M | 262.69 M | 1.000 |
| fresh read after the insert | 264.20 M | 263.80 M | 0.998 |
| held `filter(n >= 0).count()` | 216.51 M | 216.17 M | 0.998 |
| fresh `filter(n >= 0).count()` after the insert | 220.61 M | 220.47 M | 0.999 |

The re-bind adds no user-space instructions to a held action. The two flagged rows are not like for like on main:
main's held frame reads the old snapshot (one data file), and the head reads the table as of the action (that file and
the inserted row's file), which is Spark's answer. Main's own fresh read of the same two files costs more instructions
than its held read (+0.6% and +1.9%).

**Repeated actions, like for like** (the same table and insert, 41 actions per process, the median, 3 processes per
build and shape, under the exclusive lock): a held `count` 11.47 ms on the head against 11.42 ms on main (1.004x), a
held read 11.14 ms against 11.39 ms (0.978x); a fresh `count` after the insert 12.74 against 13.23, a fresh read
12.92 against 13.35. In steady state a held action costs what main's does. The cost series times only the first
action after the insert, and on the head that action is the first read of the new snapshot (its manifest list and new
manifest, and the re-bound provider), which main's held frame never reads. Not split further this round:
`held_filter_count` stays at 1.19x in the series' shape, over 1.15x, so it goes to the owner.

Up: [map.md](map.md).
