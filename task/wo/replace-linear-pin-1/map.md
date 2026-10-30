# task/wo/replace-linear-pin-1/ — REPLACE-LINEAR-PIN-1

REPLACE-LINEAR-PIN-1 (2026-10-22, owner ruling "go with recommendations"): the depth-40 memory pin of REPLACE-LINEAR-1,
`test_replace_dict_depth40_memory_linear`, read the wheel's code-page layout as well as the operation, so it failed on
pull request #1140 and on no change to `replace`. The order is one change to the pin's child: after the existing
warm-up select and before it reads `VmHWM`, the child runs the SAME mode once at depth 1 (a one-column flat select for
`flat`, a one-entry `replace` dict for `replace`) and collects it. `VmHWM` stays the measure; `_DEPTH`, `_HEADROOM`,
`_DELTA_FLOOR` and the bound `max(2 × control, floor)` stay byte for byte. The bound is never raised, the pin is never
skipped or marked xfail, and no CI run is repeated to find a pass. The unit built on branch `fix/replace-linear-pin-1`
from main `0512470d` (builder Claude Sonnet 5.5, orchestrator review). PR #1140 waits behind it.

The measurement came first, on the runner, on throwaway draft pull requests (#1144 on main, #1145 on the #1140 head,
two builds each, then #1146 on the final head). The child printed `VmHWM`, `VmRSS`, `RssAnon`, `RssFile`, `RssShmem` and
the resident size of the `_native` mapping before and after each mode. In all eight reads the delta is file pages inside
the `_native` mapping: `RssFile` growth equals the mapping's growth to the byte, and `RssAnon` grows 74 to 106 KB
whatever the build. The two #1140 builds carry the same `.so` (sha256 prefix `11fad1e8`), yet the replace delta read
8.26 MB on one runner and 9.90 MB on the other, with the control at 4.59 MB and 3.67 MB, so the variance is runner
state and the flat control inherits it in the opposite direction: a low control lowers `2 × control` to the 8 MiB
floor while replace reads high. With the warm-up the local deltas over ten runs are 0.79 MB for flat and 0.95 MB for
replace, of which 0.72 MB and 0.85 MB are file pages that only the 40-wide statement touches; without it they are 4.85
MB and 7.78 MB. On the runner, on the final head, they are 0.34 MB and 0.49 MB against the 8 MiB bound. The floor therefore becomes the effective bound, and C-002's recorded blow-up (22.9 MB at N=12, 290.6 MB
at N=16, death at N=40 on the base tree) sits far above it.

The evidence is in [evidence/](evidence/map.md): the eight-row runner table and its raw lines, the ten local deltas for
both modes with and without the warm-up (the mutation check, not committed), the pin file ten times, the dataframe
directory once under `-n 8`, and the runner read of the final head. The unit's ledger is
[../../ledgers/staging/replace-linear-pin-1-ledger.md](../../ledgers/completed/replace-linear-pin-1-ledger.md). The
completed REPLACE-LINEAR-1 ledger is not edited.

Up: [../map.md](../map.md).
