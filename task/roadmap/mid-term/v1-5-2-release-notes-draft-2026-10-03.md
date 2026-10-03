# v1.5.2 release notes — DRAFT (opened 2026-10-03 by TA-CHAIN-1)

**Draft, not final.** These notes cover `origin/main` from `db3a1f37` (tag `v1.5.1`) to the
v1.5.2 tag. The release clerk finalizes and renames this file at tag time and adds the other
v1.5.2 units' sections (owner amendment to the TA-CHAIN-1 order, 2026-10-03, R-TC1-8). Every
line below comes from a commit subject, a staging ledger or a work-order ruling.

## TA indicators: chained indicators answer (TA-CHAIN-1)

Chained TA indicators (an indicator computed from another indicator's output) now answer where polars_talib answers; a leading NaN or NULL run on any `ta.*` input is skipped instead of propagating.

Before this fix, `ta.ema(ta.trange(high, low, close), 21)` answered NaN on every row, because
TRANGE's first output row is NaN and the C-faithful kernel carried that NaN through EMA
forever. The `ta_*` window wrapper now skips each input's leading NaN/NULL run, starts the
kernel at the latest first valid row across the inputs, and returns the skipped rows as NaN,
as polars_talib 0.1.5 does. This applies to both the SQL `ta_*(…) OVER (…)` door and
the Python `ta.*` door. An interior NaN still propagates exactly as in C TA-Lib, and the
kernel layer (`repark_ta::ema` and the other kernels) is unchanged. With `null_lookback=True`,
the facade still NULLs exactly the first `lookback` rows by row position; a skipped run in
front of those rows stays NaN. Ledger:
[ta-chain-1-ledger.md](../../ledgers/staging/ta-chain-1-ledger.md).
