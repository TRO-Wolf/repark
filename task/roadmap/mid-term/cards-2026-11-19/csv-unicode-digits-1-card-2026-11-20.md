# Card CSV-UNICODE-DIGITS-1: a CSV read over non-ASCII digits answers NULL, or raises, where Spark parses the digits (a declared divergence)

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20 (the owner's delegation, about 00:22 UTC). **Source:** the Fable reply of 2026-11-20 in the agents' lessons folder (file `Fable reply — CSV-STRING-FIELD-TEXT-1 step 0 2026-11-20.md`), §10 item 5, kept outside the repository.

**Status:** filed 2026-11-20; not dispatched.

**Severity: S2, a declared divergence.** A silent NULL for `DOUBLE`. **Label-blocking: not stated.** Fable's guidance names it a declared divergence and gives no label verdict, so whether the label excludes it is open for the owner. This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M, by Fable]** Spark answers `123` for `INT` and `123.00` for `DECIMAL` over Arabic-Indic or fullwidth digits, and NULL for `DOUBLE`. Main raises on these today, even for a `STRING` field.

**Decision.** The kernel stays ASCII. Those cells are pinned as a declared divergence and carded S2. The answers depend on the JDK's Unicode version, so the table does too.

## First step

None beyond the pin.

Up: [map.md](map.md).
