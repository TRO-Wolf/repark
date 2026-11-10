# Card FACADE-INGEST-ARROW-NORMALIZE-1: narrow the Arrow list kinds at createDataFrame to the plain types Spark has

**Date:** 2026-11-09. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Unit C park memo of 2026-11-09 (Q3, the normalise option) and the owner's ruling of 2026-11-09 ("defaults"), which adopted the card unscheduled.

**Status:** filed 2026-11-09, unscheduled. It needs a sketch and a measurement before any ruling on scheduling.

**What it would do:** at `createDataFrame`, narrow incoming DataFrame Arrow types (`LargeList`, `FixedSizeList`, the list views, `RunEndEncoded`) to the plain types Spark has. Spark has one array type. Today the facade passes five list kinds and run-end encoding straight through. A narrowing at the door would remove those variants for every rule behind the Spark door at once.

**Cost:** offset narrowing, and a batch over 2^31 elements cannot narrow. Both are costs of the option and are measured in the sketch, not assumed here.

**Why it does not replace the rule-level fix:** the native door has `arrow_cast`, so a frame built there reaches a rule with its own types. Each rule still needs its type-variant inventory (the 2026-11-09 rule in `task/lessons.md`).

**Close:** a sketch that measures the offset-narrowing cost and the 2^31 limit, then an owner ruling on whether the door narrows.
