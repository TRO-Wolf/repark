# Card SPILL-SMALL-POOL-STALL-1: a grouped `sum` under a 3 MB memory pool sits near 3% CPU for over 100 s

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the step 0 critic verdict of AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 (Opus 5.5 medium, 2026-11-19, PR #1365), its "Step 5" side note after the shapes table; kept outside the repository.

**Status:** filed 2026-11-19, not dispatched. Not reproduced yet.

**Rating:** not rated; rating owed; measured once by the AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 critic.

## Finding

A `big` grouped `sum` with a 3 MB memory pool at `target_partitions` 4 sat at about 3% CPU for over 100 s. The critic killed its own process. The verdict's words: "This may be a hang under a tiny pool. Worth one look." The stall is not reproduced in that record. The verdict's Step 3 item 4 ran `big` grouped at `target_partitions` 4 with a 3 MB pool and recorded answers (`out/rp_spill.jsonl`).

**Main:** the side note does not name the package. The verdict's other main runs used `pkg-main-4c365814`.

Up: [map.md](map.md).
