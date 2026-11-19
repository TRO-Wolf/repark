# map — task/roadmap/mid-term/cards-2026-11-19/

Ten cards filed from the 2026-11-18 window, under the owner's rulings recorded in [rulings-2026-11-18.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md). Six are the view residue of TEMP-VIEW-SESSION-SETTINGS-1 part 2 (five view cards and the race card); four come from the decimal units (the verify-ddl and sum step 0 verdicts). Each card's severity and label status are the ruling's, quoted in the card. None is dispatched.

## Cards

- [TEMP-VIEW-FRAME-READER-PARSE-1](temp-view-frame-reader-parse-1-card-2026-11-19.md) (S1, label-blocking, ruling 5) — **card:** a JSON- or CSV-reader DataFrame view, and a SQL view over one, keeps main's answer when the zone differs.
- [TEMP-VIEW-FRAME-CROSSJOIN-1](temp-view-frame-crossjoin-1-card-2026-11-19.md) (S1, label-blocking, ruling 6) — **card:** `crossJoin` and the exact-key join door analyse both held sides under the live settings.
- [HELD-FRAME-USING-JOIN-1](held-frame-using-join-1-card-2026-11-19.md) (S1, label-blocking, ruling 6) — **card:** a USING join keeps only the left frame's state, so a held right side answers under the live settings.
- [HELD-FRAME-DERIVED-ANSI-1](held-frame-derived-ansi-1-card-2026-11-19.md) (S2, not label-blocking, ruling 6) — **card:** `withColumn` and `select(F.expr(...))` over a held ANSI frame answer unlike Spark.
- [LEFT-ANTI-PROJECTION-PUSHDOWN-1](left-anti-projection-pushdown-1-card-2026-11-19.md) (S3, not label-blocking, on the label path, rulings 6 and 8) — **card:** a left-anti or left-semi join evaluates the left projection on every row; the prerequisite of CROSSJOIN-1's fix.
- [TEMP-VIEW-FRAME-CONF-RACE-1](temp-view-frame-conf-race-1-card-2026-11-19.md) (S3, not label-blocking, ruling 10) — **card:** a frame view can answer under the planning state's setting while another thread runs `spark.conf.set`.
- [AVG-DECIMAL-GROUPED-RUN-VARIANCE-1](avg-decimal-grouped-run-variance-1-card-2026-11-19.md) (S2, not label-blocking, ruling 8; built next on lane B after `sum`) — **card:** a grouped decimal `avg` answers differently from run to run.
- [DECIMAL-COMPARE-WIDEN-OVERFLOW-1](decimal-compare-widen-overflow-1-card-2026-11-19.md) (S2, not label-blocking, ruling 7) — **card:** SQL compares a `decimal(38,10)` quotient widened to `(38,14)`, which overflows past 24 integer digits.
- [DATAFRAME-DIVIDE-MARKER-SPELLABLE-1](dataframe-divide-marker-spellable-1-card-2026-11-19.md) (S3, not label-blocking, ruling 7) — **card:** a written double cast on both sides of `/` is taken for the DataFrame divide marker.
- [DECIMAL-ADD-SCALE38-OVERFLOW-1](decimal-add-scale38-overflow-1-card-2026-11-19.md) (S3, not label-blocking, ruling 10) — **card:** `x + 1` on a `decimal(38,38)` column raises Arrow's arithmetic overflow where Spark answers.

Up: [../map.md](../map.md).
