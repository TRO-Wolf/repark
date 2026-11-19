# Card TEMP-VIEW-FRAME-CONF-RACE-1: while another thread runs `spark.conf.set` on a captured key, a frame view can answer under the planning state's setting

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 10; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the re-check verifier's notes in the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), "The re-check verifier" (its N1, and N2 as a note).

**Status:** filed 2026-11-19, not dispatched.

**Severity: S3, not label-blocking** (owner, 2026-11-19, about 01:28 UTC, verbatim "Go with your recommendations and tonight you don’t even need to wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck"; the rulings record's item 10 names it not blocking).

## Finding

Class (c), S3, from the re-check verifier's N1. While another thread runs `spark.conf.set` on a captured key, a frame view sometimes answers under the planning state's setting instead of its own: 3 to 13% of writes on the head, about the same on `d4423a69`, about 50% on main. The inline decision compares against the live options (`frame_settings.rs:204`); when they match it inlines the frame's own plan, which is then analysed under DataFusion's earlier state snapshot. Every answer is the old or the new setting's, never a third; the `-NULL` refusal is never skipped; no answer is worse than main's.

Note (N2, a path, never observed): `closed_scan(found, &live)` (`frame_settings.rs:207-208`) takes every setting outside the four captured ones from `live`. After an expired wait or a dropped session `live` is the stale last read, and two concurrent reads store last-writer-wins (`extension.rs:43-46`), so the last read can step back.

The write-landing race itself was closed by delta 3 (`9aa2f182`, two of the seven files, cap 7). This card is the residue the verifier named.

**Main:** about 50% of writes answer under the planning state's setting in this race (N1's measure).

Up: [map.md](map.md).
