# Superseded lessons

Entries moved out of [task/lessons.md](../../task/lessons.md) once a later owner ruling replaced
them. Each keeps its text; its first line names what replaced it.

## 2026-10-22 — two Opus 5.5 lanes at a time; re-evaluate by 2026-11-22
*Superseded by the 2026-11-02 one-lane rule and the 2026-11-14 two-agent cap in task/lessons.md; the 2026-11-22 review date did not carry over (owner, 2026-11-18).*

- **DO run at most two Opus 5.5 builds concurrently, not one.** *Why:* the one-lane rule of
  2026-09-26 came from the September usage spikes (eight Opus executors live at once), and the
  10-20 plan kept one lane only because the 10-17 halt was lifted for one; the closeout dates then
  slipped a day for every day that lane ran long. The owner relaxed it on 2026-10-22 ("allow for
  2 lanes at a time for sure"). Both lanes keep the standing limits: `claude-opus-5-5` only, one
  Rust suite per clone, inside the 26-core cap, timed measurements on a quiet box only, verifiers
  and read-only evaluations count toward the two unless the owner says otherwise. Lane 2 opened
  the same day with ICE-INTEGRITY-PINS-1 pins U and X beside the ZONE-SEAM-1 fix round.
- **DO re-evaluate the lane count by 2026-11-22.** *Why:* the owner asked for a re-evaluation
  within a month. Inputs: usage against the owner's ceiling, box contention (cost-gate failures or
  load-induced test timeouts, as the seam verifier saw on 10-22), and whether the v2 and v3 label
  dates moved. The CLAUDE.md capability-tier section is unchanged: Opus still only on the owner's
  explicit word, which this ruling is.

## 2026-08-06 — carried from v1 (one bullet)
*Superseded by [docs/maps-policy.md](../maps-policy.md) (the 2026-10-29 map policy) and the 2026-11-18 ceilings ruling in task/lessons.md.*

- **DO update every touched directory's `map.md` in the same change as the code — lockstep, no
  exceptions.** A change is not done until the maps reflect it; `scripts/check_map_md.sh` is the
  pre-commit oracle. New directory → new `map.md` in the same change.
