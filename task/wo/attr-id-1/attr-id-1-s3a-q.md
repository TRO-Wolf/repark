# Rulings on S3a's halt (same session; lane /tmp/xattr, HEAD 9f372d94 plus your uncommitted cutover)

## Q1: the timing bar, ruled like for like
The 597 s bar exists to catch **overhead** from the id mechanism, not the cost of cells that now answer instead of failing. Re-state the bar as two measures:
- **(a) like-for-like:** sum the replay time over the cells whose outcome **class** is unchanged against `main.json` (EQUAL→EQUAL, and the same error class to the same error class). Compare that sum with the same cells' time on main, and require **at most 1.2x**. Use per-cell timings; if the driver lacks them, add per-cell wall time to the replay output without changing any answer.
- **(b) gains:** report the total time of the FIXED cells separately. It carries no bar.

Measure both as the median of 3. If (a) is above 1.2x, halt again with the per-shape breakdown.

## Q2: confirmed
R5 (the sensitive qualifier-case inversion, with the DataFusion-normalized held qualifier) and R9/R12-qualified (the facade-held join qualifiers) belong to S3e, per §9 Q4. Record them as S3e-owned residues in the ledger.

## Then
**Commit the cutover now; do this first.** Your working tree holds the uncommitted S3a changes in `attr_id.rs`, `case_bind.rs`, `map.md` and others. Then run the like-for-like measure and `bash /tmp/xattr/gate.sh`, and update `/tmp/xattr/handback.json` with:
- status;
- the replay counts (5,252 diffs, 0 moved away, 3,925 FIXED, 1,208 named residues, as you measured);
- timings (a) and (b);
- the list of deleted helpers.

Use the S3a commit message from the brief.
