# Cost, slice 1 fold 2 (2026-10-13)

Head `b543987e` (release module built at `6f90b403`, the last commit that changed Rust
product code) against main `2ac03089`. Both modules are release builds with
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`; the main module is the second verifier's.
Method: [`../interleaved_cost.py`](../interleaved_cost.py), eight interleaved sets of 15
repeats on cores 32-47 under the build lock, one locked command per side; odd sets run
main then head, even sets head then main. Sets 6 to 8 were added after set 2 put one
`merge_mor` pair at 1.0752; all eight sets are kept.

Gated statements (ratio at or below 1.05):

| Statement | Median pair ratio | Ratio of medians | Pair range |
|---|---|---|---|
| insert | 0.9946 | 0.9917 | 0.9580 – 1.0257 |
| merge | 1.0005 | 0.9974 | 0.9661 – 1.0267 |
| merge_mor | 1.0089 | 1.0065 | 0.9649 – 1.0752 |

Verdict: every median ratio is at or below 1.05. No halt. One `merge_mor` pair of eight
read 1.0752 and another 0.9649; this fold changed no code on the write path (the
`sequence` kernel and the Python output check only).

Ungated statements added by this fold (ruling 6, accepted, no gate, no optimisation):

| Statement | Median pair ratio | Ratio of medians | Pair range | main ms | head ms |
|---|---|---|---|---|---|
| nest8 — `coalesce(…, NULL)` eight deep over `timestamp_ns` | 1.8481 | 1.8498 | 1.8271 – 1.8893 | 27.0 | 49.9 |
| union200 — one nanosecond branch, 200 NULL branches | 1.1258 | 1.1252 | 1.1196 – 1.1296 | 191.5 | 215.9 |

Raw sets: [interleaved-sets-fold2.json](interleaved-sets-fold2.json). Reduced:
[interleaved-cost-fold2.json](interleaved-cost-fold2.json).
