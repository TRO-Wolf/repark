# task/wo/update-schema-batch-mismatch-1/evidence/step0/probe/ — step 0's probe

- `print_batches.py` — `print_batches.py --repeats N --out <raw.jsonl>`: the first probe's cells on both doors plus a NOT NULL control, one fresh two-file table per attempt; sets `REPARK_STEP0_BATCH_LOG` so the hooked build appends its events to the same file.
- `render_step0.py` — `render_step0.py <raw.jsonl> --out <table.md>`: per cell, the outcomes, the path taken, the batch order and the refusals predicted from the printed nullability.
- `print-hook.patch` — the environment-gated print hook, applied to main's working tree for the run and never committed; `git apply` it to reproduce.

Up: [../map.md](../map.md).
