# Maintenance policies — `[<profile>.maintenance]` and `CALL run_maintenance()`

One CALL maintains one table through the ordered cycle from
[iceberg-guide.md](iceberg-guide.md) ("The maintenance runbook"): the policy in
`repark.toml` names the targets, the dry run prints the plan, the apply runs it.
Each step commits on its own; a failing step stops the chain.

## The policy shape

`maintenance` is profile-scoped like every other table in
[repark-toml.md](repark-toml.md). Every key is optional; a profile with no
`maintenance` table means "no policy". A `tables` entry overrides the profile
values key by key for one table. Unknown keys refuse loud with the key path.

```toml
[default.maintenance]
target_file_size_bytes = 536870912
snapshot_retain_last = 5
snapshot_older_than = "7d"
orphan_older_than = "3d"
rewrite_manifests = true
position_delete_ratio = 0.3

[default.maintenance.tables."glue.silver.orders"]
target_file_size_bytes = 268435456
snapshot_retain_last = 20
```

Durations are strings `"<n>d"`, `"<n>h"`, or `"<n>m"`. Anything else refuses
loud naming the key. Cutoffs compute at run time as now minus the duration.

## The step order and the delete gate

| Step | Procedure | Runs when |
|---|---|---|
| 1 | `rewrite_position_delete_files` | delete bytes over data bytes (from the `files` and `delete_files` metadata tables) is at or above `position_delete_ratio` |
| 2 | `rewrite_data_files` | always, with `target-file-size-bytes` from the policy |
| 3 | `rewrite_manifests` | `rewrite_manifests = true` |
| 4 | `expire_snapshots` | always, with `older_than` and `retain_last` |
| 5 | `remove_orphan_files` | `orphan_older_than` is set |

Ordinals keep these numbers when a step is gated out.

## The CALL and its result frame

```sql
CALL <catalog>.system.run_maintenance(table => 'db.t' [, dry_run => true|false] [, <any policy key> => value])
```

`dry_run` defaults to true. Inline keys override the file's per-table entry,
which overrides the profile values. The result frame has one row per step:
`step` (int), `procedure` (string), `arguments` (string, the CALL as issued),
`status` (`planned` on a dry run; `ran`, `failed`, or `skipped` on apply),
`result` (string, the step's own result frame as JSON, or the error text).

Worked dry run against a five-file, five-snapshot memory-catalog table (step 1
gated out: no delete files against the 0.3 threshold; cutoffs compute at run
time, so the `older_than` values move):

| step | procedure | arguments | status |
|---|---|---|---|
| 2 | rewrite_data_files | CALL mem.system.rewrite_data_files(table => 'ns.orders', options => map('target-file-size-bytes', '67108864')) | planned |
| 3 | rewrite_manifests | CALL mem.system.rewrite_manifests(table => 'ns.orders') | planned |
| 4 | expire_snapshots | CALL mem.system.expire_snapshots(table => 'ns.orders', older_than => 1789040131633, retain_last => 2) | planned |
| 5 | remove_orphan_files | CALL mem.system.remove_orphan_files(table => 'ns.orders', older_than => 1788780931633) | planned |

The matching apply answers `ran` with each step's own result frame as JSON:

| step | procedure | status | result |
|---|---|---|---|
| 2 | rewrite_data_files | ran | [{"rewritten_data_files_count":5,"added_data_files_count":1,"rewritten_bytes_count":4347,"failed_data_files_count":0,"removed_delete_files_count":0}] |
| 4 | expire_snapshots | ran | [{"deleted_data_files_count":5,"deleted_position_delete_files_count":0,"deleted_equality_delete_files_count":0,"deleted_manifest_files_count":5,"deleted_manifest_lists_count":5,"deleted_statistics_files_count":0}] |

Afterwards the table reads back unchanged: 5 files compacted to 1, snapshots
expired to `retain_last`, ids still `[1, 2, 3, 4, 5]`.

## The Python wrapper

```python
frame = session.run_maintenance(table, dry_run=True, **overrides)
```

A thin call to the SQL form above: `table` resolves under the session catalog
and namespace like `session.table`, `dry_run` defaults to true, and every
override becomes a CALL argument (`snapshot_retain_last=2` renders
`retain_last => 2`). It returns the same result frame.

## No policy, no inline keys

A CALL with neither refuses loud:

```text
run_maintenance: no [default.maintenance] table and no inline keys for ns.t
```

## Planning a partition spec — `CALL plan_partitioning()`

`run_maintenance` keeps today's layout; `plan_partitioning` proposes tomorrow's.
It reads the `files` metadata table (per-file sizes and paths plus the
`readable_metrics` lower/upper bounds) and the `refs` table, never the data,
and returns one row per candidate spec, best first:

```sql
CALL <catalog>.system.plan_partitioning(table => 'db.t', target_file_size_bytes => 524288)
```

Both arguments are required and the target must be a positive integer.

| Column | Contents |
|---|---|
| `candidate` | the spec in Spark DDL spelling — `days(ts)`, `bucket(16, id)`, `identity(region)`, or `unpartitioned` |
| `score` | the target-band penalty, lower is better; 0 means every projected partition value lands between 0.25× and 4× the target |
| `projected_partitions` | partition values carrying bytes under the candidate |
| `projected_files_at_target` | projected files per value — `ceil(post-rewrite bytes / target)` — summed |
| `ddl` | the `ALTER TABLE … ADD PARTITION FIELD` statements the candidate would take |
| `calls` | the maintenance CALL chain that would follow (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`) |
| `plan_id` | a hash of the snapshot id and the candidate; an apply step requires this dry-run id |
| `notes` | per-candidate spread assumptions plus the table-wide caveats below |

`projected_files_at_target` projects post-rewrite bytes: the procedure reads
every live data file's parquet footer through the table's own `FileIO` (a
metadata read only — no row-group data is decoded), sums each column chunk's
`total_compressed_size` and `total_uncompressed_size`, and projects each
partition value's share of the uncompressed sum scaled by that `byte_ratio`
before dividing by the target — compression is counted once, at the size a
rewrite of the same rows would store. Every row's `notes` reports the ratio as
`byte_ratio=<value> (footers)`. When any footer is unreadable the ratio falls
back to the measured constant 0.55, each file's uncompressed size is estimated
as `file_size_in_bytes / 0.55`, and the notes say `(fallback)`. The AP-0-R-001
caveat stays on every row, and `projected_partitions` is the column the AP-0
rewrite measured exact.

Known issue (S2-24): on zstd tables `rewrite_data_files` currently writes about
1.5× its input's compressed bytes under the same codec (the RP-17 re-measure
rewrote 206 files at 2 831 692 B into 20 at 4 474 081 B), so a compaction is a
net-size *loss* there and the projection under-reads the live actual until fork
card F-REWRITE-SIZE-1 lands.

Candidate generation follows P-2: timestamp and date columns get
`years`/`months`/`days`/`hours`; int and string columns get `identity` when the
union of their bound endpoints holds at most 1 000 values, else `bucket(N)` for
N ∈ {8, 16, 32, 64, 128}; plus `unpartitioned` and the two-field specs crossing
the best single candidate of the top three columns. A column with no readable
bounds produces no candidate — the last row's `notes` names it and why. Tables
with a branch besides `main` refuse; a table carrying several partition specs
plans normally with a note that applying would rewrite to one spec.

The procedure plans only — it writes nothing. Applying a printed plan is
`apply_partitioning` below, keyed by `plan_id`.

## Applying a printed plan — `CALL apply_partitioning()`

```sql
CALL <catalog>.system.apply_partitioning(
  table => 'db.t', plan_id => '<id from the plan frame>'
  [, dry_run => true] [, target_file_size_bytes => 524288])
```

`table` and `plan_id` are required. `dry_run` defaults **true**: nothing commits;
the frame lists the steps it would run, each `status` `dry_run`. `dry_run => false`
executes. `target_file_size_bytes` is optional, a positive integer spelled as in
`plan_partitioning` — pass the same value used for planning so a two-field
candidate is found; omit it and lookup re-plans at target 1, which can miss a pair.

The `plan_id` is re-derived at the current snapshot. Nothing is stored. A moved
snapshot, or an id not from this table, refuses; re-run `plan_partitioning` and
pass a fresh id. If you omitted the target, the refusal also names
`target_file_size_bytes`.

Steps, one commit each: every `ALTER TABLE … ADD PARTITION FIELD …` from the
row's `ddl` (none for `unpartitioned`), then `rewrite_data_files`,
`rewrite_manifests`, `expire_snapshots`. Frame: `step` Int32 plus `procedure`,
`arguments`, `status` (`applied` / `dry_run` / `skipped`), `result`, `plan_id`
Utf8. **Not a transaction** — a failure stops the chain and leaves earlier commits.
P-5: a branch other than `main` refuses; sort order is preserved; a multi-spec
table is rewritten onto one current spec.

```sql
CALL ice.system.plan_partitioning(table => 'sales.orders', target_file_size_bytes => 524288);
CALL ice.system.apply_partitioning(table => 'sales.orders', plan_id => '<id>',
  target_file_size_bytes => 524288, dry_run => false);
```

## Reserved: `adaptive_partitioning`

The `adaptive_partitioning` key is reserved for a later unit and refuses with
"not yet supported" wherever it appears: the file policy, a per-table entry,
or an inline argument.
