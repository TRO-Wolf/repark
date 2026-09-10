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

## Reserved: `adaptive_partitioning`

The `adaptive_partitioning` key is reserved for a later unit and refuses with
"not yet supported" wherever it appears: the file policy, a per-table entry,
or an inline argument.
