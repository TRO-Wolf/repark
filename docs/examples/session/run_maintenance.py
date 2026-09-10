"""Plan table maintenance through session.run_maintenance on a memory catalog.

pins: maint-policy-1/C-021
"""

from __future__ import annotations

import tempfile

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.run_maintenance",
]


def main() -> None:
    """Dry-run the maintenance plan with inline policy keys and read it back."""
    with tempfile.TemporaryDirectory() as warehouse:
        repark = ReparkSession.builder.appName("ex-ses-run-maintenance").getOrCreate()
        try:
            repark.register_memory_catalog("ex_maint_cat", warehouse)
            repark.sql("CREATE NAMESPACE ex_maint_cat.ex_maint_db")
            repark.sql(
                "CREATE TABLE ex_maint_cat.ex_maint_db.orders USING iceberg "
                "AS SELECT 1 AS id, 'a' AS name"
            )
            frame = repark.run_maintenance(
                "ex_maint_cat.ex_maint_db.orders",
                target_file_size_bytes=67108864,
                snapshot_retain_last=5,
            ).to_arrow()
            statuses = set(frame.column("status").to_pylist())
            if statuses != {"planned"}:
                raise SystemExit(f"dry run statuses {statuses!r} != {{'planned'}}")
            procedures = frame.column("procedure").to_pylist()
            if procedures != ["rewrite_data_files", "expire_snapshots"]:
                raise SystemExit(f"planned procedures {procedures!r} unexpected")
        finally:
            repark.stop()


if __name__ == "__main__":
    main()
