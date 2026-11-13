from __future__ import annotations

import struct
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import common as cm
import write_sources as ws

FIELD_NAMES = {1: "id", 2: "dc", 3: "ds", 4: "dp", 5: "dq", 6: "fc", 7: "fs", 8: "fp", 9: "fq"}
PART_FIELDS = {1: "id", 2: "pk", 3: "v"}


def decode(raw: Any, name: str) -> Any:
    if raw is None:
        return None
    data = bytes(raw)
    if name == "id":
        return struct.unpack("<q", data)[0]
    if len(data) == 8:
        word = struct.unpack("<Q", data)[0]
        return f"{cm.token(struct.unpack('<d', data)[0])}/{word:016x}"
    if len(data) == 4:
        word = struct.unpack("<I", data)[0]
        return f"{cm.token(struct.unpack('<f', data)[0])}/{word:08x}"
    return data.hex()


def iceberg_files(session: Any, table: str) -> list[dict[str, Any]]:
    names = PART_FIELDS if table.endswith("_part") else FIELD_NAMES
    rows = session.sql(
        f"SELECT file_path, record_count, value_counts, null_value_counts, nan_value_counts, "
        f"lower_bounds, upper_bounds FROM {table}.files"
    ).collect()
    out = []
    for row in rows:
        entry: dict[str, Any] = {"file": Path(row.file_path).name, "records": row.record_count}
        for key in ("value_counts", "null_value_counts", "nan_value_counts"):
            entry[key] = {names.get(k, k): v for k, v in sorted((getattr(row, key) or {}).items())}
        for key in ("lower_bounds", "upper_bounds"):
            source = getattr(row, key) or {}
            entry[key] = {
                names.get(k, k): decode(v, names.get(k, "")) for k, v in sorted(source.items())
            }
        out.append(entry)
    return out


def summaries(session: Any, table: str) -> list[Any]:
    rows = session.sql(f"SELECT partition_summaries FROM {table}.manifests").collect()
    out = []
    for row in rows:
        for summary in row.partition_summaries or []:
            out.append(
                {
                    "contains_null": summary.contains_null,
                    "contains_nan": summary.contains_nan,
                    "lower": summary.lower_bound,
                    "upper": summary.upper_bound,
                }
            )
    return out


def parquet_footers(root: Path) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    out = []
    for path in sorted(root.rglob("*.parquet")):
        meta = pq.ParquetFile(path).metadata
        group = meta.row_group(0)
        columns = {}
        for index in range(group.num_columns):
            column = group.column(index)
            stats = column.statistics
            if stats is None:
                columns[column.path_in_schema] = None
                continue
            columns[column.path_in_schema] = {
                "has_min_max": stats.has_min_max,
                "min": cm.token(stats.min) if stats.has_min_max else None,
                "max": cm.token(stats.max) if stats.has_min_max else None,
                "nulls": stats.null_count,
            }
        out.append(
            {
                "file": str(path.relative_to(root.parent)),
                "created_by": meta.created_by,
                "rows": meta.num_rows,
                "columns": columns,
            }
        )
    return out


def main() -> int:
    warehouse, root, out = (Path(a) for a in sys.argv[1:4])
    session = ws.session_for("spark", warehouse)
    tables = [
        f"{cm.CAT}.{cm.NS}.{w}_{f}_{k}"
        for w in ("rb", "sp")
        for f in ws.FORMATS
        for k in ("l1", "l5", "part")
    ]
    with out.open("w") as sink:
        for table in tables:
            record = cm.attempt(lambda t=table: {"files": iceberg_files(session, t)})
            if table.endswith("_part"):
                record["summaries"] = cm.attempt(lambda t=table: {"s": summaries(session, t)})
            cm.dump(sink, {"record": "iceberg", "table": table, **record})
        for entry in parquet_footers(root):
            cm.dump(sink, {"record": "footer", **entry})
        for entry in parquet_footers(warehouse):
            cm.dump(sink, {"record": "footer", **entry})
    session.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
