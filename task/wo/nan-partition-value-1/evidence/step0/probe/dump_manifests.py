import json
import math
import struct
import sys
from pathlib import Path

import fastavro

root = Path(sys.argv[1])
for table in sorted(p for p in root.glob("*") if (p / "metadata").is_dir()):
    for mf in sorted((table / "metadata").glob("*.avro")):
        if mf.name.startswith("snap-"):
            continue
        with mf.open("rb") as fh:
            reader = fastavro.reader(fh)
            meta = {
                k: v.decode() if isinstance(v, bytes) else v
                for k, v in reader.metadata.items()
                if k in ("partition-spec", "format-version", "content")
            }
            for entry in reader:
                part = entry["data_file"]["partition"]
                shown = {}
                for k, v in part.items():
                    if isinstance(v, float):
                        bits = struct.pack(">d", v).hex()
                        shown[k] = {"value": "NaN" if math.isnan(v) else v, "bits": bits}
                    else:
                        shown[k] = v
                lb = entry["data_file"].get("lower_bounds")
                print(
                    json.dumps(
                        {
                            "table": table.name,
                            "manifest": mf.name,
                            "status": entry["status"],
                            "partition": shown,
                            "record_count": entry["data_file"]["record_count"],
                            "nan_value_counts": entry["data_file"].get("nan_value_counts"),
                            "spec": meta.get("partition-spec"),
                        },
                        default=str,
                    )
                )
