import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("pins", sys.argv[1])
pins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pins)
root = Path(sys.argv[2])
root.mkdir(parents=True, exist_ok=True)
pins._write_null_partition_sets(root)
pins._write_spread_nan_set(root)
print(sorted(p.name for p in root.iterdir()))
