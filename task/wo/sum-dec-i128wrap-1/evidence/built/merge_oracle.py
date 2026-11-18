import json
import sys
from pathlib import Path

on, off, out = (Path(arg) for arg in sys.argv[1:4])
runs = {json.loads(path.read_text())["ansi"]: json.loads(path.read_text()) for path in (on, off)}
for mode, run in runs.items():
    assert run["cells"].pop("conf/ansi") == mode, mode
oracle = {"cells": {mode: runs[mode]["cells"] for mode in ("false", "true")}}
oracle["provenance"] = runs["true"]["provenance"]
out.write_text(json.dumps(oracle, sort_keys=True, separators=None) + "\n")
