from __future__ import annotations

import importlib
import json
import sys
import tempfile
from pathlib import Path


def main() -> None:
    tests, engine, parent, jar, out = sys.argv[1:6]
    sys.path.insert(0, tests)
    rec = importlib.import_module("_record_temp_view_session_settings_1")
    cells = importlib.import_module("_temp_view_session_settings_1_cells")
    sweep = importlib.import_module("_temp_view_session_settings_1_sweep")
    found = {}
    for mode in ("true", "false"):
        root = tempfile.mkdtemp(prefix="sw-" + mode + "-", dir=parent)
        session = rec.session(engine, root, mode, jar)
        table = cells.Cells(session, root, mode)
        table.fixtures()
        table.baseline()
        sweep.Sweep(table).run()
        found[mode] = table.found
        session.stop()
    Path(out).write_text(json.dumps(found, sort_keys=True, indent=0) + "\n")
    print("WROTE", out, len(found["true"]), flush=True)


if __name__ == "__main__":
    main()
