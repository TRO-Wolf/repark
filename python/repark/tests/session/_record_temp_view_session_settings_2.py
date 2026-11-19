from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import _record_temp_view_session_settings_1 as recorder
import _temp_view_session_settings_2_cells as cells


def main() -> None:
    engine, parent, jar, out, what = sys.argv[1:6]
    found: dict[str, dict] = {}
    for mode in ("true", "false", "unset") if what == "cells" else ("true",):
        root = tempfile.mkdtemp(prefix="tvs2-" + mode + "-", dir=parent)
        s = recorder.session(engine, root, mode, jar)
        table = cells.Cells(s, root, mode)
        found[mode] = table.run() if what == "cells" else table.explain()
        s.stop()
    Path(out).write_text(json.dumps(found, sort_keys=True, separators=(",", ":")) + "\n")
    print("WROTE " + out, flush=True)


if __name__ == "__main__":
    main()
