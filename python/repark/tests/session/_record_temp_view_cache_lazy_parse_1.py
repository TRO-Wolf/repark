from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import _record_temp_view_session_settings_1 as recorder
import _temp_view_cache_lazy_parse_1_cells as cells


def main() -> None:
    engine, parent, jar, out = sys.argv[1:5]
    root = tempfile.mkdtemp(prefix="cl1-", dir=parent)
    s = recorder.session(engine, root, "true", jar)
    found = cells.Cells(s, root).run()
    s.stop()
    Path(out).write_text(json.dumps(found, sort_keys=True, indent=1) + "\n")
    print("WROTE " + out, flush=True)


if __name__ == "__main__":
    main()
