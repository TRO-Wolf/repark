from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import _ns_array_apply_in_pandas_refuse_1_cases as cases
import numpy as np
import pandas as pd
import pyarrow as pa

import repark


def record_cells() -> dict[str, Any]:
    """Measure every cell on the installed build, one fresh session and warehouse each."""
    cells: dict[str, Any] = {}
    for key in cases.cell_keys():
        with tempfile.TemporaryDirectory(prefix="ns-array-refuse-") as warehouse:
            cells[key], _error = cases.answer_in_fresh_session(Path(warehouse), key)
    return cells


def main() -> None:
    """Record what a build of main answers in every cell into the fixture or ``--output``."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("commit")
    parser.add_argument("--output", type=Path, default=cases.FIXTURE)
    arguments = parser.parse_args()
    versions = {"numpy": np.__version__, "pandas": pd.__version__, "pyarrow": pa.__version__}
    cells = record_cells()
    lines = [
        f"  {json.dumps(key)}: {json.dumps(cell, sort_keys=True, separators=(',', ':'))}"
        for key, cell in sorted(cells.items())
    ]
    header = [
        "{",
        f' "recorded_at_commit": {json.dumps(arguments.commit)},',
        f' "versions": {json.dumps(versions, sort_keys=True)},',
        ' "cells": {',
    ]
    arguments.output.write_text("\n".join([*header, ",\n".join(lines), " }", "}"]) + "\n")
    print(repark.__file__)
    print(len(cells), "cells")


if __name__ == "__main__":
    main()
