from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Any

HERE = Path(__file__).resolve().parent
EVIDENCE = HERE.parent
LIST_KINDS = {"L_I64", "LL_I64", "FSL2_I64", "L_UTF8", "L_STRUCT", "NEST_L"}
FIXED = {"CROSS-PRODUCT", "WRONG-ANSWER", "WRONG-WRITE"}
PLAN_KEYS = ("ecj_before", "ecj_after", "physical_join")
ALIAS = re.compile(r"_repark_j[lr]_[0-9a-f]+|__repark_[a-z_]+_[0-9a-f]+|#\d+|@\d+")


def probe() -> ModuleType:
    path = EVIDENCE / "item1" / "probe_item1.py"
    spec = importlib.util.spec_from_file_location("probe_item1", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def twin_rows(step0: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    twin = step0[f"join|{record['kind']}|df_str|{record['jt']}|base"]["outcome"]
    return {"n": twin["n"], "sha": twin["sha"]}


def spark_step0(path: Path) -> dict[str, Any]:
    rows = (json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
    return {row["cell_id"]: row for row in rows if "cell_id" in row}


def masked(lines: list[str]) -> list[str]:
    return [ALIAS.sub("<id>", line) for line in lines]


def unmasked(record: dict[str, Any]) -> str:
    return ALIAS.sub("<id>", json.dumps(record["outcome"], sort_keys=True))


def predicted(cell_id: str, before: str, after: str, record: dict[str, Any]) -> bool:
    section = cell_id.split("|")[0]
    if before == after:
        return True
    if before in FIXED and after == "EQUAL":
        return True
    return section == "mixed" and record["form"] == "join_str" and after == "EQUAL"


def compare() -> dict[str, Any]:
    module = probe()
    spark, _ = module.load(EVIDENCE / "item1" / "raw" / "spark")
    base, _ = module.load(EVIDENCE / "item1" / "raw" / "main")
    head, head_meta = module.load(HERE / "raw" / "head")
    step0 = spark_step0(EVIDENCE / "step0" / "raw" / "spark.jsonl")
    moves: Counter[tuple[str, str, str]] = Counter()
    unpredicted, list_moved, lateral = [], [], []
    for cell_id, record in head.items():
        if "outcome" not in record:
            continue
        before = module.label_of(base[cell_id], spark.get(cell_id))
        after = module.label_of(record, spark.get(cell_id))
        moves[(cell_id.split("|")[0], before, after)] += 1
        if not predicted(cell_id, before, after, record):
            unpredicted.append((cell_id, before, after))
        kind = record.get("kind", "")
        if kind in LIST_KINDS and unmasked(record) != unmasked(base[cell_id]):
            list_moved.append(cell_id)
        if cell_id.startswith("lateral|"):
            holds = {k: record["outcome"].get(k) for k in ("n", "sha")} == twin_rows(step0, record)
            lateral.append((cell_id, holds))
    plans = plan_checks(base, head)
    return {
        "meta": head_meta,
        "moves": sorted(moves.items()),
        "unpredicted": unpredicted,
        "list_moved": list_moved,
        "lateral": lateral,
        "plans": plans,
    }


def plan_checks(base: dict[str, Any], head: dict[str, Any]) -> dict[str, Any]:
    same_controls, cast_hash = [], []
    for cell_id, record in head.items():
        if not cell_id.startswith("plan|") or "plan" not in record:
            continue
        kind = record["kind"]
        if kind in LIST_KINDS:
            same = all(
                masked(record["plan"].get(key, [])) == masked(base[cell_id]["plan"].get(key, []))
                for key in PLAN_KEYS
            )
            same_controls.append((cell_id, same))
        else:
            physical = " ".join(record["plan"].get("physical_join", []))
            cast_hash.append((cell_id, "HashJoinExec" in physical and "CAST(" in physical))
    return {"controls_identical": same_controls, "cast_hash_join": cast_hash}


def main() -> None:
    result = compare()
    sys.stdout.write(json.dumps(result, indent=1) + "\n")


if __name__ == "__main__":
    main()
