from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

_SCRATCH_PATH_RE = re.compile(r"(?:file://)?/tmp/[^\s'\"]+/fixtures")


def _norm(val: Any) -> Any:
    if not isinstance(val, dict):
        return val
    copy = dict(val)
    copy.pop("scan", None)
    return copy


def _equal(spark_val: Any, repark_val: Any) -> bool:
    if spark_val is None or repark_val is None:
        return False
    if spark_val.get("status") != repark_val.get("status"):
        return False
    if spark_val["status"] == "err":
        return True
    return _norm(spark_val) == _norm(repark_val)


def _cell_summary(val: Any) -> str:
    if val is None:
        return "MISSING"
    if val.get("status") == "err":
        cond = val.get("condition")
        err_type = val.get("type", "Error")
        raw_msg = _SCRATCH_PATH_RE.sub("<fixtures>", str(val.get("message", "")))
        msg = raw_msg.replace("|", "/").replace("\n", " ")[:75]
        if cond:
            return f"ERR `{cond}` ({err_type})"
        return f"ERR `{err_type}`: {msg}"
    parts: list[str] = []
    if "schema" in val:
        parts.append(f"`{val['schema']}`")
    if "count" in val:
        parts.append(f"count={val['count']}")
    if "rows" in val:
        encoded = json.dumps(val["rows"], separators=(",", ":"))[:95]
        parts.append(f"rows={len(val['rows'])} `{encoded}`")
    if "where_2" in val:
        grp = json.dumps(val["group"], separators=(",", ":"))
        parts.append(f"where2={len(val['where_2'])} grp=`{grp}`")
    if "files" in val:
        parts.append(f"files={len(val['files'])}")
    if "scan" in val:
        scan = val["scan"]
        if "distinct_read_files" in scan:
            parts.append(f"scanned={len(scan['distinct_read_files'])}")
        elif "scanned_parts_count" in scan:
            parts.append(f"scanned={scan['scanned_parts_count']}")
    return " ".join(parts).replace("|", "/")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", required=True)
    parser.add_argument("--main", required=True)
    parser.add_argument("--head", default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    sp = json.loads(Path(args.spark).read_text(encoding="utf-8"))
    mn = json.loads(Path(args.main).read_text(encoding="utf-8"))
    hd = json.loads(Path(args.head).read_text(encoding="utf-8")) if args.head else None

    lines: list[str] = [
        "# PARQUET-PARTITION-DISCOVERY-1 — Domain Table",
        "",
        "- **Oracle:** live Spark 4.1.2 + Iceberg 1.11.0 (`local[2]`, Java 17).",
        "- **Main:** `514281bd4` (tree-equal to prebuilt `pkg-d1merged-ea94eedc`).",
        f"- **Total measured cells:** {len(sp)}.",
        "",
        "## Parity summary by reader / control",
        "",
    ]

    groups: dict[str, list[str]] = {}
    for cid in sorted(sp.keys()):
        parts = cid.split("/")
        grp = parts[0] if parts[0] == "ICEBERG" else parts[1]
        groups.setdefault(grp, []).append(cid)

    if hd is None:
        lines.append("| Reader / control | Total | Main == Spark | Main != Spark |")
        lines.append("|---|---:|---:|---:|")
        for grp in sorted(groups.keys()):
            cids = groups[grp]
            eq_m = sum(1 for cid in cids if _equal(sp[cid], mn.get(cid)))
            lines.append(f"| `{grp}` | {len(cids)} | {eq_m} | {len(cids) - eq_m} |")
    else:
        lines.append(
            "| Reader / control | Total | Main == Spark | Head == Spark | Fixed | Regressions |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|")
        for grp in sorted(groups.keys()):
            cids = groups[grp]
            eq_m = sum(1 for cid in cids if _equal(sp[cid], mn.get(cid)))
            eq_h = sum(1 for cid in cids if _equal(sp[cid], hd.get(cid)))
            fixed = sum(
                1
                for cid in cids
                if not _equal(sp[cid], mn.get(cid)) and _equal(sp[cid], hd.get(cid))
            )
            reg = sum(
                1
                for cid in cids
                if _equal(sp[cid], mn.get(cid)) and not _equal(sp[cid], hd.get(cid))
            )
            lines.append(f"| `{grp}` | {len(cids)} | {eq_m} | {eq_h} | {fixed} | {reg} |")

    lines.extend(["", "## Full domain table", ""])
    if hd is None:
        lines.append("| Cell | Spark 4.1.2 | RePark main | Eq? |")
        lines.append("|---|---|---|---|")
        for cid in sorted(sp.keys()):
            eq_m = "YES" if _equal(sp[cid], mn.get(cid)) else "NO"
            lines.append(
                f"| `{cid}` | {_cell_summary(sp[cid])} | {_cell_summary(mn.get(cid))} | {eq_m} |"
            )
    else:
        lines.append("| Cell | Spark 4.1.2 | RePark main | RePark head | Status |")
        lines.append("|---|---|---|---|---|")
        for cid in sorted(sp.keys()):
            eq_m = _equal(sp[cid], mn.get(cid))
            eq_h = _equal(sp[cid], hd.get(cid))
            if eq_m and eq_h:
                status = "KEEP"
            elif not eq_m and eq_h:
                status = "FIXED"
            elif eq_m and not eq_h:
                status = "REGR"
            else:
                status = "DIFF"
            lines.append(
                f"| `{cid}` | {_cell_summary(sp[cid])} | {_cell_summary(mn.get(cid))} "
                f"| {_cell_summary(hd.get(cid))} | {status} |"
            )

    Path(args.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"RENDERED {args.out} lines={len(lines)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
