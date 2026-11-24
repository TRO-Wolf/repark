from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        if any(
            k in obj for k in ("dtype", "err", "count", "vals", "arrow_type", "hour", "fmt", "name")
        ):
            out[prefix] = obj
            return out
        for k, v in sorted(obj.items()):
            sub = f"{prefix}|{k}" if prefix else k
            out.update(flatten(v, sub))
    else:
        out[prefix] = obj
    return out


def summarize(spark_path: Path, main_path: Path, head_path: Path | None = None) -> None:
    spark = json.loads(spark_path.read_text(encoding="utf-8"))
    main = json.loads(main_path.read_text(encoding="utf-8"))
    head = json.loads(head_path.read_text(encoding="utf-8")) if head_path else None

    for section in (
        "card_cells",
        "col_cells",
        "unaliased_cells",
        "zone_change_cells",
        "mixing_cells",
        "neighbour_cells",
        "write_cells",
        "runtime_conf_cells",
    ):
        s_flat = flatten(spark[section])
        m_flat = flatten(main[section])
        h_flat = flatten(head[section]) if head else None
        keys = sorted(set(s_flat) | set(m_flat) | (set(h_flat) if h_flat else set()))
        eq_main = sum(1 for k in keys if s_flat.get(k) == m_flat.get(k))
        if h_flat is not None:
            eq_head = sum(1 for k in keys if s_flat.get(k) == h_flat.get(k))
            reg = [
                k for k in keys if s_flat.get(k) == m_flat.get(k) and s_flat.get(k) != h_flat.get(k)
            ]
            fixed = [
                k for k in keys if s_flat.get(k) != m_flat.get(k) and s_flat.get(k) == h_flat.get(k)
            ]
            moved_other = [
                k
                for k in keys
                if m_flat.get(k) != h_flat.get(k)
                and s_flat.get(k) != m_flat.get(k)
                and s_flat.get(k) != h_flat.get(k)
            ]
            print(
                f"=== {section}: total={len(keys)} main_eq={eq_main} head_eq={eq_head} "
                f"fixed={len(fixed)} regressed={len(reg)} moved_other={len(moved_other)} ==="
            )
            for k in reg:
                s_val, m_val, h_val = s_flat.get(k), m_flat.get(k), h_flat.get(k)
                print(f"  REGRESSED {k}: spark={s_val} main={m_val} head={h_val}")
            for k in moved_other:
                s_val, m_val, h_val = s_flat.get(k), m_flat.get(k), h_flat.get(k)
                print(f"  MOVED_OTHER {k}: spark={s_val} main={m_val} head={h_val}")
            rem = [k for k in keys if s_flat.get(k) != h_flat.get(k)]
            if rem:
                print(f"  REMAINING_DIFFS ({len(rem)}):")
                for k in rem[:40]:
                    s_val, m_val, h_val = s_flat.get(k), m_flat.get(k), h_flat.get(k)
                    print(f"    {k}: spark={s_val} | main={m_val} | head={h_val}")
        else:
            diffs = len(keys) - eq_main
            print(f"=== {section}: total={len(keys)} main_eq={eq_main} diffs={diffs} ===")
            seen_patterns: set[tuple[str, str, str]] = set()
            for k in keys:
                if s_flat.get(k) != m_flat.get(k):
                    base_k = k.split("|")[0]
                    sub_k = "|".join(k.split("|")[1:2]) if "|" in k else ""
                    pat = (
                        base_k,
                        sub_k,
                        json.dumps(s_flat.get(k), sort_keys=True)
                        + " vs "
                        + json.dumps(m_flat.get(k), sort_keys=True),
                    )
                    if pat not in seen_patterns:
                        seen_patterns.add(pat)
                        print(f"  DIFF {k}:")
                        print(f"    spark={s_flat.get(k)}")
                        print(f"    main ={m_flat.get(k)}")


def main() -> int:
    sp = Path(sys.argv[1])
    mp = Path(sys.argv[2])
    hp = Path(sys.argv[3]) if len(sys.argv) > 3 else None
    summarize(sp, mp, hp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
