from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import common as cm
import write_sources as ws


def main() -> int:
    engine, warehouse, root, out, tag = (
        sys.argv[1],
        Path(sys.argv[2]),
        Path(sys.argv[3]),
        Path(sys.argv[4]),
        sys.argv[5],
    )
    session = ws.session_for(engine, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cm.CAT}.{cm.NS}").collect()
    with out.open("w") as sink:
        cm.dump(sink, cm.header(engine))
        for fmt in ("parquet", "avro"):
            name = f"{cm.CAT}.{cm.NS}.ingest_{tag}_{fmt}"
            if engine == "repark":
                step = cm.attempt(
                    lambda n=name, f=fmt: (
                        session.sql(
                            f"CREATE TABLE {n} (id BIGINT, d DOUBLE) USING iceberg {ws.props(f)}"
                        ).collect(),
                        session.read.parquet(str(root / "pa_const")).writeTo(n).append(),
                        {},
                    )[2]
                )
                cm.dump(sink, {"record": "write", "table": name, **step})
            for reader_tag in ("rb", "sp"):
                target = f"{cm.CAT}.{cm.NS}.ingest_{reader_tag}_{fmt}"
                result = cm.attempt(
                    lambda t=target: cm.collect(session.sql(f"SELECT id, d FROM {t} ORDER BY id"))
                )
                cm.dump(sink, {"record": "read", "table": target, **result})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
