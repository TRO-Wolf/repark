"""Orchestrate the dynamicFlatten bed, isolated repark cells, and one Spark JVM."""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from windows.hardware import hardware_fields, native_build_flavor
from windows.oracles import ensure_java_home, peak_rss_bytes, pyspark_version

from dynflatten.models import CandidateShare, EngineTiming, FixtureResult, RunResult
from dynflatten.spark_flatten import spark_dynamic_flatten

_DATASETS_DIR = Path(__file__).resolve().parents[2] / "datasets"
_WORKER = Path(__file__).resolve().parent / "cell_worker.py"
SKIP_RSS_REASON = "spark_rss_is_jvm_not_python; one_jvm_power_budget"
DEFAULT_WARMUP = 1
DEFAULT_ITERATIONS = 3
EQUALITY_ROW_CAP = 20_000


def _load_bed() -> Any:
    """Import ``repark_datasets.nested.bed`` via the datasets loader."""
    import importlib
    import types

    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module("repark_datasets.nested.bed")


def _median(samples: list[float]) -> float | None:
    """Median of a non-empty sample list."""
    if not samples:
        return None
    return float(statistics.median(samples))


def _timing_from_payload(payload: dict[str, Any]) -> EngineTiming:
    """Build an ``EngineTiming`` from a worker JSON payload."""
    return EngineTiming.model_validate(payload)


def run_repark_isolated(
    parquet_path: Path,
    json_out: Path,
    *,
    ddl: str,
    warmup: int,
    iterations: int,
) -> EngineTiming:
    """Run one repark cell in a subprocess (process-lifetime peak RSS)."""
    command = [
        sys.executable,
        str(_WORKER),
        "--parquet",
        str(parquet_path),
        "--ddl",
        ddl,
        "--json-out",
        str(json_out),
        "--warmup",
        str(warmup),
        "--iterations",
        str(iterations),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if not json_out.is_file():
        return EngineTiming(
            engine="repark",
            outcome="error",
            warmup=warmup,
            iterations=0,
            message=(f"worker_exit={completed.returncode} stderr={completed.stderr[-800:]}"),
        )
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    return _timing_from_payload(payload)


def _arrow_type_decode_dictionary(data_type: Any) -> Any:
    """Replace dictionary Arrow types with their value type."""
    import pyarrow as pa

    if pa.types.is_dictionary(data_type):
        return data_type.value_type
    if pa.types.is_struct(data_type):
        return pa.struct(
            [
                pa.field(
                    field.name,
                    _arrow_type_decode_dictionary(field.type),
                    nullable=field.nullable,
                )
                for field in data_type
            ]
        )
    if pa.types.is_list(data_type):
        return pa.list_(_arrow_type_decode_dictionary(data_type.value_type))
    return data_type


def _decode_table(table: Any) -> Any:
    """Cast dictionary columns to utf8 for value comparison."""
    import pyarrow as pa

    fields = [
        pa.field(field.name, _arrow_type_decode_dictionary(field.type), nullable=field.nullable)
        for field in table.schema
    ]
    return table.cast(pa.schema(fields))


def _row_set_equal(repark_table: Any, spark_table: Any) -> bool:
    """Value equality after dictionary decode (order-insensitive)."""
    from repark_parity import assert_frames_equal

    left = _decode_table(repark_table)
    right = _decode_table(spark_table)
    try:
        assert_frames_equal(left, right, order_sensitive=False)
    except Exception:
        return False
    return True


def run_spark_cells(
    files: list[dict[str, Any]],
    bed_dir: Path,
    *,
    warmup: int,
    iterations: int,
    collect_equality: bool,
) -> tuple[dict[str, EngineTiming], dict[str, bool | None], str | None]:
    """Time the Spark explode program on every fixture with one JVM."""
    version, skip = pyspark_version()
    if skip is not None:
        empty = {
            row["shape"]: EngineTiming(
                engine="pyspark",
                outcome="skip",
                warmup=0,
                iterations=0,
                message=skip,
                version=version,
            )
            for row in files
        }
        return empty, {row["shape"]: None for row in files}, skip
    ensure_java_home()
    from pyspark.sql import SparkSession

    previous = SparkSession.getActiveSession()
    created = previous is None
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dynflatten-oracle")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    timings: dict[str, EngineTiming] = {}
    equality: dict[str, bool | None] = {}
    try:
        for row in files:
            parquet_path = bed_dir / row["path"]
            source = session.read.parquet(str(parquet_path))
            for _ in range(warmup):
                spark_dynamic_flatten(source).toArrow()
            samples: list[float] = []
            rows_out = 0
            spark_table = None
            for _ in range(iterations):
                started = time.perf_counter()
                flat = spark_dynamic_flatten(source)
                table = flat.toArrow()
                samples.append((time.perf_counter() - started) * 1000.0)
                rows_out = table.num_rows
                spark_table = table
            timings[row["shape"]] = EngineTiming(
                engine="pyspark",
                outcome="ok",
                warmup=warmup,
                iterations=iterations,
                execute_ms=samples,
                median_execute_ms=_median(samples),
                median_wall_ms=_median(samples),
                peak_rss_bytes=None,
                rows_out=rows_out,
                message=SKIP_RSS_REASON,
                version=version,
            )
            equality[row["shape"]] = None
            _ = (collect_equality, spark_table)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        for row in files:
            timings.setdefault(
                row["shape"],
                EngineTiming(
                    engine="pyspark",
                    outcome="error",
                    warmup=warmup,
                    iterations=0,
                    message=f"{type(error).__name__}: {error}",
                    version=version,
                ),
            )
            equality.setdefault(row["shape"], None)
    finally:
        if created:
            session.stop()
    return timings, equality, None


def rank_candidates(fixtures: list[FixtureResult]) -> list[CandidateShare]:
    """Rank the three H-3 intake candidates by measured wall share."""
    ok = [row for row in fixtures if row.repark.outcome == "ok" and row.repark.median_wall_ms]
    total = sum(row.repark.median_wall_ms or 0.0 for row in ok)
    if total <= 0:
        return [
            CandidateShare(
                name="optimizer_wrapper_walks",
                wall_share=0.0,
                evidence="no successful repark cells",
                verdict="not worth it",
                projected_gain="n/a",
            )
        ]
    rewrite = sum(row.repark.median_rewrite_ms or 0.0 for row in ok)
    struct_wall = sum((row.repark.median_execute_ms or 0.0) for row in ok if row.kind == "struct")
    cartesian_wall = sum(
        (row.repark.median_execute_ms or 0.0) for row in ok if row.kind == "cartesian"
    )
    walk_share = rewrite / total
    struct_share = struct_wall / total
    cartesian_share = cartesian_wall / total
    walk_verdict = "not worth it" if walk_share < 0.05 else "implement"
    struct_verdict = "implement" if struct_share >= 0.20 else "not worth it"
    cartesian_verdict = "implement" if cartesian_share >= 0.20 else "not worth it"
    ranked = [
        CandidateShare(
            name="optimizer_wrapper_walks",
            wall_share=walk_share,
            evidence=(
                f"rewrite median sum {rewrite:.1f} ms of total {total:.1f} ms "
                f"(schema walks are plan-time; Rust pin holds exact counts)"
            ),
            verdict=walk_verdict,
            projected_gain=(
                "sub-millisecond on these cells"
                if walk_verdict != "implement"
                else "remove repeated has_struct/has_list scans"
            ),
        ),
        CandidateShare(
            name="null_mask_struct_extractor",
            wall_share=struct_share,
            evidence=(
                f"struct-shape execute {struct_wall:.1f} ms of total {total:.1f} ms "
                f"(CASE WHEN parent IS NULL per field)"
            ),
            verdict=struct_verdict,
            projected_gain=(
                "validity-bitmap extract on struct_d3/struct_d6"
                if struct_verdict == "implement"
                else "struct execute is not the dominant wall"
            ),
        ),
        CandidateShare(
            name="cartesian_multi_list_operator",
            wall_share=cartesian_share,
            evidence=(
                f"cartesian execute {cartesian_wall:.1f} ms of total {total:.1f} ms "
                f"(two sequential Unnests; zip/pad is not a substitute)"
            ),
            verdict=cartesian_verdict,
            projected_gain=(
                "one Cartesian operator"
                if cartesian_verdict == "implement"
                else "two Unnests are not the dominant wall"
            ),
        ),
    ]
    return sorted(ranked, key=lambda item: item.wall_share, reverse=True)


def run_measurement(
    *,
    scale: str,
    out_dir: Path,
    seed: int = 42,
    warmup: int = DEFAULT_WARMUP,
    iterations: int = DEFAULT_ITERATIONS,
    skip_pyspark: bool = False,
) -> RunResult:
    """Generate the bed, time repark (isolated) and Spark (one JVM), rank candidates."""
    started = time.perf_counter()
    bed = _load_bed()
    bed.refuse_real_dataset_inputs(argv=[])
    manifest = bed.write_bed(scale=scale, seed=seed, out=out_dir)
    files: list[dict[str, Any]] = manifest["files"]
    worker_dir = out_dir / "cells"
    worker_dir.mkdir(parents=True, exist_ok=True)
    repark_timings: dict[str, EngineTiming] = {}
    repark_tables: dict[str, Any] = {}
    for row in files:
        parquet_path = out_dir / row["path"]
        json_out = worker_dir / f"{row['shape']}.json"
        ddl = bed.ddl_for(row["shape"])
        timing = run_repark_isolated(
            parquet_path,
            json_out,
            ddl=ddl,
            warmup=warmup,
            iterations=iterations,
        )
        repark_timings[row["shape"]] = timing
        if (
            timing.outcome == "ok"
            and timing.rows_out is not None
            and timing.rows_out <= EQUALITY_ROW_CAP
        ):
            import pyarrow.parquet as pq

            from repark import ReparkSession

            session = (
                ReparkSession.builder.appName("dynflatten-eq").master("local[1]").getOrCreate()
            )
            try:
                loaded = session.createDataFrame(
                    pq.read_table(parquet_path).to_pylist(), schema=ddl
                )
                repark_tables[row["shape"]] = loaded.dynamicFlatten().to_arrow()
            finally:
                session.stop()
    spark_timings: dict[str, EngineTiming] = {}
    equality: dict[str, bool | None] = {row["shape"]: None for row in files}
    spark_skip: str | None = "skipped by flag" if skip_pyspark else None
    if not skip_pyspark:
        spark_timings, _eq, spark_skip = run_spark_cells(
            files,
            out_dir,
            warmup=warmup,
            iterations=iterations,
            collect_equality=scale == "gate",
        )
        if scale == "gate":
            from pyspark.sql import SparkSession

            previous = SparkSession.getActiveSession()
            created = previous is None
            session = SparkSession.builder.master("local[1]").appName("dynflatten-eq").getOrCreate()
            try:
                for row in files:
                    shape = row["shape"]
                    if shape not in repark_tables:
                        continue
                    parquet_path = out_dir / row["path"]
                    spark_table = spark_dynamic_flatten(
                        session.read.parquet(str(parquet_path))
                    ).toArrow()
                    equality[shape] = _row_set_equal(repark_tables[shape], spark_table)
            finally:
                if created:
                    session.stop()
    else:
        spark_timings = {
            row["shape"]: EngineTiming(
                engine="pyspark",
                outcome="skip",
                warmup=0,
                iterations=0,
                message=spark_skip,
            )
            for row in files
        }
    fixtures: list[FixtureResult] = []
    for row in files:
        repark = repark_timings[row["shape"]]
        spark = spark_timings[row["shape"]]
        ratio = None
        if repark.median_wall_ms and spark.median_wall_ms and spark.median_wall_ms > 0:
            ratio = repark.median_wall_ms / spark.median_wall_ms
        fixtures.append(
            FixtureResult(
                shape=row["shape"],
                kind=row["kind"],
                struct_depth=row["struct_depth"],
                list_width=row["list_width"],
                rows_in=row["rows"],
                parquet_bytes=row["bytes"],
                digest=row["digest"],
                repark=repark,
                spark=spark,
                row_set_equal=equality[row["shape"]],
                wall_ratio_repark_over_spark=ratio,
            )
        )
    from repark import __version__ as repark_version

    pyspark_ver, pyspark_skip = (None, spark_skip)
    if not skip_pyspark:
        pyspark_ver, pyspark_skip = pyspark_version()
    notes = [
        f"full-scale skip: {manifest.get('skipped')}",
        "repark cells: one subprocess each (H-3a isolation)",
        "spark cells: one JVM (power budget); peak RSS is Python-process only",
        "walk counts: Rust kernel stats (schema-only; see octo flatten_stats pins)",
    ]
    return RunResult(
        scale=scale,
        seed=seed,
        native_build=native_build_flavor(),
        machine=hardware_fields(),
        engine_version=str(repark_version),
        pyspark_version=pyspark_ver,
        pyspark_skip_reason=pyspark_skip,
        fixtures=fixtures,
        candidates=rank_candidates(fixtures),
        peak_rss_bytes=peak_rss_bytes(),
        wall_seconds=time.perf_counter() - started,
        notes=notes,
        extra={"manifest": manifest, "bed": str(out_dir)},
    )


def render_markdown(result: RunResult) -> str:
    """Render the baseline note body from a run result."""
    lines = [
        f"# dynamicFlatten baseline ({result.scale}, {result.machine.get('cpu', 'unknown')})",
        "",
        "- date: 2026-09-04",
        f"- scale: `{result.scale}` seed `{result.seed}`",
        f"- native: `{result.native_build}`",
        f"- machine: cpu `{result.machine.get('cpu')}` cores `{result.machine.get('cores')}` "
        f"governor `{result.machine.get('governor')}` ram_gib `{result.machine.get('ram_gib')}`",
        f"- repark `{result.engine_version}` pyspark `{result.pyspark_version}` "
        f"skip `{result.pyspark_skip_reason}`",
        f"- wall_seconds `{result.wall_seconds:.1f}` peak_rss_bytes `{result.peak_rss_bytes}`",
        "",
        "## Fixtures",
        "",
        "| shape | rows_in | repark_ms | rewrite_ms | execute_ms | rss_MiB | plan_nodes | "
        "spark_ms | ratio | rows_out | row_set_equal |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in result.fixtures:
        rss = (
            f"{row.repark.peak_rss_bytes / (1024 * 1024):.1f}" if row.repark.peak_rss_bytes else ""
        )
        ratio = (
            f"{row.wall_ratio_repark_over_spark:.2f}"
            if row.wall_ratio_repark_over_spark is not None
            else ""
        )
        equal = "" if row.row_set_equal is None else str(row.row_set_equal)
        lines.append(
            f"| {row.shape} | {row.rows_in} | {row.repark.median_wall_ms or ''} | "
            f"{row.repark.median_rewrite_ms or ''} | {row.repark.median_execute_ms or ''} | "
            f"{rss} | {row.repark.plan_nodes or ''} | {row.spark.median_wall_ms or ''} | "
            f"{ratio} | {row.repark.rows_out or ''} | {equal} |"
        )
    lines.extend(
        [
            "",
            "## Candidates",
            "",
            "| rank | name | wall_share | verdict | evidence |",
            "|---:|---|---:|---|---|",
        ]
    )
    for index, candidate in enumerate(result.candidates, start=1):
        lines.append(
            f"| {index} | {candidate.name} | {candidate.wall_share:.3f} | "
            f"{candidate.verdict} | {candidate.evidence} |"
        )
    lines.extend(["", "## Notes", ""])
    for note in result.notes:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)
