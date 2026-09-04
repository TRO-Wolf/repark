"""Orchestrate the dynamicFlatten bed, isolated repark cells, and one Spark JVM."""

from __future__ import annotations

import functools
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from windows.hardware import hardware_fields, native_build_flavor
from windows.oracles import ensure_java_home, peak_rss_bytes, pyspark_version

from dynflatten.models import CandidateCost, EngineTiming, FixtureResult, RunResult
from dynflatten.spark_flatten import spark_dynamic_flatten

_DATASETS_DIR = Path(__file__).resolve().parents[2] / "datasets"
_WORKER = Path(__file__).resolve().parent / "cell_worker.py"
SKIP_RSS_REASON = "spark_rss_is_jvm_not_python; one_jvm_power_budget"
DEFAULT_WARMUP = 1
DEFAULT_ITERATIONS = 5
EQUALITY_ROW_CAP = 20_000
THREADS = 8
NOISE_MULTIPLE = 3.0
NOISE_FLOOR_SHAPE = "struct_d3"
NOISE_REPEATS = 5


def native_is_release() -> bool:
    """True when the loaded native module was compiled without debug assertions."""
    from repark import _native

    return not getattr(_native, "__debug_assertions__", True)


def _load_bed() -> Any:
    """Import ``repark_datasets.nested.bed`` via the datasets loader."""
    import importlib
    import types

    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__dict__["__path__"] = [str(_DATASETS_DIR)]
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
    target_partitions: int | None = THREADS,
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
    if target_partitions is not None:
        command += ["--target-partitions", str(target_partitions)]
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
        SparkSession.builder.master(f"local[{THREADS}]")
        .appName("dynflatten-oracle")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(THREADS))
        .getOrCreate()
    )
    timings: dict[str, EngineTiming] = {}
    equality: dict[str, bool | None] = {}
    try:
        for row in files:
            parquet_path = bed_dir / row["path"]
            source = session.read.parquet(str(parquet_path)).cache()
            source.count()
            for _ in range(warmup):
                spark_dynamic_flatten(source).toArrow()
            samples: list[float] = []
            rows_out = 0
            for _ in range(iterations):
                started = time.perf_counter()
                table = spark_dynamic_flatten(source).toArrow()
                samples.append((time.perf_counter() - started) * 1000.0)
                rows_out = table.num_rows
            source.unpersist()
            timings[row["shape"]] = EngineTiming(
                engine="pyspark",
                outcome="ok",
                warmup=warmup,
                iterations=iterations,
                execute_ms=samples,
                median_execute_ms=_median(samples),
                median_wall_ms=_median(samples),
                min_execute_ms=min(samples) if samples else None,
                min_wall_ms=min(samples) if samples else None,
                peak_rss_bytes=None,
                rows_out=rows_out,
                target_partitions=THREADS,
                message=SKIP_RSS_REASON,
                version=version,
            )
            equality[row["shape"]] = None
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


def _median_execute(by_shape: dict[str, FixtureResult], shape: str) -> float | None:
    """Median execute wall for one shape, or None when the cell did not run."""
    row = by_shape.get(shape)
    return None if row is None else row.repark.median_execute_ms


def _median_rewrite(by_shape: dict[str, FixtureResult], shape: str) -> float | None:
    """Median rewrite wall for one shape, or None when the cell did not run."""
    row = by_shape.get(shape)
    return None if row is None else row.repark.median_rewrite_ms


def _strongest(per_fixture: dict[str, float]) -> tuple[str | None, float | None]:
    """The single fixture with the largest isolated cost, never a sum across fixtures."""
    if not per_fixture:
        return None, None
    shape = max(per_fixture, key=lambda key: per_fixture[key])
    return shape, per_fixture[shape]


def _verdict_for(cost: float | None, noise_floor_ms: float | None) -> tuple[str, float | None]:
    """Queue a candidate only when its isolated cost clears the noise floor by NOISE_MULTIPLE."""
    if cost is None or noise_floor_ms is None or noise_floor_ms <= 0:
        return "not measurable", None
    ratio = cost / noise_floor_ms
    return ("queued" if ratio >= NOISE_MULTIPLE else "not worth it"), ratio


def _fmt(value: float | None) -> str:
    """One decimal place, or empty when the value is missing or zero."""
    return f"{value:.1f}" if value else ""


def rank_candidates(
    fixtures: list[FixtureResult], noise_floor_ms: float | None
) -> list[CandidateCost]:
    """Rank the H-3 candidates by their strongest SINGLE-fixture isolated cost."""
    by_shape = {
        row.shape: row
        for row in fixtures
        if row.repark.outcome == "ok" and row.repark.median_wall_ms
    }
    execute = functools.partial(_median_execute, by_shape)
    rewrite = functools.partial(_median_rewrite, by_shape)

    walks: dict[str, float] = {}
    for name in ("struct_d3", "struct_d6", "cartesian_two_lists"):
        value = rewrite(name)
        if value is not None:
            walks[name] = value

    null_mask: dict[str, float] = {}
    for nulls, plain in (("struct_d3", "struct_d3_nonull"), ("struct_d6", "struct_d6_nonull")):
        left, right = execute(nulls), execute(plain)
        if left is not None and right is not None:
            null_mask[nulls] = left - right

    cartesian: dict[str, float] = {}
    both = execute("cartesian_two_lists")
    legs = execute("cartesian_legs_only")
    tags = execute("cartesian_tags_only")
    if both is not None and legs is not None and tags is not None:
        cartesian["cartesian_two_lists"] = both - (legs + tags)

    specs = [
        (
            "null_mask_struct_extractor",
            null_mask,
            "struct fixture at 30 % null parents minus the same fixture at 0 % nulls",
            "validity-bitmap extract instead of CASE WHEN parent IS NULL per field",
        ),
        (
            "cartesian_multi_list_operator",
            cartesian,
            "two-list fixture minus (legs-only + tags-only)",
            "one Cartesian operator",
        ),
        (
            "optimizer_wrapper_walks",
            walks,
            "rewrite wall of one fixture (the wrapper's subtree walks)",
            "remove repeated has_struct/has_list scans",
        ),
    ]
    ranked: list[CandidateCost] = []
    for name, per_fixture, evidence, gain in specs:
        strongest, cost = _strongest(per_fixture)
        verdict, ratio = _verdict_for(cost, noise_floor_ms)
        detail = ", ".join(f"{shape} {value:.2f} ms" for shape, value in per_fixture.items())
        ranked.append(
            CandidateCost(
                name=name,
                evidence=(f"{evidence}; per fixture: {detail}" if detail else f"{evidence}: none"),
                verdict=verdict,
                projected_gain=gain,
                isolated_cost_ms=cost,
                strongest_fixture=strongest,
                per_fixture_ms=per_fixture,
                noise_floor_ms=noise_floor_ms,
                cost_over_noise=ratio,
            )
        )
    return sorted(ranked, key=lambda item: item.isolated_cost_ms or 0.0, reverse=True)


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
    started_wall = time.time()
    release = native_is_release()
    if not release:
        msg = "H-3a: refusing to measure or write a report on a debug native build"
        raise RuntimeError(msg)
    bed = _load_bed()
    bed.refuse_real_dataset_inputs(argv=[])
    manifest = bed.write_bed(scale=scale, seed=seed, out=out_dir)
    files: list[dict[str, Any]] = manifest["files"]
    worker_dir = out_dir / "cells"
    worker_dir.mkdir(parents=True, exist_ok=True)
    repark_timings: dict[str, EngineTiming] = {}
    all_cores: dict[str, EngineTiming] = {}
    repark_tables: dict[str, Any] = {}
    noise_floor: float | None = None
    noise_samples: list[float] = []
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
        if row["shape"] == NOISE_FLOOR_SHAPE:
            repeats = [timing.median_wall_ms] if timing.median_wall_ms else []
            for index in range(NOISE_REPEATS):
                repeat = run_repark_isolated(
                    parquet_path,
                    worker_dir / f"{row['shape']}_noise{index}.json",
                    ddl=ddl,
                    warmup=warmup,
                    iterations=iterations,
                )
                if repeat.outcome == "ok" and repeat.median_wall_ms:
                    repeats.append(repeat.median_wall_ms)
            if len(repeats) >= 2:
                noise_floor = max(repeats) - min(repeats)
                noise_samples = repeats
        if not row.get("isolation"):
            all_cores[row["shape"]] = run_repark_isolated(
                parquet_path,
                worker_dir / f"{row['shape']}_allcores.json",
                ddl=ddl,
                warmup=warmup,
                iterations=iterations,
                target_partitions=None,
            )
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
                isolation=bool(row.get("isolation")),
                repark_all_cores=all_cores.get(row["shape"]),
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
        candidates=rank_candidates(fixtures, noise_floor),
        run_date=time.strftime("%Y-%m-%d", time.localtime(started_wall)),
        native_is_release=release,
        target_partitions=THREADS,
        spark_threads=THREADS,
        noise_floor_ms=noise_floor,
        noise_floor_shape=NOISE_FLOOR_SHAPE,
        noise_samples=noise_samples,
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
        f"- date: {result.run_date}",
        f"- scale: `{result.scale}` seed `{result.seed}`",
        f"- native: `{result.native_build}` release `{result.native_is_release}`",
        f"- threads: repark target_partitions `{result.target_partitions}`, "
        f"spark `local[{result.spark_threads}]`",
        f"- noise floor: `{result.noise_floor_ms}` ms on `{result.noise_floor_shape}`",
        f"- machine: cpu `{result.machine.get('cpu')}` cores `{result.machine.get('cores')}` "
        f"governor `{result.machine.get('governor')}` ram_gib `{result.machine.get('ram_gib')}`",
        f"- repark `{result.engine_version}` pyspark `{result.pyspark_version}` "
        f"skip `{result.pyspark_skip_reason}`",
        f"- wall_seconds `{result.wall_seconds:.1f}` peak_rss_bytes `{result.peak_rss_bytes}`",
        "",
        "## Fixtures",
        "",
        "| shape | iso | rows_in | repark_med | repark_min | rewrite | rss_MiB | "
        "spark_med | spark_min | ratio | all_cores | rows_out |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result.fixtures:
        rss = (
            f"{row.repark.peak_rss_bytes / (1024 * 1024):.0f}" if row.repark.peak_rss_bytes else ""
        )
        ratio = (
            f"{row.wall_ratio_repark_over_spark:.2f}"
            if row.wall_ratio_repark_over_spark is not None
            else ""
        )
        all_cores = ""
        if row.repark_all_cores is not None and row.repark_all_cores.median_wall_ms:
            all_cores = f"{row.repark_all_cores.median_wall_ms:.1f}"

        lines.append(
            f"| {row.shape} | {'y' if row.isolation else ''} | {row.rows_in} | "
            f"{_fmt(row.repark.median_wall_ms)} | {_fmt(row.repark.min_wall_ms)} | "
            f"{_fmt(row.repark.median_rewrite_ms)} | {rss} | "
            f"{_fmt(row.spark.median_wall_ms)} | {_fmt(row.spark.min_wall_ms)} | "
            f"{ratio} | {all_cores} | {row.repark.rows_out or ''} |"
        )
    lines.extend(
        [
            "",
            "## Candidates (strongest single-fixture isolated cost)",
            "",
            "| rank | name | strongest fixture | isolated_ms | x_noise | verdict | evidence |",
            "|---:|---|---|---:|---:|---|---|",
        ]
    )
    for index, candidate in enumerate(result.candidates, start=1):
        cost = f"{candidate.isolated_cost_ms:.2f}" if candidate.isolated_cost_ms is not None else ""
        over = f"{candidate.cost_over_noise:.1f}" if candidate.cost_over_noise is not None else ""
        lines.append(
            f"| {index} | {candidate.name} | {cost} | {over} | "
            f"{candidate.verdict} | {candidate.evidence} |"
        )
    lines.extend(["", "## Notes", ""])
    for note in result.notes:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)
