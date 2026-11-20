#!/usr/bin/env python3
"""
Compare idle CPU and memory stability across observation window lengths.

Each round runs every window's config once, in order, so all windows are
sampled across the same period of runner noise. After the warm-up rounds
(discarded), each measured run's benchmark JSON is saved under
<output-dir>/<window>/iter_NN. The report gives, per window, the mean,
standard deviation, and coefficient of variation (CV) of idle CPU and idle
memory, plus the per-run CPU values.

Usage (relative paths are resolved against tools/pipeline_perf_test):
    python scripts/run_idle_window_experiment.py --rounds 5 \\
        --output-dir results/window_experiment \\
        --window 15s <config> <results-dir> \\
        --window 60s <config> <results-dir>
"""

import argparse
import json
import math
import os
import shutil
import statistics
import sys
from pathlib import Path
from typing import Mapping, Optional

from analyze_idle_state_variance import extract_metric
from run_idle_state_variance import (
    PERF_TEST_DIR,
    RunCommand,
    RunnerError,
    _resolve,
    _run_orchestrator,
    _run_subprocess,
    append_step_summary,
    collect_runner_metadata,
    format_metadata_markdown,
)

CPU_METRIC = "idle_cpu_percentage_avg"
RAM_METRIC = "idle_ram_mib_avg"
DURATION_METRIC = "idle_test_duration"


def run_rounds(
    windows: list[tuple[str, str, Path]],
    rounds: int,
    warmup_rounds: int,
    output_dir: Path,
    run_command: RunCommand = _run_subprocess,
    perf_test_dir: Path = PERF_TEST_DIR,
) -> None:
    for r in range(1, warmup_rounds + 1):
        for name, config, _ in windows:
            print(f"=== Warm-up round {r}/{warmup_rounds}: {name} (discarded) ===", flush=True)
            _run_orchestrator(config, f"Warm-up {name} round {r}", run_command, perf_test_dir)

    for r in range(1, rounds + 1):
        for name, config, results_dir in windows:
            print(f"=== Round {r}/{rounds}: {name} ===", flush=True)
            if results_dir.exists():
                shutil.rmtree(results_dir)
            _run_orchestrator(config, f"{name} round {r}", run_command, perf_test_dir)
            json_files = sorted(results_dir.glob("*.json")) if results_dir.is_dir() else []
            if not json_files:
                raise RunnerError(f"{name} round {r} produced no benchmark JSON in {results_dir}")
            iter_dir = output_dir / name / f"iter_{r:02d}"
            iter_dir.mkdir(parents=True)
            for json_file in json_files:
                shutil.copy2(json_file, iter_dir / json_file.name)


def collect_values(window_dir: Path, metric: str) -> list[float]:
    values = []
    for iter_dir in sorted(window_dir.glob("iter_*")):
        json_files = sorted(iter_dir.glob("*.json"))
        if not json_files:
            continue
        value = extract_metric(json_files[-1], metric)
        if value is not None and math.isfinite(float(value)):
            values.append(float(value))
    return values


def describe(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {"n": 0, "mean": None, "stddev": None, "cv_percent": None, "min": None, "max": None}
    mean = statistics.fmean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "n": len(values),
        "mean": mean,
        "stddev": stddev,
        "cv_percent": (stddev / mean * 100.0) if mean > 0 else None,
        "min": min(values),
        "max": max(values),
    }


def summarize_windows(output_dir: Path, names: list[str]) -> dict[str, dict]:
    summary = {}
    for name in names:
        window_dir = output_dir / name
        cpu = collect_values(window_dir, CPU_METRIC)
        summary[name] = {
            "cpu": describe(cpu),
            "cpu_values": cpu,
            "ram": describe(collect_values(window_dir, RAM_METRIC)),
            "duration": describe(collect_values(window_dir, DURATION_METRIC)),
        }
    return summary


def _fmt(value: Optional[float], digits: int = 2, suffix: str = "") -> str:
    return "n/a" if value is None else f"{value:.{digits}f}{suffix}"


def format_report(summary: Mapping[str, dict], max_cv_percent: float) -> str:
    lines = [
        "### Idle observation window comparison (Windows, 1 core)",
        "",
        "| Window | Measured (s) | N | CPU mean % | CPU stddev | CPU CV | RAM mean MiB | RAM CV |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, s in summary.items():
        cpu, ram, dur = s["cpu"], s["ram"], s["duration"]
        lines.append(
            f"| {name} | {_fmt(dur['mean'], 1)} | {cpu['n']} | {_fmt(cpu['mean'], 3)} | "
            f"{_fmt(cpu['stddev'], 3)} | {_fmt(cpu['cv_percent'], 1, '%')} | "
            f"{_fmt(ram['mean'])} | {_fmt(ram['cv_percent'], 2, '%')} |"
        )
    lines += ["", f"CPU CV at or below {max_cv_percent:.0f}% (the memory threshold):"]
    for name, s in summary.items():
        cv = s["cpu"]["cv_percent"]
        verdict = "n/a" if cv is None else ("yes" if cv <= max_cv_percent else "no")
        lines.append(f"- {name}: {verdict}")
    lines += ["", "Per-run idle CPU % (in round order):", ""]
    for name, s in summary.items():
        lines.append(f"- {name}: " + ", ".join(f"{v:.3f}" for v in s["cpu_values"]))
    return "\n".join(lines)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--window",
        action="append",
        nargs=3,
        metavar=("NAME", "CONFIG", "RESULTS_DIR"),
        required=True,
        help="Window label, orchestrator config, and the config's results directory.",
    )
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--max-cv-percent", type=float, default=15.0)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(
    argv: Optional[list[str]] = None,
    env: Mapping[str, str] = os.environ,
    run_command: RunCommand = _run_subprocess,
    perf_test_dir: Path = PERF_TEST_DIR,
) -> int:
    args = parse_args(argv)
    windows = [
        (name, config, _resolve(Path(results), perf_test_dir))
        for name, config, results in args.window
    ]
    output_dir = _resolve(args.output_dir, perf_test_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    metadata = collect_runner_metadata(
        {
            "windows": [name for name, _, _ in windows],
            "rounds": args.rounds,
            "warmup_rounds": args.warmup_rounds,
        },
        env,
    )
    (output_dir / "runner-metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    append_step_summary(format_metadata_markdown(metadata), env)

    try:
        run_rounds(windows, args.rounds, args.warmup_rounds, output_dir, run_command, perf_test_dir)
    except RunnerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    summary = summarize_windows(output_dir, [name for name, _, _ in windows])
    (output_dir / "comparison.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    report = format_report(summary, args.max_cv_percent)
    print(report)
    append_step_summary(report, env)
    return 0


if __name__ == "__main__":
    sys.exit(main())
