#!/usr/bin/env python3
"""Profile one fixed command; changing the command is an explicit experiment variable."""
import argparse
import json
import pathlib
import statistics
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="build/kafka-debug/apps/photo_runtime")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    samples = []
    for _ in range(args.runs):
        started = time.monotonic()
        result = subprocess.run([args.binary, "--health"], capture_output=True, text=True, check=False)
        samples.append({"exit_code": result.returncode, "elapsed_ms": (time.monotonic() - started) * 1000})
    values = [sample["elapsed_ms"] for sample in samples]
    report = {
        "command": [args.binary, "--health"],
        "runs": samples,
        "summary": {"min_ms": min(values), "max_ms": max(values), "mean_ms": statistics.mean(values)},
        "interpretation": "startup/health command timing only; no throughput bottleneck is claimed",
    }
    output = pathlib.Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if all(sample["exit_code"] == 0 for sample in samples) else 1


if __name__ == "__main__":
    raise SystemExit(main())
