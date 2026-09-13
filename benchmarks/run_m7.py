#!/usr/bin/env python3
"""Collect reproducible M7 evidence without inventing throughput numbers."""
import argparse
import datetime
import json
import pathlib
import platform
import subprocess
import time


def command_result(command):
    started = time.monotonic()
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {
        "command": command,
        "exit_code": result.returncode,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", default="kafka-debug")
    parser.add_argument("--binary", default="build/kafka-debug/apps/photo_runtime")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", default="unset")
    args = parser.parse_args()
    results = [command_result([args.binary, "--health"]),
               command_result(["ctest", "--preset", args.preset, "--output-on-failure"])]
    report = {
        "schema_version": 1,
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "seed": args.seed,
        "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip(),
        "host": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
        "results": results,
        "interpretation": "timings are command-level evidence; no throughput or latency is inferred from them",
    }
    output = pathlib.Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if all(result["exit_code"] == 0 for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
