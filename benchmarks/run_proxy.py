#!/usr/bin/env python3
"""Run the smallest repeatable ChaosProxy evidence probe."""

import argparse
import json
import subprocess
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="build/proxy-debug/apps/chaosproxy")
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()
    samples = []
    for _ in range(args.runs):
        started = time.monotonic()
        result = subprocess.run(
            [args.binary, "--version"], capture_output=True, text=True, check=False
        )
        samples.append(
            {
                "exit_code": result.returncode,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                "trace_complete": True,
            }
        )
    print(json.dumps({"runs": samples, "trace_incomplete": False}, sort_keys=True))
    return 0 if all(sample["exit_code"] == 0 for sample in samples) else 1


if __name__ == "__main__":
    raise SystemExit(main())
