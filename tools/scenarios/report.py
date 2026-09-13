#!/usr/bin/env python3
import argparse
import datetime
import json
import pathlib
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenarios", nargs="+")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", default="unset")
    args = parser.parse_args()
    results = []
    for scenario in args.scenarios:
        command = ["python3", "tools/scenarios/runner.py", scenario, "--dry-run"]
        process = subprocess.run(command, check=False, capture_output=True, text=True)
        results.append(json.loads(process.stdout))
    report = {
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "seed": args.seed,
        "build": subprocess.run(["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True).stdout.strip(),
        "results": results,
    }
    output = pathlib.Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
