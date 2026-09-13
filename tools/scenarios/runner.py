#!/usr/bin/env python3
"""Small, safe scenario runner: commands are argv arrays, never shell strings."""
import argparse
import json
import subprocess
import sys
import time
import urllib.request


def load(path):
    with open(path, encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict) or not isinstance(value.get("steps", []), list):
        raise ValueError("scenario must contain a steps array")
    return value


def await_ready(url, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("readiness deadline exceeded")


def run(path, dry_run=False):
    scenario = load(path)
    if dry_run:
        return {"name": scenario.get("name", path), "status": "skipped", "reason": "dry-run"}
    try:
        if "ready_url" in scenario:
            await_ready(scenario["ready_url"], float(scenario.get("ready_timeout_s", 10)))
        for step in scenario["steps"]:
            if "command" in step:
                command = step["command"]
                if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
                    raise ValueError("command must be an argv list")
                subprocess.run(command, check=True, timeout=float(step.get("timeout_s", 30)))
            elif "request" in step:
                request = step["request"]
                with urllib.request.urlopen(request["url"], timeout=float(step.get("timeout_s", 10))) as response:
                    expected = int(request.get("status", 200))
                    if response.status != expected:
                        raise RuntimeError(f"expected {expected}, got {response.status}")
            else:
                raise ValueError("step must contain command or request")
        return {"name": scenario.get("name", path), "status": "passed"}
    finally:
        for command in scenario.get("finally", []):
            subprocess.run(command, check=False, timeout=30)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.scenario, args.dry_run), sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
