import os
import json
import subprocess
import tempfile
from fastapi import Body

RUNNERS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "engine_core", "runners")
)


def _runner_path(runner_name: str) -> str:
    return os.path.join(RUNNERS_DIR, runner_name)


def run_engine(runner_name: str, payload: dict, allowed_runners):
    if runner_name not in allowed_runners:
        return {"error": f"runner not allowed: {runner_name}", "allowed": sorted(list(allowed_runners))}

    runner_path = _runner_path(runner_name)
    if not os.path.exists(runner_path):
        return {"error": f"runner not found: {runner_path}"}

    with tempfile.TemporaryDirectory() as td:
        in_path = os.path.join(td, "input.json")
        out_path = os.path.join(td, "output.json")

        with open(in_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        cmd = ["python", runner_path, "--in", in_path, "--out", out_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode != 0:
            return {
                "error": "runner failed",
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "cmd": cmd,
            }

        if os.path.exists(out_path):
            with open(out_path, "r", encoding="utf-8") as f:
                return json.load(f)

        try:
            return json.loads(proc.stdout)
        except Exception:
            return {
                "error": "no output.json and stdout not json",
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "cmd": cmd,
            }