from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_PREFLIGHT_TIMEOUT_SECONDS = 180
OUTPUT_TAIL_CHARS = 12000


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def dataset_worker_root() -> Path:
    configured = os.getenv("SPEECHCRAFT_DATASET_WORKER_ROOT")
    if configured and configured.strip():
        return Path(configured).expanduser().resolve()
    return repo_root() / "workers" / "dataset"


def dataset_worker_python() -> Path:
    configured = os.getenv("SPEECHCRAFT_DATASET_WORKER_PYTHON")
    if configured and configured.strip():
        return Path(configured).expanduser().resolve()
    return dataset_worker_root() / ".venv" / "bin" / "python"


def dataset_worker_preflight_script() -> Path:
    return dataset_worker_root() / "scripts" / "preflight.py"


def run_dataset_worker_preflight(*, artifact_root: str | None = None) -> dict[str, Any]:
    worker_python = dataset_worker_python()
    preflight_script = dataset_worker_preflight_script()
    command = [str(worker_python), str(preflight_script), "--json"]
    if artifact_root:
        command.extend(["--artifact-root", artifact_root])

    if not worker_python.exists():
        return _failed_preflight(command, f"Dataset worker python not found: {worker_python}")
    if not preflight_script.exists():
        return _failed_preflight(command, f"Dataset worker preflight script not found: {preflight_script}")

    try:
        completed = subprocess.run(
            command,
            cwd=str(dataset_worker_root()),
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_PREFLIGHT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            **_failed_preflight(
                command,
                f"Dataset worker preflight timed out after {exc.timeout} seconds",
                reason_codes=["preflight_timeout"],
            ),
            "timeout_seconds": exc.timeout,
            "stdout_tail": _tail(exc.stdout),
            "stderr_tail": _tail(exc.stderr),
        }
    except OSError as exc:
        return _failed_preflight(
            command,
            f"Dataset worker preflight failed to launch: {exc}",
            reason_codes=["preflight_launch_failed"],
        )

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    try:
        payload = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError as exc:
        return {
            **_failed_preflight(
                command,
                f"Dataset worker preflight returned invalid JSON: {exc}",
                reason_codes=["preflight_invalid_json"],
            ),
            "returncode": completed.returncode,
            "stdout_tail": _tail(stdout),
            "stderr_tail": _tail(stderr),
        }

    if not isinstance(payload, dict):
        return {
            **_failed_preflight(
                command,
                "Dataset worker preflight returned non-object JSON",
                reason_codes=["preflight_invalid_json"],
            ),
            "returncode": completed.returncode,
            "stdout_tail": _tail(stdout),
            "stderr_tail": _tail(stderr),
        }

    payload.setdefault("ok", completed.returncode == 0)
    payload["returncode"] = completed.returncode
    payload["command"] = command
    payload["worker_root"] = str(dataset_worker_root())
    payload["stderr_tail"] = _tail(stderr)
    return payload


def _failed_preflight(command: list[str], error: str, *, reason_codes: list[str] | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "reason_codes": reason_codes or ["preflight_failed"],
        "command": command,
        "worker_root": str(dataset_worker_root()),
    }


def _tail(value: str | bytes | None, limit: int = OUTPUT_TAIL_CHARS) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = value
    return text[-limit:]
