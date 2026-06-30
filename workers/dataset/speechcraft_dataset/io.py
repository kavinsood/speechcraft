from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any


def validate_run_relative_path(value: str, field_name: str) -> str:
    path = value.strip()
    if not path:
        raise ValueError(f"{field_name} must not be empty")
    if "\\" in path:
        raise ValueError(f"{field_name} must use POSIX separators")
    if ":" in path:
        raise ValueError(f"{field_name} must not contain drive or URI separators")
    if "//" in path:
        raise ValueError(f"{field_name} must not contain repeated separators")
    if path.startswith("~"):
        raise ValueError(f"{field_name} must be relative to the run root")
    if path.startswith("./") or "/./" in path or path.endswith("/."):
        raise ValueError(f"{field_name} must not contain current-directory path parts")
    if path.endswith("/") or path.endswith("/.."):
        raise ValueError(f"{field_name} must not end with a directory traversal marker")
    parsed = PurePosixPath(path)
    if parsed.is_absolute():
        raise ValueError(f"{field_name} must be relative to the run root")
    if any(part in {"", ".", ".."} for part in parsed.parts):
        raise ValueError(f"{field_name} must not contain empty, current, or parent path parts")
    return path


def resolve_under_root(root: Path, relative_path: str) -> Path:
    clean = validate_run_relative_path(relative_path, "artifact path")
    resolved_root = root.expanduser().resolve()
    resolved = (resolved_root / clean).resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise ValueError(f"Path escaped run root: {relative_path}")
    return resolved


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def read_json_value(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object row in {path}")
            rows.append(payload)
    return rows


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True)
