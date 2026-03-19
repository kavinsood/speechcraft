#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


@dataclass
class SmokeConfig:
    base_url: str
    project_id: str
    timeout_seconds: float


def request_json(config: SmokeConfig, path: str) -> Any:
    url = f"{config.base_url.rstrip('/')}{path}"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=config.timeout_seconds) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def log(step: str, detail: str) -> None:
    print(f"[smoke] {step}: {detail}")


def run_smoke(config: SmokeConfig) -> int:
    try:
        health = request_json(config, "/healthz")
        log("healthz", json.dumps(health))

        projects = request_json(config, "/api/projects")
        log("projects", f"{len(projects)} project(s)")

        project = request_json(config, f"/api/projects/{quote(config.project_id)}")
        log("project", project.get("name", config.project_id))

        slices = request_json(config, f"/api/projects/{quote(config.project_id)}/slices")
        log("slices", f"{len(slices)} slice(s)")

        if slices:
            slice_id = slices[0]["id"]
            detail = request_json(config, f"/api/slices/{quote(slice_id)}")
            log("slice-detail", detail["id"])

            peaks = request_json(config, f"/api/clips/{quote(slice_id)}/waveform-peaks?bins=48")
            log("waveform-peaks", f"{peaks['bins']} bins for {slice_id}")

        preview = request_json(config, f"/api/projects/{quote(config.project_id)}/export-preview")
        log("export-preview", f"{preview['accepted_slice_count']} accepted slice(s)")

        exports = request_json(config, f"/api/projects/{quote(config.project_id)}/exports")
        log("export-runs", f"{len(exports)} run(s)")
        return 0
    except HTTPError as exc:
        print(f"[smoke] HTTP error {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"[smoke] Connection error: {exc.reason}", file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover - smoke script fallback
        print(f"[smoke] Unexpected failure: {exc}", file=sys.stderr)
        return 1


def parse_args() -> SmokeConfig:
    parser = argparse.ArgumentParser(
        description="Run non-destructive backend smoke checks against a running Speechcraft server."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL for the running backend server.",
    )
    parser.add_argument(
        "--project-id",
        default="phase1-demo",
        help="Project to inspect during the smoke run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds.",
    )
    args = parser.parse_args()
    return SmokeConfig(
        base_url=args.base_url,
        project_id=args.project_id,
        timeout_seconds=args.timeout_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(run_smoke(parse_args()))
