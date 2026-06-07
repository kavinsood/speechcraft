import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from app.dataset_worker_client import run_dataset_worker_preflight


class DatasetWorkerClientTests(TestCase):
    def test_preflight_reports_missing_worker_python_without_importing_worker(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            worker_root = Path(temp_dir_raw)
            (worker_root / "scripts").mkdir()
            (worker_root / "scripts" / "preflight.py").write_text("print('{}')", encoding="utf-8")

            with patch.dict("os.environ", {"SPEECHCRAFT_DATASET_WORKER_ROOT": str(worker_root)}, clear=False):
                result = run_dataset_worker_preflight()

        self.assertFalse(result["ok"])
        self.assertIn("Dataset worker python not found", result["error"])
        self.assertEqual(result["worker_root"], str(worker_root.resolve()))

    def test_preflight_passes_artifact_root_and_parses_worker_json(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            worker_root = Path(temp_dir_raw)
            worker_python = worker_root / ".venv" / "bin" / "python"
            preflight = worker_root / "scripts" / "preflight.py"
            worker_python.parent.mkdir(parents=True)
            preflight.parent.mkdir(parents=True)
            worker_python.write_text("#!/bin/sh\n", encoding="utf-8")
            preflight.write_text("#!/usr/bin/env python\n", encoding="utf-8")

            completed = subprocess.CompletedProcess(
                [str(worker_python), str(preflight), "--json", "--artifact-root", "runs/run-1"],
                0,
                stdout=json.dumps({"ok": True, "python": {"version": "3.11.0"}}),
                stderr="",
            )

            with patch.dict("os.environ", {"SPEECHCRAFT_DATASET_WORKER_ROOT": str(worker_root)}, clear=False):
                with patch("app.dataset_worker_client.subprocess.run", return_value=completed) as run:
                    result = run_dataset_worker_preflight(artifact_root="runs/run-1")

        self.assertTrue(result["ok"])
        self.assertEqual(result["returncode"], 0)
        self.assertEqual(result["python"]["version"], "3.11.0")
        self.assertEqual(
            run.call_args.args[0],
            [str(worker_python.resolve()), str(preflight.resolve()), "--json", "--artifact-root", "runs/run-1"],
        )
        self.assertEqual(run.call_args.kwargs["cwd"], str(worker_root.resolve()))

    def test_preflight_invalid_json_is_reported_as_failure(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            worker_root = Path(temp_dir_raw)
            worker_python = worker_root / ".venv" / "bin" / "python"
            preflight = worker_root / "scripts" / "preflight.py"
            worker_python.parent.mkdir(parents=True)
            preflight.parent.mkdir(parents=True)
            worker_python.write_text("#!/bin/sh\n", encoding="utf-8")
            preflight.write_text("#!/usr/bin/env python\n", encoding="utf-8")
            completed = subprocess.CompletedProcess([str(worker_python), str(preflight), "--json"], 1, stdout="nope")

            with patch.dict("os.environ", {"SPEECHCRAFT_DATASET_WORKER_ROOT": str(worker_root)}, clear=False):
                with patch("app.dataset_worker_client.subprocess.run", return_value=completed):
                    result = run_dataset_worker_preflight()

        self.assertFalse(result["ok"])
        self.assertIn("invalid JSON", result["error"])
        self.assertEqual(result["reason_codes"], ["preflight_invalid_json"])
        self.assertEqual(result["stdout_tail"], "nope")

    def test_preflight_timeout_is_reported_as_structured_failure(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            worker_root = Path(temp_dir_raw)
            worker_python = worker_root / ".venv" / "bin" / "python"
            preflight = worker_root / "scripts" / "preflight.py"
            worker_python.parent.mkdir(parents=True)
            preflight.parent.mkdir(parents=True)
            worker_python.write_text("#!/bin/sh\n", encoding="utf-8")
            preflight.write_text("#!/usr/bin/env python\n", encoding="utf-8")
            timeout = subprocess.TimeoutExpired([str(worker_python), str(preflight), "--json"], 180)
            timeout.stdout = b"partial stdout"
            timeout.stderr = b"partial stderr"

            with patch.dict("os.environ", {"SPEECHCRAFT_DATASET_WORKER_ROOT": str(worker_root)}, clear=False):
                with patch("app.dataset_worker_client.subprocess.run", side_effect=timeout):
                    result = run_dataset_worker_preflight()

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason_codes"], ["preflight_timeout"])
        self.assertEqual(result["stdout_tail"], "partial stdout")
        self.assertEqual(result["stderr_tail"], "partial stderr")
