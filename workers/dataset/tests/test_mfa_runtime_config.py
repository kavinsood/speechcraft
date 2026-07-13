from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from speechcraft_dataset import mfa


class _Completed:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class MfaRuntimeConfigTests(unittest.TestCase):
    def test_runtime_env_uses_shared_model_root_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            model_root = Path(tmp) / "shared_models"
            env = mfa.mfa_runtime_env(
                {"mfa_model_root_dir": str(model_root)},
                binary="/bin/true",
            )
            self.assertEqual(env.get("MFA_ROOT_DIR"), str(model_root.resolve()))

    def test_run_mfa_command_uses_temporary_directory_and_shared_models(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_root = Path(tmp)
            artifacts = run_root / "artifacts"
            (artifacts / "mfa_corpus").mkdir(parents=True)
            (artifacts / "mfa_corpus_manifest.json").write_text("{}", encoding="utf-8")
            shared_model_root = run_root / "shared_models"
            config = {
                "config_hash": "sha256:test",
                "mfa_bin": "/bin/true",
                "mfa_model_root_dir": str(shared_model_root),
                "mfa_dictionary": "english_us_mfa",
                "mfa_acoustic_model": "english_mfa",
                "mfa_single_speaker": True,
            }
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_run(command, **kwargs):  # type: ignore[no-untyped-def]
                calls.append((list(command), kwargs.get("env")))
                if command[1:4] == ["model", "inspect", "dictionary"] or command[1:4] == ["model", "inspect", "acoustic"]:
                    return _Completed(returncode=0, stdout="ok")
                if command[1] == "align":
                    return _Completed(returncode=0, stdout="aligned")
                raise AssertionError(f"Unexpected command: {command}")

            with patch("speechcraft_dataset.mfa.subprocess.run", side_effect=fake_run):
                summary = mfa.run_mfa_command(run_root, config)

            self.assertEqual(summary["status"], "ok")
            align_command = [command for command, _ in calls if len(command) > 1 and command[1] == "align"][0]
            self.assertIn("--temporary_directory", align_command)
            temp_index = align_command.index("--temporary_directory") + 1
            self.assertEqual(align_command[temp_index], str(run_root / "artifacts" / "mfa_runtime"))
            align_env = [env for command, env in calls if len(command) > 1 and command[1] == "align"][0]
            assert align_env is not None
            self.assertEqual(align_env.get("MFA_ROOT_DIR"), str(shared_model_root.resolve()))
            self.assertEqual(summary["mfa_model_root_dir"], str(shared_model_root.resolve()))
            self.assertEqual(summary["mfa_temp_dir"], str(run_root / "artifacts" / "mfa_runtime"))

    def test_run_mfa_command_fails_cleanly_when_models_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_root = Path(tmp)
            artifacts = run_root / "artifacts"
            (artifacts / "mfa_corpus").mkdir(parents=True)
            (artifacts / "mfa_corpus_manifest.json").write_text("{}", encoding="utf-8")
            config = {
                "config_hash": "sha256:test",
                "mfa_bin": "/bin/true",
                "mfa_model_root_dir": str(run_root / "shared_models"),
                "mfa_dictionary": "english_us_mfa",
                "mfa_acoustic_model": "english_mfa",
                "mfa_single_speaker": True,
            }

            def fake_run(command, **kwargs):  # type: ignore[no-untyped-def]
                if command[1:4] == ["model", "inspect", "dictionary"] or command[1:4] == ["model", "inspect", "acoustic"]:
                    return _Completed(returncode=1, stderr="missing model")
                raise AssertionError(f"Unexpected command: {command}")

            with patch("speechcraft_dataset.mfa.subprocess.run", side_effect=fake_run):
                with self.assertRaises(RuntimeError):
                    mfa.run_mfa_command(run_root, config)

            summary = json.loads((artifacts / "mfa_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["reason_codes"], ["mfa_model_setup_failed"])


if __name__ == "__main__":
    unittest.main()
