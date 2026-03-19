from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import httpx


BACKEND_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = BACKEND_ROOT / ".venv" / "bin" / "python"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class LiveServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tempdir = tempfile.TemporaryDirectory()
        root = Path(cls._tempdir.name)
        cls._port = _find_free_port()
        cls.base_url = f"http://127.0.0.1:{cls._port}"

        env = os.environ.copy()
        env["PYTHONPATH"] = str(BACKEND_ROOT)
        env["SPEECHCRAFT_DB_PATH"] = str(root / "data" / "project.db")
        env["SPEECHCRAFT_LEGACY_SEED_PATH"] = str(root / "data" / "missing-phase1-demo.json")
        env["SPEECHCRAFT_MEDIA_ROOT"] = str(root / "data" / "media")
        env["SPEECHCRAFT_EXPORTS_ROOT"] = str(root / "exports")

        cls._server = subprocess.Popen(
            [
                str(PYTHON_BIN),
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(cls._port),
            ],
            cwd=BACKEND_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.time() + 20
        last_error: Exception | None = None
        while time.time() < deadline:
            if cls._server.poll() is not None:
                stderr = cls._server.stderr.read() if cls._server.stderr is not None else ""
                raise RuntimeError(f"uvicorn exited during test startup:\n{stderr}")
            try:
                with httpx.Client(base_url=cls.base_url, timeout=1.0) as client:
                    response = client.get("/healthz")
                if response.status_code == 200:
                    return
            except Exception as exc:  # pragma: no cover - startup timing
                last_error = exc
                time.sleep(0.2)

        cls._server.terminate()
        try:
            cls._server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls._server.kill()
            cls._server.wait(timeout=5)
        raise RuntimeError(f"Timed out waiting for test server startup: {last_error}")

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if getattr(cls, "_server", None) is not None and cls._server.poll() is None:
                cls._server.terminate()
                try:
                    cls._server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cls._server.kill()
                    cls._server.wait(timeout=5)
        finally:
            if getattr(cls, "_tempdir", None) is not None:
                cls._tempdir.cleanup()
        super().tearDownClass()

    def setUp(self) -> None:
        self.client = httpx.Client(base_url=self.base_url, timeout=5.0)

    def tearDown(self) -> None:
        self.client.close()
