from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import httpx


BACKEND_ROOT = Path(__file__).resolve().parents[1]
SERVER_STARTUP_TIMEOUT_SECONDS = 20


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class LiveServerTestCase(unittest.TestCase):
    @classmethod
    def _read_server_log(cls) -> str:
        if getattr(cls, "_server_log_path", None) is None:
            return ""
        try:
            if getattr(cls, "_server_log_handle", None) is not None:
                cls._server_log_handle.flush()
            return cls._server_log_path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return ""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tempdir = tempfile.TemporaryDirectory()
        root = Path(cls._tempdir.name)
        cls._port = _find_free_port()
        cls.base_url = f"http://127.0.0.1:{cls._port}"
        cls._server_log_path = root / "uvicorn.log"
        cls._server_log_handle = cls._server_log_path.open("w+", encoding="utf-8", buffering=1)

        env = os.environ.copy()
        env["PYTHONPATH"] = str(BACKEND_ROOT)
        env["SPEECHCRAFT_DB_PATH"] = str(root / "data" / "project.db")
        env["SPEECHCRAFT_LEGACY_SEED_PATH"] = str(root / "data" / "missing-phase1-demo.json")
        env["SPEECHCRAFT_MEDIA_ROOT"] = str(root / "data" / "media")
        env["SPEECHCRAFT_EXPORTS_ROOT"] = str(root / "exports")

        cls._server = subprocess.Popen(
            [
                sys.executable,
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
            stdout=cls._server_log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

        deadline = time.time() + SERVER_STARTUP_TIMEOUT_SECONDS
        last_error: Exception | None = None
        while time.time() < deadline:
            if cls._server.poll() is not None:
                raise RuntimeError(
                    "uvicorn exited during test startup:\n"
                    f"{cls._read_server_log() or '<no server output>'}"
                )
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
        raise RuntimeError(
            "Timed out waiting for test server startup: "
            f"{last_error}\n{cls._read_server_log() or '<no server output>'}"
        )

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
            if getattr(cls, "_server_log_handle", None) is not None:
                cls._server_log_handle.close()
            if getattr(cls, "_tempdir", None) is not None:
                cls._tempdir.cleanup()
        super().tearDownClass()

    def setUp(self) -> None:
        self.client = httpx.Client(base_url=self.base_url, timeout=5.0)

    def tearDown(self) -> None:
        self.client.close()
