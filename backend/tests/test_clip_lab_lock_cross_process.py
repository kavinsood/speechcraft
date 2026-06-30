from __future__ import annotations

import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
BACKEND_ROOT = ROOT_DIR / "backend"
WORKER_ROOT = ROOT_DIR / "workers" / "dataset"
WORKER_PYTHON = Path(os.environ.get("SPEECHCRAFT_WORKER_PYTHON", WORKER_ROOT / ".venv" / "bin" / "python"))
if not WORKER_PYTHON.exists():
    WORKER_PYTHON = Path(sys.executable)


def _hold_backend_lock(run_root: str, ready: multiprocessing.synchronize.Event) -> None:
    from app.clip_lab_state import clip_lab_run_lock

    with clip_lab_run_lock(Path(run_root)):
        ready.set()
        time.sleep(8)


def _worker_try_lock_script() -> str:
    return (
        "import sys\n"
        "from pathlib import Path\n"
        f"sys.path.insert(0, {repr(str(WORKER_ROOT))})\n"
        "from speechcraft_dataset.clip_lab_coordination import clip_lab_run_lock\n"
        "run_root = Path(sys.argv[1])\n"
        "ready_path = run_root / '.worker_lock_ready'\n"
        "try:\n"
        "    with clip_lab_run_lock(run_root, timeout=0):\n"
        "        ready_path.write_text('ready', encoding='utf-8')\n"
        "        import time\n"
        "        time.sleep(8)\n"
        "        sys.exit(0)\n"
        "except TimeoutError:\n"
        "    sys.exit(2)\n"
    )


def _wait_for_ready(path: Path, *, timeout_sec: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for readiness signal: {path}")


class ClipLabCrossProcessLockTests(unittest.TestCase):
    def test_backend_lock_blocks_worker_lock(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = str(Path(temp_dir_raw))
            ready = ctx.Event()
            proc = ctx.Process(target=_hold_backend_lock, args=(run_root, ready))
            proc.start()
            try:
                self.assertTrue(ready.wait(timeout=5))
                result = subprocess.run(
                    [str(WORKER_PYTHON), "-c", _worker_try_lock_script(), run_root],
                    cwd=str(WORKER_ROOT),
                    env={**os.environ, "PYTHONPATH": str(WORKER_ROOT)},
                    check=False,
                )
                self.assertEqual(result.returncode, 2)
            finally:
                proc.terminate()
                proc.join(timeout=3)

    def test_worker_lock_blocks_backend_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            ready_path = run_root / ".worker_lock_ready"
            proc = subprocess.Popen(
                [
                    str(WORKER_PYTHON),
                    "-c",
                    _worker_try_lock_script(),
                    str(run_root),
                ],
                cwd=str(WORKER_ROOT),
                env={**os.environ, "PYTHONPATH": str(WORKER_ROOT)},
            )
            try:
                _wait_for_ready(ready_path)
                from app.clip_lab_state import ClipLabStateBusyError, assert_clip_lab_run_available

                with self.assertRaises(ClipLabStateBusyError):
                    assert_clip_lab_run_available(run_root)
            finally:
                proc.terminate()
                proc.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
