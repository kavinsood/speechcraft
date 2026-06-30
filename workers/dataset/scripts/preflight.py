#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
WORKER_ROOT = SCRIPT_DIR.parent
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))


DEFAULT_MFA_DICTIONARY = "english_us_mfa"
DEFAULT_MFA_ACOUSTIC_MODEL = "english_mfa"


def module_check(module_name: str, import_name: str | None = None) -> dict[str, Any]:
    target = import_name or module_name
    try:
        module = importlib.import_module(target)
        return {
            "name": module_name,
            "ok": True,
            "version": getattr(module, "__version__", None),
            "error": None,
        }
    except Exception as exc:
        return {
            "name": module_name,
            "ok": False,
            "version": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def run_command(command: list[str], env: dict[str, str] | None = None) -> dict[str, Any]:
    try:
        completed = subprocess.run(command, text=True, capture_output=True, timeout=30, env=env)
        return {
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
        }


def torch_check() -> dict[str, Any]:
    result = module_check("torch")
    if not result["ok"]:
        return result
    try:
        import torch

        cuda_available = bool(torch.cuda.is_available())
        probe_error = None
        gpu_name = None
        if cuda_available:
            gpu_name = torch.cuda.get_device_name(0)
            _ = torch.randn(1, device="cuda")
        return {
            **result,
            "cuda_available": cuda_available,
            "torch_cuda": getattr(torch.version, "cuda", None),
            "gpu_name": gpu_name,
            "cuda_probe_ok": cuda_available,
            "cuda_probe_error": probe_error,
        }
    except Exception as exc:
        return {
            **result,
            "cuda_available": False,
            "torch_cuda": None,
            "gpu_name": None,
            "cuda_probe_ok": False,
            "cuda_probe_error": f"{type(exc).__name__}: {exc}",
        }


def ffmpeg_check() -> dict[str, Any]:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        return {"ok": False, "path": None, "version": None, "error": "ffmpeg not found on PATH"}
    version = run_command([ffmpeg, "-version"])
    return {
        "ok": version["ok"],
        "path": ffmpeg,
        "version": version["stdout"].splitlines()[0] if version["stdout"] else None,
        "error": None if version["ok"] else version["stderr"],
    }


def mfa_check(binary: str | None, dictionary: str, acoustic_model: str, root_dir: str | None) -> dict[str, Any]:
    mfa = binary or shutil.which("mfa")
    if not mfa:
        return {
            "ok": False,
            "binary": None,
            "version": None,
            "dictionary": dictionary,
            "acoustic_model": acoustic_model,
            "checks": [],
            "error": "MFA binary not configured and not found on PATH",
        }

    mfa_env = dict(os.environ)
    bin_dir = str(Path(mfa).resolve().parent)
    path_entries = [entry for entry in mfa_env.get("PATH", "").split(os.pathsep) if entry]
    if bin_dir not in path_entries:
        mfa_env["PATH"] = os.pathsep.join([bin_dir, *path_entries])
    if root_dir:
        mfa_root = Path(root_dir)
        mfa_root.mkdir(parents=True, exist_ok=True)
        mfa_env["MFA_ROOT_DIR"] = str(mfa_root)
    mfa_env.setdefault("TMPDIR", "/tmp")

    fstcompile = shutil.which("fstcompile", path=mfa_env["PATH"])
    checks = [
        {
            "name": "openfst_fstcompile",
            "ok": fstcompile is not None,
            "returncode": 0 if fstcompile else 1,
            "stdout": fstcompile or "",
            "stderr": "" if fstcompile else "fstcompile not found on MFA runtime PATH",
        },
        {"name": "version", **run_command([mfa, "version"], env=mfa_env)},
    ]
    checks.append(
        {
            "name": "dictionary",
            **run_command([mfa, "model", "inspect", "dictionary", dictionary], env=mfa_env),
        }
    )
    checks.append(
        {
            "name": "acoustic_model",
            **run_command([mfa, "model", "inspect", "acoustic", acoustic_model], env=mfa_env),
        }
    )
    version_check = checks[1]
    return {
        "ok": all(check["ok"] for check in checks),
        "binary": mfa,
        "mfa_root_dir": mfa_env.get("MFA_ROOT_DIR"),
        "fstcompile": fstcompile,
        "version": version_check["stdout"].splitlines()[0] if version_check["stdout"] else None,
        "dictionary": dictionary,
        "acoustic_model": acoustic_model,
        "checks": checks,
        "error": None if all(check["ok"] for check in checks) else "One or more MFA checks failed",
    }


def artifact_root_check(path: str | None) -> dict[str, Any]:
    if not path:
        return {"ok": True, "path": None, "error": None}
    root = Path(path)
    try:
        root.mkdir(parents=True, exist_ok=True)
        probe = root / ".speechcraft_preflight_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return {"ok": True, "path": str(root), "error": None}
    except Exception as exc:
        return {"ok": False, "path": str(root), "error": f"{type(exc).__name__}: {exc}"}


def asr_model_check(args: argparse.Namespace) -> dict[str, Any]:
    try:
        from speechcraft_dataset.models import check_asr_model

        return check_asr_model(
            model=args.asr_model,
            model_path=args.asr_model_path,
            cache_dir=args.asr_cache_dir,
            local_only=True,
            load_model=True,
            device=args.asr_device,
            compute_type=args.asr_compute_type,
            timeout_seconds=args.asr_model_timeout_seconds,
        )
    except Exception as exc:
        return {
            "ok": False,
            "model": args.asr_model,
            "model_path": args.asr_model_path,
            "local_only": True,
            "load_checked": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    modules = [
        torch_check(),
        module_check("torchaudio"),
        module_check("nemo", "nemo"),
        module_check("faster_whisper"),
        module_check("ctranslate2"),
        module_check("silero_vad"),
        module_check("soundfile"),
        module_check("praatio"),
        module_check("numpy"),
        module_check("scipy"),
    ]
    report = {
        "python": {
            "executable": sys.executable,
            "version": platform.python_version(),
            "platform": platform.platform(),
        },
        "modules": modules,
        "ffmpeg": ffmpeg_check(),
        "asr_model": asr_model_check(args),
        "mfa": mfa_check(args.mfa_bin, args.mfa_dictionary, args.mfa_acoustic_model, args.mfa_root_dir),
        "artifact_root": artifact_root_check(args.artifact_root),
    }
    report["ok"] = (
        all(module["ok"] for module in modules)
        and bool(report["ffmpeg"]["ok"])
        and bool(report["asr_model"]["ok"])
        and bool(report["mfa"]["ok"])
        and bool(report["artifact_root"]["ok"])
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="SpeechCraft dataset worker preflight")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--asr-model", default=os.environ.get("SPEECHCRAFT_ASR_MODEL", "small.en"))
    parser.add_argument("--asr-model-path", default=os.environ.get("SPEECHCRAFT_ASR_MODEL_PATH"))
    parser.add_argument("--asr-cache-dir", default=os.environ.get("SPEECHCRAFT_ASR_CACHE_DIR"))
    parser.add_argument("--asr-device", default=os.environ.get("SPEECHCRAFT_ASR_DEVICE", "cpu"))
    parser.add_argument("--asr-compute-type", default=os.environ.get("SPEECHCRAFT_ASR_COMPUTE_TYPE", "int8"))
    parser.add_argument("--asr-model-timeout-seconds", type=int, default=120)
    parser.add_argument("--check-asr-model-load", action="store_true")
    parser.add_argument("--mfa-bin", default=os.environ.get("SPEECHCRAFT_MFA_BIN"))
    parser.add_argument("--mfa-root-dir", default=os.environ.get("SPEECHCRAFT_MFA_ROOT_DIR"))
    parser.add_argument("--mfa-dictionary", default=os.environ.get("SPEECHCRAFT_MFA_DICTIONARY", DEFAULT_MFA_DICTIONARY))
    parser.add_argument(
        "--mfa-acoustic-model",
        default=os.environ.get("SPEECHCRAFT_MFA_ACOUSTIC_MODEL", DEFAULT_MFA_ACOUSTIC_MODEL),
    )
    args = parser.parse_args()
    report = build_report(args)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("ok" if report["ok"] else "failed")
        print(json.dumps(report, indent=2, sort_keys=True))
    raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
