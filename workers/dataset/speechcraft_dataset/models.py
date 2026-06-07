from __future__ import annotations

import argparse
import gc
import json
import signal
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


FASTER_WHISPER_REPOS = {
    "tiny": "Systran/faster-whisper-tiny",
    "tiny.en": "Systran/faster-whisper-tiny.en",
    "base": "Systran/faster-whisper-base",
    "base.en": "Systran/faster-whisper-base.en",
    "small": "Systran/faster-whisper-small",
    "small.en": "Systran/faster-whisper-small.en",
    "medium": "Systran/faster-whisper-medium",
    "medium.en": "Systran/faster-whisper-medium.en",
    "large-v1": "Systran/faster-whisper-large-v1",
    "large-v2": "Systran/faster-whisper-large-v2",
    "large-v3": "Systran/faster-whisper-large-v3",
}


class TimeoutError(RuntimeError):
    pass


@contextmanager
def timeout_after(seconds: int | float | None, label: str) -> Iterator[None]:
    if not seconds or seconds <= 0:
        yield
        return
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def _handle_timeout(_signum: int, _frame: object) -> None:
        raise TimeoutError(f"{label} timed out after {seconds} seconds")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, float(seconds))
    signal.signal(signal.SIGALRM, _handle_timeout)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def faster_whisper_repo_id(model_name: str) -> str:
    return FASTER_WHISPER_REPOS.get(model_name, model_name)


def resolve_asr_model_reference(config: dict[str, Any]) -> str:
    model_path = str(config.get("faster_whisper_model_path") or "").strip()
    if model_path:
        path = Path(model_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"faster_whisper_model_path does not exist: {path}")
        return str(path.resolve())

    model_name = str(config.get("faster_whisper_model") or "small.en")
    cache_dir = str(config.get("faster_whisper_cache_dir") or "").strip() or None
    local_only = bool(config.get("asr_local_files_only", True))
    repo_id = faster_whisper_repo_id(model_name)
    try:
        from huggingface_hub import snapshot_download

        snapshot_path = snapshot_download(
            repo_id=repo_id,
            cache_dir=cache_dir,
            local_files_only=local_only,
        )
        return str(Path(snapshot_path).resolve())
    except Exception as exc:
        if local_only:
            raise FileNotFoundError(
                f"ASR model {model_name!r} is not available locally ({repo_id}). "
                f"Download it once or set faster_whisper_model_path. "
                f"Original error: {type(exc).__name__}: {exc}"
            ) from exc
        return model_name


def check_asr_model(
    *,
    model: str,
    model_path: str | None = None,
    cache_dir: str | None = None,
    local_only: bool = True,
    load_model: bool = False,
    device: str = "cpu",
    compute_type: str = "int8",
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    resolved_path = None
    snapshot_path = None
    reference = model_path or model
    if model_path:
        path = Path(model_path).expanduser()
        exists = path.exists()
        resolved_path = str(path.resolve()) if exists else str(path)
        result: dict[str, Any] = {
            "ok": exists,
            "model": model,
            "model_path": resolved_path,
            "reference": resolved_path,
            "source": "local_path",
            "snapshot_path": resolved_path if exists else None,
            "local_only": local_only,
            "load_checked": False,
            "error": None if exists else f"ASR model path does not exist: {path}",
        }
    else:
        repo_id = faster_whisper_repo_id(model)
        try:
            from huggingface_hub import snapshot_download

            with timeout_after(timeout_seconds, "ASR model snapshot check"):
                snapshot_path = snapshot_download(
                    repo_id=repo_id,
                    cache_dir=cache_dir,
                    local_files_only=local_only,
                )
            result = {
                "ok": True,
                "model": model,
                "repo_id": repo_id,
                "reference": reference,
                "source": "huggingface_snapshot",
                "snapshot_path": snapshot_path,
                "local_only": local_only,
                "load_checked": False,
                "error": None,
            }
        except Exception as exc:
            result = {
                "ok": False,
                "model": model,
                "repo_id": repo_id,
                "reference": reference,
                "source": "huggingface_snapshot",
                "snapshot_path": None,
                "local_only": local_only,
                "load_checked": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
    if not result["ok"] or not load_model:
        return result

    try:
        import torch
        from faster_whisper import WhisperModel

        model_ref = str(result["snapshot_path"] or result["reference"])
        loaded_model = None
        try:
            with timeout_after(timeout_seconds, "ASR model load"):
                loaded_model = WhisperModel(model_ref, device=device, compute_type=compute_type)
            result.update({"ok": True, "load_checked": True, "device": device, "compute_type": compute_type})
        finally:
            if loaded_model is not None:
                del loaded_model
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
    except Exception as exc:
        result.update(
            {
                "ok": False,
                "load_checked": True,
                "device": device,
                "compute_type": compute_type,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SpeechCraft dataset worker model utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text, local_only in (
        ("check-asr", "Check that a faster-whisper model is available locally", True),
        ("download-asr", "Download/cache a faster-whisper model", False),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.set_defaults(local_only=local_only)
        command.add_argument("--json", action="store_true")
        command.add_argument("--model", default="small.en")
        command.add_argument("--model-path", default=None)
        command.add_argument("--cache-dir", default=None)
        command.add_argument("--load-model", action="store_true")
        command.add_argument("--device", default="cpu")
        command.add_argument("--compute-type", default="int8")
        command.add_argument("--timeout-seconds", type=int, default=120)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_asr_model(
        model=args.model,
        model_path=args.model_path,
        cache_dir=args.cache_dir,
        local_only=args.local_only,
        load_model=args.load_model,
        device=args.device,
        compute_type=args.compute_type,
        timeout_seconds=args.timeout_seconds,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("ok" if result["ok"] else "failed")
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
