from __future__ import annotations

import argparse
import socket
import threading
import time
from contextlib import contextmanager
from os import getpid

from .repository import DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS, SQLiteRepository, repository

DEFAULT_PROCESSING_JOB_HEARTBEAT_INTERVAL_SECONDS = 5.0


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{getpid()}"


def process_next_job(
    repo: SQLiteRepository,
    worker_id: str,
    *,
    heartbeat_interval_seconds: float = DEFAULT_PROCESSING_JOB_HEARTBEAT_INTERVAL_SECONDS,
    stale_after_seconds: float = DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS,
) -> bool:
    claimed_job = repo.claim_next_processing_job(worker_id, stale_after_seconds=stale_after_seconds)
    if claimed_job is None:
        return False
    with processing_job_heartbeat(
        repo,
        claimed_job.id,
        worker_id=worker_id,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    ):
        repo.run_claimed_processing_job(claimed_job.id, worker_id=worker_id)
    return True


@contextmanager
def processing_job_heartbeat(
    repo: SQLiteRepository,
    job_id: str,
    *,
    worker_id: str,
    heartbeat_interval_seconds: float,
):
    stop_event = threading.Event()

    def heartbeat_loop() -> None:
        interval = max(heartbeat_interval_seconds, 0.1)
        while not stop_event.wait(interval):
            try:
                repo.heartbeat_processing_job(job_id, worker_id=worker_id)
            except Exception:
                return

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        name=f"processing-job-heartbeat-{job_id}",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        yield
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=max(heartbeat_interval_seconds, 0.1) + 1.0)


def run_worker_loop(
    repo: SQLiteRepository,
    *,
    worker_id: str,
    poll_interval_seconds: float,
    heartbeat_interval_seconds: float,
    stale_after_seconds: float,
    run_once: bool,
) -> None:
    # Dev usage: run `python -m app.worker` in a separate process from the API server.
    while True:
        processed_job = process_next_job(
            repo,
            worker_id,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            stale_after_seconds=stale_after_seconds,
        )
        if run_once:
            return
        if not processed_job:
            time.sleep(max(poll_interval_seconds, 0.1))


def main() -> None:
    parser = argparse.ArgumentParser(description="Speechcraft ProcessingJob worker")
    parser.add_argument("--once", action="store_true", help="Process at most one pending job and exit")
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Idle poll interval when no pending jobs are available",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds",
        type=float,
        default=DEFAULT_PROCESSING_JOB_HEARTBEAT_INTERVAL_SECONDS,
        help="Heartbeat interval while a claimed job is executing",
    )
    parser.add_argument(
        "--stale-after-seconds",
        type=float,
        default=DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS,
        help="Fail RUNNING jobs whose heartbeat is older than this timeout before claiming new work",
    )
    parser.add_argument(
        "--worker-id",
        default=default_worker_id(),
        help="Explicit worker identifier stored on claimed jobs",
    )
    args = parser.parse_args()
    run_worker_loop(
        repository,
        worker_id=args.worker_id,
        poll_interval_seconds=args.poll_interval_seconds,
        heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        stale_after_seconds=args.stale_after_seconds,
        run_once=args.once,
    )


if __name__ == "__main__":
    main()
