"""Detached worker: runs the existing extraction pipeline for one task_id and
writes progress/result into the task store. Spawned by submit_transcript via
`python -m social_post_extractor_mcp.worker <task_id>`."""
from __future__ import annotations

import contextlib
import os
import signal
import sys
import traceback
from pathlib import Path

# MUST load env files before importing extraction code: social_extractor reads
# os.getenv directly; the env-file load normally happens at server.py import
# time, which this module does NOT import.
from .env_loader import load_default_env_files

load_default_env_files(Path(__file__).resolve().parents[1])

from . import task_store as ts
from .social_extractor import (  # noqa: E402
    SocialExtractorService,
    extract_youtube_transcript_value,
)

_SOCIAL = {"douyin", "xiaohongshu", "bilibili"}

_PROXY_KEYS = (
    "HTTP_PROXY", "http_proxy",
    "HTTPS_PROXY", "https_proxy",
    "ALL_PROXY", "all_proxy",
)


class _WorkerTimeout(Exception):
    """Raised by SIGALRM handler when the worker wall-clock limit is exceeded."""


def _alarm_handler(signum: int, frame: object) -> None:  # noqa: ARG001
    raise _WorkerTimeout()


@contextlib.contextmanager
def _china_no_proxy():
    """Context manager: strip proxy env vars for the duration of a China-platform
    extraction call, then restore the original environment on exit."""
    saved = {}
    for key in _PROXY_KEYS:
        val = os.environ.pop(key, None)
        if val is not None:
            saved[key] = val

    # Capture prior NO_PROXY / no_proxy so we can restore exactly.
    prior_no_proxy = os.environ.get("NO_PROXY")
    prior_no_proxy_lc = os.environ.get("no_proxy")
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"

    try:
        yield
    finally:
        # Restore proxy vars.
        for key, val in saved.items():
            os.environ[key] = val

        # Restore NO_PROXY.
        if prior_no_proxy is None:
            os.environ.pop("NO_PROXY", None)
        else:
            os.environ["NO_PROXY"] = prior_no_proxy

        if prior_no_proxy_lc is None:
            os.environ.pop("no_proxy", None)
        else:
            os.environ["no_proxy"] = prior_no_proxy_lc


def run(task_id: str) -> None:
    ts.write_task(task_id, status="running", pid=os.getpid(),
                  stage="extracting", progress=0.1)
    task = ts.read_task(task_id) or {}
    url = task.get("url", "")
    platform = task.get("platform", "")
    asr_model = task.get("asr_model")

    max_seconds = int(os.environ.get("WORKER_MAX_SECONDS") or 1800)
    signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(max_seconds)

    try:
        try:
            if platform in _SOCIAL:
                with _china_no_proxy():
                    result = SocialExtractorService().extract_social_post(
                        url, asr_model=asr_model
                    )
                transcript = result.get("raw_transcript") or result.get("script_preview") or ""
                metadata = {
                    "platform": result.get("platform"),
                    "content_type": result.get("content_type"),
                    "post_id": result.get("post_id"),
                    "script_path": result.get("script_path"),
                    "info_path": result.get("info_path"),
                    "info": result.get("info"),
                }
            else:
                result = extract_youtube_transcript_value(url, asr_model=asr_model)
                transcript = result.get("transcript", "")
                metadata = {k: v for k, v in result.items() if k != "transcript"}
            ts.write_task(task_id, status="succeeded", stage="done",
                          progress=1.0, transcript=transcript, metadata=metadata)
        except _WorkerTimeout:
            ts.log_path(task_id).parent.mkdir(parents=True, exist_ok=True)
            with open(ts.log_path(task_id), "a", encoding="utf-8") as _lf:
                _lf.write("\n--- worker watchdog timeout ---\n")
                _lf.write(traceback.format_exc())
            ts.write_task(
                task_id, status="failed", stage="done",
                error=(
                    f"worker exceeded WORKER_MAX_SECONDS ({max_seconds}s)"
                    " — likely a hung network call; see log_path"
                ),
                log_path=str(ts.log_path(task_id)),
            )
        except Exception as exc:  # noqa: BLE001 - worker boundary, record & exit
            ts.log_path(task_id).parent.mkdir(parents=True, exist_ok=True)
            with open(ts.log_path(task_id), "a", encoding="utf-8") as _lf:
                _lf.write("\n--- worker exception ---\n")
                _lf.write(traceback.format_exc())
            ts.write_task(task_id, status="failed", stage="done",
                          error=str(exc)[:2000], log_path=str(ts.log_path(task_id)))
    finally:
        signal.alarm(0)


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: python -m social_post_extractor_mcp.worker <task_id>",
              file=sys.stderr)
        return 2
    run(args[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
