"""Detached worker: runs the existing extraction pipeline for one task_id and
writes progress/result into the task store. Spawned by submit_transcript via
`python -m social_post_extractor_mcp.worker <task_id>`."""
from __future__ import annotations

import os
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


def run(task_id: str) -> None:
    ts.write_task(task_id, status="running", pid=os.getpid(),
                  stage="extracting", progress=0.1)
    task = ts.read_task(task_id) or {}
    url = task.get("url", "")
    platform = task.get("platform", "")
    asr_model = task.get("asr_model")
    try:
        if platform in _SOCIAL:
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
    except Exception as exc:  # noqa: BLE001 - worker boundary, record & exit
        ts.log_path(task_id).parent.mkdir(parents=True, exist_ok=True)
        ts.log_path(task_id).write_text(traceback.format_exc(), encoding="utf-8")
        ts.write_task(task_id, status="failed", stage="done",
                      error=str(exc)[:2000], log_path=str(ts.log_path(task_id)))


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
