import sys
from pathlib import Path
from unittest import mock

import pytest

from social_post_extractor_mcp import task_store as ts
from social_post_extractor_mcp import worker


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_REACH_HOME", str(tmp_path))
    return tmp_path


def test_worker_youtube_success(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    fake = {"video_id": "x", "title": "T", "transcript": "hello world"}
    monkeypatch.setattr(
        worker, "extract_youtube_transcript_value", lambda url, asr_model=None: fake
    )
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "hello world"
    assert r["metadata"]["title"] == "T"
    assert "transcript" not in r["metadata"]


def test_worker_social_success(store, monkeypatch):
    task_id = ts.new_task("https://v.douyin.com/abc/", "douyin")
    fake = {
        "platform": "douyin", "content_type": "video", "post_id": "9",
        "raw_transcript": "你好", "script_preview": "ignored",
        "script_path": "/s", "info_path": "/i", "info": {"k": 1},
    }
    svc = mock.Mock()
    svc.extract_social_post.return_value = fake
    monkeypatch.setattr(worker, "SocialExtractorService", lambda: svc)
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "你好"
    assert r["metadata"]["post_id"] == "9"
    svc.extract_social_post.assert_called_once()


def test_worker_failure_records_error(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    def boom(url, asr_model=None):
        raise RuntimeError("geo blocked")
    monkeypatch.setattr(worker, "extract_youtube_transcript_value", boom)
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "failed"
    assert "geo blocked" in r["error"]
    assert r["log_path"]


def test_worker_writes_pid_running(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    seen = {}
    def capture(url, asr_model=None):
        seen.update(ts.read_task(task_id))
        return {"transcript": "ok"}
    monkeypatch.setattr(worker, "extract_youtube_transcript_value", capture)
    worker.run(task_id)
    assert seen["status"] == "running"
    assert seen["pid"] > 0
