import os
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


def test_worker_timeout_marks_failed(store, monkeypatch):
    """SIGALRM watchdog fires before the 3-second sleep completes."""
    import time

    task_id = ts.new_task("https://youtu.be/timeout", "youtube")
    monkeypatch.setenv("WORKER_MAX_SECONDS", "1")

    def slow(url, asr_model=None):
        time.sleep(3)
        return {"transcript": "never"}

    monkeypatch.setattr(worker, "extract_youtube_transcript_value", slow)
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "failed"
    assert "WORKER_MAX_SECONDS" in r["error"]
    assert r.get("log_path")


def test_worker_china_platform_strips_proxy(store, monkeypatch):
    """Proxy env vars are absent during the social extraction call but restored after."""
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:10011")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:10011")

    task_id = ts.new_task("https://v.douyin.com/abc/", "douyin")

    seen_env: dict = {}

    def extract_social_post(url, asr_model=None):
        seen_env["ALL_PROXY"] = os.environ.get("ALL_PROXY")
        seen_env["HTTPS_PROXY"] = os.environ.get("HTTPS_PROXY")
        seen_env["NO_PROXY"] = os.environ.get("NO_PROXY")
        return {
            "platform": "douyin", "content_type": "video", "post_id": "1",
            "raw_transcript": "x", "script_preview": "",
            "script_path": "/s", "info_path": "/i", "info": {},
        }

    svc = mock.Mock()
    svc.extract_social_post.side_effect = extract_social_post
    monkeypatch.setattr(worker, "SocialExtractorService", lambda: svc)

    worker.run(task_id)

    # During the call: proxy vars must be absent, NO_PROXY must be "*"
    assert seen_env["ALL_PROXY"] is None
    assert seen_env["HTTPS_PROXY"] is None
    assert seen_env["NO_PROXY"] == "*"

    # After the call: original vars must be restored
    assert os.environ.get("ALL_PROXY") == "http://127.0.0.1:10011"
    assert os.environ.get("HTTPS_PROXY") == "http://127.0.0.1:10011"
    # NO_PROXY was not set before, so it must be absent again
    assert os.environ.get("NO_PROXY") is None

    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"


def test_worker_youtube_keeps_proxy(store, monkeypatch):
    """YouTube extraction must NOT have its proxy stripped."""
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:10011")

    task_id = ts.new_task("https://youtu.be/y", "youtube")

    seen_proxy: list = []

    def extract_yt(url, asr_model=None):
        seen_proxy.append(os.environ.get("ALL_PROXY"))
        return {"transcript": "ok", "title": "t"}

    monkeypatch.setattr(worker, "extract_youtube_transcript_value", extract_yt)
    worker.run(task_id)

    assert seen_proxy == ["http://127.0.0.1:10011"]
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
