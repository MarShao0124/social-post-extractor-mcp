import json
from unittest import mock

import pytest

import social_post_extractor_mcp.server as srv
from social_post_extractor_mcp import task_store as ts


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_REACH_HOME", str(tmp_path))
    return tmp_path


def test_detect_platform():
    assert srv._detect_platform("https://v.douyin.com/abc/") == "douyin"
    assert srv._detect_platform("see https://www.xiaohongshu.com/x or xhslink.com/y") == "xiaohongshu"
    assert srv._detect_platform("https://b23.tv/abc") == "bilibili"
    assert srv._detect_platform("https://www.bilibili.com/video/BV1") == "bilibili"
    assert srv._detect_platform("https://youtu.be/x") == "youtube"
    assert srv._detect_platform("https://example.com/v.mp4") == "generic"


def test_submit_invalid_url_does_not_spawn(store):
    with mock.patch("social_post_extractor_mcp.server.subprocess.Popen") as popen:
        out = json.loads(srv.submit_transcript("not a url"))
    assert out["status"] == "error"
    popen.assert_not_called()


def test_submit_spawns_detached_and_returns_task_id(store):
    with mock.patch("social_post_extractor_mcp.server.subprocess.Popen") as popen:
        out = json.loads(srv.submit_transcript("https://youtu.be/x"))
    assert out["status"] == "running"
    assert len(out["task_id"]) == 32
    popen.assert_called_once()
    kwargs = popen.call_args.kwargs
    assert kwargs["start_new_session"] is True
    assert kwargs["close_fds"] is True
    assert ts.read_task(out["task_id"])["platform"] == "youtube"


def test_get_transcript_delegates_to_resolve(store):
    tid = ts.new_task("u", "youtube")
    ts.write_task(tid, status="succeeded", transcript="hi", progress=1.0)
    out = json.loads(srv.get_transcript(tid))
    assert out["status"] == "succeeded"
    assert out["transcript"] == "hi"


def test_removed_tools_are_gone():
    for name in ("extract_douyin_text", "social_extract_transcript",
                 "youtube_extract_transcript", "social_capture_url",
                 "extract_social_post_script"):
        assert not hasattr(srv, name), f"{name} should have been removed"
