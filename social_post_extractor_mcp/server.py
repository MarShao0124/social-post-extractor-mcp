#!/usr/bin/env python3
"""MCP server for Douyin, Xiaohongshu, and Bilibili extraction."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from mcp.server.fastmcp import Context, FastMCP

from .env_loader import load_default_env_files
from .social_extractor import (
    OwnerAnalyticsCommandProvider,
    SocialExtractorService,
)
from . import task_store as ts


load_default_env_files(Path(__file__).resolve().parents[1])


mcp = FastMCP(
    "Social Post Extractor MCP Server",
    dependencies=["requests", "ffmpeg-python", "mcp"],
)

_SERVICE = SocialExtractorService()
_OWNER_ANALYTICS = OwnerAnalyticsCommandProvider()



def parse_social_post_info_value(share_link: str) -> dict:
    post = _SERVICE.parse_social_post(share_link)
    return {
        "platform": post.platform,
        "content_type": post.content_type,
        "post_id": post.post_id,
        "title": post.title,
        "body": post.body,
        "author": {
            "name": post.author_name,
            "id": post.author_id,
            **(post.author_profile or {}),
        },
        "public_metrics": post.public_metrics,
        "owner_metrics": post.owner_metrics,
        "media": post.media,
        "publish_time": post.publish_time,
        "cover_url": post.cover_url,
        "duration_sec": post.duration_sec,
        "video_url": post.video_url,
        "image_urls": post.image_urls,
        "page_url": post.page_url,
        "resolved_url": post.resolved_url,
        "status": "success",
    }



@mcp.tool()
def parse_social_post_info(share_link: str) -> str:
    """自动识别抖音或小红书链接并返回结构化信息。"""
    try:
        return json.dumps(parse_social_post_info_value(share_link), ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2)





@mcp.tool()
def social_analyze_owner_posts(
    platform: str,
    report_type: str = "recent_posts",
    limit: int = 10,
    post_id: Optional[str] = None,
    account_id: Optional[str] = None,
    account_name: Optional[str] = None,
    period: Optional[str] = None,
    timeout: int = 120,
) -> str:
    """
    调用 browser-backed CLI 拉取自己账号的复盘数据。

    需要本机已登录对应平台，并安装 opencli / bb-browser。
    """
    try:
        result = _OWNER_ANALYTICS.run(
            platform,
            report_type,
            limit=limit,
            post_id=post_id,
            account_id=account_id,
            account_name=account_name,
            period=period,
            timeout=timeout,
        )
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2)


@mcp.tool()
def parse_douyin_video_info(share_link: str) -> str:
    """兼容旧接口：只处理抖音链接并返回视频信息。"""
    try:
        result = parse_social_post_info_value(share_link)
        if result["platform"] != "douyin":
            raise ValueError("该接口仅支持抖音链接")
        return json.dumps(
            {
                "video_id": result["post_id"],
                "title": result["title"],
                "download_url": result["video_url"],
                "status": "success",
            },
            ensure_ascii=False,
            indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2)


@mcp.tool()
def get_douyin_download_link(share_link: str) -> str:
    """兼容旧接口：返回抖音无水印视频链接。"""
    try:
        result = parse_social_post_info_value(share_link)
        if result["platform"] != "douyin":
            raise ValueError("该接口仅支持抖音链接")
        return json.dumps(
            {
                "status": "success",
                "video_id": result["post_id"],
                "title": result["title"],
                "download_url": result["video_url"],
                "description": f"视频标题: {result['title']}",
                "usage_tip": "可以直接使用此链接下载无水印视频",
            },
            ensure_ascii=False,
            indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2)




_PLATFORM_PATTERNS = [
    ("douyin", re.compile(r"douyin\.com|iesdouyin", re.I)),
    ("xiaohongshu", re.compile(r"xiaohongshu\.com|xhslink", re.I)),
    ("bilibili", re.compile(r"bilibili\.com|b23\.tv", re.I)),
    ("youtube", re.compile(r"youtube\.com|youtu\.be", re.I)),
]


def _detect_platform(text: str) -> str:
    for name, pat in _PLATFORM_PATTERNS:
        if pat.search(text):
            return name
    if re.search(r"https?://", text):
        return "generic"
    return ""


@mcp.tool()
def submit_transcript(url: str, asr_model: Optional[str] = None) -> str:
    """提交一个转写任务，立即返回 task_id（<1s，不会超时）。

    用 get_transcript(task_id) 轮询结果。支持抖音/小红书/B站/YouTube/通用视频 URL。
    这是 mcporter 等有 60s 限制的客户端应使用的入口。
    """
    try:
        platform = _detect_platform(url)
        if not platform:
            return json.dumps(
                {"status": "error", "error": f"无法识别为有效视频 URL: {url[:120]}"},
                ensure_ascii=False, indent=2,
            )
        try:
            ts.gc_stale()
        except Exception:
            pass  # GC failure must never block task creation
        task_id = ts.new_task(url, platform)
        if asr_model:
            ts.write_task(task_id, asr_model=asr_model)
        with open(ts.log_path(task_id), "a") as _log_fd:
            subprocess.Popen(
                [sys.executable, "-m", "social_post_extractor_mcp.worker", task_id],
                stdin=subprocess.DEVNULL,
                stdout=_log_fd,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        return json.dumps(
            {"status": "running", "task_id": task_id, "platform": platform},
            ensure_ascii=False, indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)


@mcp.tool()
def get_transcript(task_id: str) -> str:
    """读取 submit_transcript 任务的状态/结果。<1s。

    status: queued|running|succeeded|failed|error。
    succeeded 时含 transcript 与 metadata；failed 时含 error 与 log_path。
    """
    try:
        return json.dumps(ts.resolve_status(task_id), ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)


@mcp.tool()
def extract_transcript_blocking(
    url: str,
    timeout_sec: int = 900,
    asr_model: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """⚠️ 仅供 Claude Code 等可长连接客户端。会在 mcporter 上 60s 超时——
    那里请改用 submit_transcript / get_transcript。

    内部先 submit（任务已落盘，连接断了仍可用 get_transcript(task_id) 找回），
    再轮询直到完成或 timeout_sec。
    """
    try:
        submitted = json.loads(submit_transcript(url, asr_model=asr_model))
        if submitted.get("status") != "running":
            return json.dumps(submitted, ensure_ascii=False, indent=2)
        task_id = submitted["task_id"]
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            time.sleep(5)
            res = ts.resolve_status(task_id)
            if ctx:
                ctx.info(f"[{res.get('status')}] {res.get('stage')} {res.get('progress')}")
            if res["status"] in ("succeeded", "failed", "error"):
                return json.dumps(res, ensure_ascii=False, indent=2)
        return json.dumps(
            {"status": "running", "task_id": task_id,
             "note": f"未在 {timeout_sec}s 内完成，请用 get_transcript({task_id}) 继续轮询"},
            ensure_ascii=False, indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)


@mcp.prompt()
def social_post_extraction_guide() -> str:
    """统一提取工具使用说明。"""
    return """
# 社交内容提取使用指南

## 支持的平台
- 抖音视频
- 小红书视频笔记
- 小红书图文笔记
- Bilibili 视频
- YouTube 视频

## 推荐工具（异步，无超时问题）
- `submit_transcript`: 提交转写任务，立即返回 task_id（<1s）
- `get_transcript`: 轮询 submit_transcript 任务状态/结果（<1s）
- `extract_transcript_blocking`: 仅供 Claude Code 等长连接客户端，内部 submit+轮询
- `parse_social_post_info`: 只解析基础信息（平台、作者、指标等）
- `parse_douyin_video_info`: 解析抖音视频信息
- `get_douyin_download_link`: 获取抖音无水印下载链接
- `social_analyze_owner_posts`: 拉取自己账号的复盘数据

## 典型用法（mcporter 等有 60s 限制的客户端）
1. `submit_transcript(url)` → 获得 task_id
2. 每隔几秒调用 `get_transcript(task_id)` 直到 status == "succeeded"

## 模型切换
支持通过环境变量设置默认 provider/model，也支持在单次调用时覆盖：
- ASR: `asr_model`
"""


def main():
    mcp.run()


if __name__ == "__main__":
    main()
