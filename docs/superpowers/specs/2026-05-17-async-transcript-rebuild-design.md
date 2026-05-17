# Async Transcript Rebuild — Design

**Date:** 2026-05-17
**Branch:** `async-rebuild` (off `ffmpeg-and-youtube`, fork `MarShao0124/social-post-extractor-mcp`)
**Discipline:** karpathy-guidelines · **Execution:** subagent-driven-development

## Problem

The current server's long-running transcript tools (`extract_douyin_text`,
`social_extract_transcript`, `youtube_extract_transcript`, `social_capture_url`,
`extract_social_post_script`) routinely take 60s–10min (download a ~140MB video,
ffmpeg-extract audio, upload to DashScope OSS, async ASR poll). They are invoked
through `mcporter call`, whose 0.11.1 client cannot complete any stdio tool call
over ~60s — even with `--timeout`, even though the same server returns in ~72s via
a reference MCP client. Progress notifications do not rescue this (mcporter ignores
the reset). The previous session worked around this by building parallel escape
hatches — a `~/.agent-reach/bin/douyin-transcript` bash wrapper and a standalone
`~/.claude/skills/agent-reach/scripts/asr.py` — leaving the actual MCP tool
documented as "DO NOT USE." This is the fragmentation we are removing.

## Goal & Success Criterion

One MCP server, working through the normal `mcporter call` path, no bypass scripts.

**Verifiable success:** through plain `mcporter call`:
1. `submit_transcript(<douyin ~140MB clip>)` returns `{task_id, status:"running"}` in **<60s**.
2. `get_transcript(<task_id>)` polled to completion returns the full Chinese transcript.
3. Same round-trip succeeds for a YouTube URL (subtitles path) and a YouTube URL with no subtitles (ASR path).
4. `~/.agent-reach/bin/douyin-transcript` is deleted; agent-reach `references/video.md` + `SKILL.md` and the `project_agent_reach_install.md` memory note are rewritten to the submit/get flow; no test or doc references a bypass script.

## Architecture — Approach A: detached worker + on-disk task store

```
mcporter call submit_transcript(url)
        │  (<1s: validate, write task file, spawn detached worker, return)
        ▼
~/.agent-reach/tasks/<task_id>.json   ◄── worker writes progress + result atomically
        ▲
        │  (<1s each: read task file)
mcporter call get_transcript(task_id)
```

Three tools replace the five long-running ones:

| Tool | Behavior | Latency |
|------|----------|---------|
| `submit_transcript(url, asr_model?)` | Detect platform, generate `task_id`, write task file `status:"queued"`, spawn detached worker, return `{task_id, status:"running"}` | <1s |
| `get_transcript(task_id)` | Read task file, return `{status, progress, transcript?, error?}` | <1s |
| `extract_transcript(url, wait=true, timeout_sec=900, asr_model?)` | Convenience: submit, then internally poll + `ctx.report_progress()` until done or `timeout_sec`. For clients that hold the connection (Claude Code). On timeout returns `{status:"running", task_id}` so caller can switch to `get_transcript`. | up to `timeout_sec` |

`submit_transcript` auto-routes by URL:
- douyin / xiaohongshu / bilibili → existing `SocialExtractorService` pipeline
- youtube / any other yt-dlp host → existing `extract_youtube_transcript_value`

The worker is the existing, already-working extraction logic — **no ASR
re-implementation**. We wrap it, we do not rewrite it.

### Worker process

- Entrypoint: `python -m social_post_extractor_mcp.worker <task_file>`.
- Spawned via `subprocess.Popen([...], start_new_session=True, stdin=DEVNULL, stdout/stderr → <task_dir>/<task_id>.log)`. `start_new_session=True` (setsid) detaches it from the MCP process group so it survives the per-call stdio server exiting. **No long-lived daemon.**
- Lifecycle written to the task file: `queued → running → succeeded | failed`. A `progress` float (0.0–1.0) and `stage` string ("downloading"/"extracting_audio"/"uploading"/"asr_polling") updated at each phase.
- Result transcript and metadata written into the same file on success; on exception, `status:"failed"` + `error` (stringified, truncated) + pointer to the `.log` file.

### Task store

- Dir: `~/.agent-reach/tasks/` (honors `AGENT_REACH_HOME` if set; falls back to `~/.agent-reach`). Created on demand.
- File: `<task_id>.json`, `task_id = uuid4().hex`.
- **Atomic writes:** worker writes `<task_id>.json.tmp` then `os.replace()` → reader never sees a torn JSON.
- **Stale GC:** on each `submit_transcript`, delete task files + logs with mtime older than 7 days. Bounded, no scheduler.

### Tool surface changes (surgical)

- **Remove** the five long-running tools listed in Problem (they are the failure mode).
- **Keep unchanged:** `parse_social_post_info`, `parse_douyin_video_info`, `get_douyin_download_link`, `social_analyze_owner_posts` — all fast (<60s), still work via `mcporter call`.
- Update the `social_post_extraction_guide` prompt to describe submit/get/extract.
- `asr.py` and the working extraction internals in `social_extractor.py` are **untouched** (frozen, not imported by anything new beyond what already imports them).

## Error Handling

- **Invalid/unsupported URL:** `submit_transcript` fails fast, returns `{status:"error", error}` *before* spawning a worker (no orphan task file).
- **Worker crash / killed:** task file stays at last `running` state. `get_transcript` detects the worker PID is dead while status≠succeeded/failed and the file mtime is stale (>120s since last update) → returns `{status:"failed", error:"worker exited without completing", log_path}`.
- **Missing task_id:** `get_transcript` returns `{status:"error", error:"unknown task_id"}`.
- **yt-dlp fatal (geo-block etc.):** worker records `status:"failed"` with the existing `_ytdlp_fatal_reason` message — no silent ASR burn.
- **Missing BAILIAN_API_KEY for ASR path:** worker fails the task with the actionable message (already implemented upstream).

## Testing (TDD per karpathy #4)

1. **Task store unit tests:** atomic write/read under interleaving; stale GC respects 7-day cutoff; unknown task_id path.
2. **submit/route unit tests:** URL → platform routing; invalid URL fails before spawn (mock `Popen`, assert not called on bad input, called once on good input).
3. **Worker integration test (mocked extraction):** patch the extraction function to a fast fake → assert task file transitions queued→running→succeeded with transcript; patch it to raise → assert failed + error + log_path.
4. **Dead-worker detection unit test:** stale `running` file + non-existent PID → `get_transcript` reports failed.
5. **End-to-end manual gate (the success criterion):** real Douyin + YouTube through `mcporter call`. Documented, run before merge.

Unit/integration tests run in CI-style `pytest`; the network E2E is the manual merge gate.

## Out of Scope (YAGNI)

- No job queue / concurrency limits (single-user vault; OS handles parallel workers fine).
- No persistent daemon (Approach C rejected).
- No retry policy beyond what the existing Douyin CDN code already does.
- No changes to `asr.py`, OCR, owner-analytics, or the upstream extraction algorithms.
- No new ASR models or providers.

## Post-Merge Cleanup (tracked, not optional)

1. Delete `~/.agent-reach/bin/douyin-transcript`.
2. Rewrite `~/.claude/skills/agent-reach/references/video.md` + `SKILL.md` quick commands to `submit_transcript`/`get_transcript`.
3. Rewrite `project_agent_reach_install.md` memory note — remove "DO NOT use mcporter call" warnings, document the async flow.
