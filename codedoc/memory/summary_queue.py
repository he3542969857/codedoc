"""会话记忆「溢出轮异步滚动摘要」的 Postgres 作业队列。

为什么用 PG 而非 SQLite:要多 worker 安全地竞争领取作业。
- 领取用 `UPDATE ... WHERE id = (SELECT id ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING`
  —— 多个 worker 并发领取互不阻塞、互不重复(SKIP LOCKED)。
- 去重用 partial-unique 索引:同一 (user_id, repo) 只允许一个未完成(queued/running)作业。
- 崩溃孤儿重建:reclaim_orphans 把卡死的 running 作业(超时未完成)打回 queued。

自包含,自己持 psycopg 连接,镜像 graph/graph_persist.py。
"""
from __future__ import annotations

import os
import psycopg

# DSN 复用项目单一来源(codedoc.graph.graph_persist),不再各模块重复明文密钥
try:
    from codedoc.graph.graph_persist import PG_DSN
except Exception:
    PG_DSN = os.environ["CODEDOC_PG_DSN"]


def _connect():
    return psycopg.connect(PG_DSN, autocommit=True)


def ensure_schema() -> None:
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS memory_summary_jobs ("
                "id BIGSERIAL PRIMARY KEY, user_id TEXT NOT NULL, repo TEXT NOT NULL, "
                "status TEXT NOT NULL DEFAULT 'queued', attempts INT NOT NULL DEFAULT 0, "
                "last_error TEXT, created_at TIMESTAMPTZ DEFAULT now(), "
                "updated_at TIMESTAMPTZ DEFAULT now())"
            )
            # partial-unique 去重:一个 (user,repo) 同时只有一个未完成作业
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_memjob_pending "
                "ON memory_summary_jobs (user_id, repo) WHERE status IN ('queued','running')"
            )
    except Exception:
        pass


def enqueue(user_id, repo) -> bool:
    """投递一个摘要作业;若该 (user,repo) 已有未完成作业则去重跳过。"""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO memory_summary_jobs (user_id, repo, status) "
                "VALUES (%s,%s,'queued') ON CONFLICT DO NOTHING",
                (str(user_id), repo),
            )
            return cur.rowcount > 0
    except Exception:
        return False


def claim_one():
    """原子领取一个 queued 作业(多 worker 安全)。返回 (id, user_id, repo) 或 None。"""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE memory_summary_jobs SET status='running', attempts=attempts+1, updated_at=now() "
                "WHERE id = (SELECT id FROM memory_summary_jobs WHERE status='queued' "
                "           ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) "
                "RETURNING id, user_id, repo"
            )
            return cur.fetchone()
    except Exception:
        return None


def complete(job_id) -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE memory_summary_jobs SET status='done', updated_at=now() WHERE id=%s", (job_id,)
            )
    except Exception:
        pass


def fail(job_id, err: str = "", max_attempts: int = 3) -> None:
    """失败:还没到上限就打回 queued 重试,否则标 error。"""
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE memory_summary_jobs SET "
                "status = CASE WHEN attempts >= %s THEN 'error' ELSE 'queued' END, "
                "last_error=%s, updated_at=now() WHERE id=%s",
                (max_attempts, (err or "")[:500], job_id),
            )
    except Exception:
        pass


def reclaim_orphans(timeout_secs: int = 300) -> int:
    """崩溃孤儿重建:把卡死的 running 作业(超时)打回 queued。返回重建数。"""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE memory_summary_jobs SET status='queued', updated_at=now() "
                "WHERE status='running' AND updated_at < now() - make_interval(secs => %s)",
                (timeout_secs,),
            )
            return cur.rowcount or 0
    except Exception:
        return 0
