"""克隆 / 索引的 Postgres 派发队列(跨 worker 一致 + 断点可恢复)。

原先 submit/upload 直接在【提交它的那个 worker】上 `threading.Thread` 跑克隆+索引——
不是跨 worker 队列,且那个 worker 崩了任务就永久卡死。这里加一层 PG 队列:
- submit/upload 只投递一个 queued 作业;
- 每个 uvicorn worker 的 repo-worker 经 `FOR UPDATE SKIP LOCKED` 抢单 → 任意 worker 可领任意作业;
- running 作业带 heartbeat,崩溃后 reclaim_orphans 打回 queued 由他人重跑 → 断点可恢复。

仓库明细(名称/状态/节点边)仍记在 SQLite user_repos(本就跨 worker 共享文件),
本模块只负责"谁来干这个克隆/索引作业"。克隆 vs 仅索引由 url 前缀('local:')区分。
"""
from __future__ import annotations

import os
import psycopg
from psycopg.rows import dict_row

# DSN 复用项目单一来源(codedoc.graph.graph_persist),不再各模块重复明文密钥
try:
    from codedoc.graph.graph_persist import PG_DSN
except Exception:
    PG_DSN = os.environ["CODEDOC_PG_DSN"]


def _connect(dict_rows=False):
    if dict_rows:
        return psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)
    return psycopg.connect(PG_DSN, autocommit=True)


def ensure_schema() -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "CREATE TABLE IF NOT EXISTS repo_jobs ("
                "task_id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, name TEXT NOT NULL, "
                "url TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', attempts INT NOT NULL DEFAULT 0, "
                "last_error TEXT, created_at TIMESTAMPTZ DEFAULT now(), heartbeat TIMESTAMPTZ)"
            )
            conn.cursor().execute(
                "CREATE INDEX IF NOT EXISTS idx_repo_jobs_status ON repo_jobs(status, created_at)")
    except Exception:
        pass


def enqueue(task_id, user_id, name, url) -> bool:
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO repo_jobs (task_id, user_id, name, url, status) "
                "VALUES (%s,%s,%s,%s,'queued') ON CONFLICT (task_id) DO NOTHING",
                (task_id, int(user_id), name, url))
            return cur.rowcount > 0
    except Exception:
        return False


def claim_one():
    """原子领取一个 queued 作业(多 worker 安全)。返回 dict 或 None。"""
    try:
        with _connect(dict_rows=True) as conn:
            return conn.cursor().execute(
                "UPDATE repo_jobs SET status='running', attempts=attempts+1, heartbeat=now() "
                "WHERE task_id = (SELECT task_id FROM repo_jobs WHERE status='queued' "
                "                 ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) "
                "RETURNING task_id, user_id, name, url").fetchone()
    except Exception:
        return None


def heartbeat(task_id) -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute("UPDATE repo_jobs SET heartbeat=now() WHERE task_id=%s", (task_id,))
    except Exception:
        pass


def complete(task_id) -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE repo_jobs SET status='done', heartbeat=now() WHERE task_id=%s", (task_id,))
    except Exception:
        pass


def fail(task_id, err: str = "", max_attempts: int = 2) -> None:
    """失败:未到上限打回 queued 重试,否则标 error。"""
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE repo_jobs SET status = CASE WHEN attempts >= %s THEN 'error' ELSE 'queued' END, "
                "last_error=%s, heartbeat=now() WHERE task_id=%s",
                (max_attempts, (err or "")[:500], task_id))
    except Exception:
        pass


def reclaim_orphans(timeout_secs=900) -> int:
    """崩溃孤儿重建:running 作业 heartbeat 超时 → 打回 queued,由其他 worker 重跑。"""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE repo_jobs SET status='queued' WHERE status='running' AND "
                "(heartbeat IS NULL OR heartbeat < now() - make_interval(secs => %s))",
                (timeout_secs,))
            return cur.rowcount or 0
    except Exception:
        return 0
