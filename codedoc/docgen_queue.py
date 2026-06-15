"""文档生成任务的 Postgres 队列(跨 worker 一致 + 断点可恢复)。

替代原先「SQLite 表 + 每个 worker 各自的进程内 FIFO」——那种模式下任务只能由提交它的
worker 处理,不是真正的跨 worker 队列。这里:
- 任务参数与结果全落 PG(codedoc 库),任意 worker 都能读到一致状态;
- 领取用 `UPDATE ... WHERE task_id=(SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`,
  四个 worker 并发抢单互不重复 → 真正跨 worker;
- running 任务带 heartbeat,崩溃后 reclaim_orphans 把卡死任务打回 queued → 断点可恢复。

列与原 SQLite 表对齐(sections/recommendations/document 仍存 JSON 字符串/文本,
序列化沿用 server.py 里的 json.dumps/loads,改动面最小)。
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

_COLS = {"status", "stage", "progress", "document", "recommendations",
         "error", "started_at", "finished_at", "sections", "template"}


def _connect(dict_rows=False):
    if dict_rows:
        return psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)
    return psycopg.connect(PG_DSN, autocommit=True)


def ensure_schema() -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "CREATE TABLE IF NOT EXISTS docgen_tasks ("
                "task_id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, repo TEXT NOT NULL, "
                "sections TEXT NOT NULL, template TEXT NOT NULL, status TEXT NOT NULL, "
                "stage TEXT DEFAULT '', progress TEXT DEFAULT '', document TEXT, "
                "recommendations TEXT, error TEXT DEFAULT '', "
                "submitted_at DOUBLE PRECISION NOT NULL, started_at DOUBLE PRECISION, "
                "finished_at DOUBLE PRECISION, heartbeat TIMESTAMPTZ)"
            )
            conn.cursor().execute(
                "CREATE INDEX IF NOT EXISTS idx_docgen_status ON docgen_tasks(status, submitted_at)")
            conn.cursor().execute(
                "CREATE INDEX IF NOT EXISTS idx_docgen_user ON docgen_tasks(user_id)")
    except Exception:
        pass


def insert(task_id, user_id, repo, sections_str, template, submitted_at) -> None:
    with _connect() as conn:
        conn.cursor().execute(
            "INSERT INTO docgen_tasks (task_id, user_id, repo, sections, template, "
            "status, stage, progress, submitted_at) VALUES (%s,%s,%s,%s,%s,'queued','queued','',%s)",
            (task_id, int(user_id), repo, sections_str, template, float(submitted_at)),
        )


def update(task_id, fields: dict) -> None:
    cols = {k: v for k, v in (fields or {}).items() if k in _COLS}
    if not cols:
        return
    sets = ", ".join("%s=%%s" % k for k in cols)
    vals = list(cols.values()) + [task_id]
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE docgen_tasks SET %s, heartbeat=now() WHERE task_id=%%s" % sets, vals)
    except Exception:
        pass


def fetch(task_id):
    try:
        with _connect(dict_rows=True) as conn:
            return conn.cursor().execute(
                "SELECT * FROM docgen_tasks WHERE task_id=%s", (task_id,)).fetchone()
    except Exception:
        return None


def list_for_user(user_id, limit=50):
    try:
        with _connect(dict_rows=True) as conn:
            return conn.cursor().execute(
                "SELECT task_id, repo, template, status, stage, progress, submitted_at, "
                "started_at, finished_at, error FROM docgen_tasks WHERE user_id=%s "
                "ORDER BY submitted_at DESC LIMIT %s", (int(user_id), int(limit))).fetchall()
    except Exception:
        return []


def queue_position(task_id, submitted_at) -> int:
    try:
        with _connect() as conn:
            row = conn.cursor().execute(
                "SELECT COUNT(*) FROM docgen_tasks WHERE status='queued' "
                "AND submitted_at <= %s AND task_id != %s", (float(submitted_at), task_id)).fetchone()
            return (row[0] if row else 0) + 1
    except Exception:
        return 1


def queue_total() -> int:
    try:
        with _connect() as conn:
            row = conn.cursor().execute(
                "SELECT COUNT(*) FROM docgen_tasks WHERE status IN ('queued','running')").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


def claim_one(started_at):
    """原子领取一个 queued 任务(多 worker 安全)。返回完整行 dict 或 None。"""
    try:
        with _connect(dict_rows=True) as conn:
            return conn.cursor().execute(
                "UPDATE docgen_tasks SET status='running', started_at=%s, heartbeat=now() "
                "WHERE task_id = (SELECT task_id FROM docgen_tasks WHERE status='queued' "
                "                 ORDER BY submitted_at FOR UPDATE SKIP LOCKED LIMIT 1) "
                "RETURNING *", (float(started_at),)).fetchone()
    except Exception:
        return None


def heartbeat(task_id) -> None:
    try:
        with _connect() as conn:
            conn.cursor().execute(
                "UPDATE docgen_tasks SET heartbeat=now() WHERE task_id=%s", (task_id,))
    except Exception:
        pass


def reclaim_orphans(timeout_secs=120) -> int:
    """崩溃孤儿重建:running 任务 heartbeat 超时 → 打回 queued,由其他 worker 重跑。"""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE docgen_tasks SET status='queued', stage='queued', progress='', "
                "started_at=NULL WHERE status='running' AND "
                "(heartbeat IS NULL OR heartbeat < now() - make_interval(secs => %s))",
                (timeout_secs,))
            return cur.rowcount or 0
    except Exception:
        return 0
