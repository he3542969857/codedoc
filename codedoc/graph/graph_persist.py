"""图谱落盘持久化(PG JSONB)。

图原本只在每个 worker 内存里,冷启动靠重新解析仓库。这里把整张图(节点+边)
存进 PostgreSQL 的 graph_snapshot 表,冷启动直接 load、4 个 worker 共享同一份快照,
不再 re-parse。save_graph 单事务原子写。
"""
from __future__ import annotations

import json
import os

import psycopg

PG_DSN = os.environ.get(
    "CODEDOC_PG_DSN",
    "host=127.0.0.1 port=5432 dbname=codedoc user=codedoc password=CHANGE_ME_PG_PASSWORD",
)


def _conn():
    return psycopg.connect(PG_DSN, autocommit=True)


def ensure_schema() -> None:
    with _conn() as cn, cn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS graph_snapshot ("
                    "repo TEXT PRIMARY KEY, nodes_json JSONB, edges_json JSONB, "
                    "n_nodes INT, n_edges INT, updated_at TIMESTAMPTZ DEFAULT now())")


def save_graph(repo: str, nodes: list[dict], edges: list[dict]) -> None:
    ensure_schema()
    nj = json.dumps(nodes, ensure_ascii=False)
    ej = json.dumps(edges, ensure_ascii=False)
    with _conn() as cn, cn.cursor() as cur:
        cur.execute(
            "INSERT INTO graph_snapshot (repo, nodes_json, edges_json, n_nodes, n_edges, updated_at) "
            "VALUES (%s,%s,%s,%s,%s, now()) "
            "ON CONFLICT (repo) DO UPDATE SET nodes_json=EXCLUDED.nodes_json, "
            "edges_json=EXCLUDED.edges_json, n_nodes=EXCLUDED.n_nodes, n_edges=EXCLUDED.n_edges, "
            "updated_at=now()",
            (repo, nj, ej, len(nodes), len(edges)))


def load_graph(repo: str):
    """返回 (nodes, edges) 或 None。"""
    try:
        with _conn() as cn, cn.cursor() as cur:
            cur.execute("SELECT nodes_json, edges_json FROM graph_snapshot WHERE repo=%s", (repo,))
            row = cur.fetchone()
            if not row:                                  # 索引名(pallets/flask)vs 查询名(flask)
                base = repo.rstrip("/").split("/")[-1]
                cur.execute("SELECT nodes_json, edges_json FROM graph_snapshot "
                            "WHERE repo=%s OR repo LIKE %s LIMIT 1", (base, "%/" + base))
                row = cur.fetchone()
            if not row:
                return None
            nodes = row[0] if isinstance(row[0], list) else json.loads(row[0])
            edges = row[1] if isinstance(row[1], list) else json.loads(row[1])
            return nodes, edges
    except Exception:
        return None


def delete_graph(repo: str) -> None:
    try:
        with _conn() as cn, cn.cursor() as cur:
            cur.execute("DELETE FROM graph_snapshot WHERE repo=%s", (repo,))
    except Exception:
        pass
