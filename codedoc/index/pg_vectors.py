"""向量索引:BGE-M3 嵌入 + pgvector(HNSW)+ BGE-reranker 精排。

按原版接口重建:
- ensure_schema()                     建表 code_vectors + HNSW 索引
- node_text(node) -> str              拼嵌入文本(名+签名+docstring+源码体)
- embed_texts(texts) -> list[vec]     SiliconFlow BGE-M3
- upsert_repo(repo, items) -> int     嵌入并写入
- query(repo, q, top_k) -> list[dict] 向量召回(SET hnsw.ef_search=200)
- rerank(query, docs) -> [(idx,score)] BGE-reranker
- count(repo) / delete_repo(repo)
向量召回 25%→96% 的关键:ef_search 默认 40 偏低,这里查询期设 200。
"""
from __future__ import annotations

import json
import os
import urllib.request
import urllib.error

import psycopg

PG_DSN = os.environ.get(
    "CODEDOC_PG_DSN",
    "host=127.0.0.1 port=5432 dbname=codedoc user=codedoc password=CHANGE_ME_PG_PASSWORD",
)
_SF_BASE = os.environ.get("CODEDOC_EMBED_BASE", "https://api.siliconflow.cn/v1")
_SF_KEY = os.environ.get("SILICONFLOW_API_KEY", "YOUR_SILICONFLOW_API_KEY")
import functools as _functools
EMBED_MODEL = "BAAI/bge-m3"
RERANK_MODEL = "BAAI/bge-reranker-v2-m3"
EMBED_DIM = 1024
_MAX_CHARS = 3000
_RERANK_MAX_CHARS = 500
_EF_SEARCH = int(os.environ.get("CODEDOC_EF_SEARCH", "200"))
_BATCH = 32


def _conn():
    return psycopg.connect(PG_DSN, autocommit=True)


def ensure_schema() -> None:
    with _conn() as cn, cn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS code_vectors ("
            "repo_name TEXT, node_id TEXT, embedding vector(%d), "
            "kind TEXT, file TEXT, name TEXT, qualified_name TEXT, document TEXT, "
            "PRIMARY KEY (repo_name, node_id))" % EMBED_DIM
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cv_hnsw ON code_vectors "
                    "USING hnsw (embedding vector_cosine_ops)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cv_repo ON code_vectors (repo_name)")


def node_text(n: dict) -> str:
    parts = [n.get("qualified_name") or n.get("name") or "",
             n.get("name") or "", n.get("signature") or "",
             n.get("docstring") or "", n.get("source") or ""]
    return "\n".join(p for p in parts if p).strip()[:_MAX_CHARS]


def _embed_batch(batch: list[str], retries: int = 3) -> list[list[float]]:
    payload = json.dumps({"model": EMBED_MODEL, "input": batch}).encode()
    for _ in range(retries):
        try:
            req = urllib.request.Request(
                _SF_BASE.rstrip("/") + "/embeddings", data=payload,
                headers={"Authorization": "Bearer " + _SF_KEY, "Content-Type": "application/json"},
                method="POST")
            d = json.loads(urllib.request.urlopen(req, timeout=60).read())
            return [item["embedding"] for item in d["data"]]
        except Exception:
            continue
    return [[0.0] * EMBED_DIM for _ in batch]


def embed_texts(texts: list[str]) -> list[list[float]]:
    out: list[list[float]] = []
    for i in range(0, len(texts), _BATCH):
        out.extend(_embed_batch([(t or " ")[:_MAX_CHARS] for t in texts[i:i + _BATCH]]))
    return out


def _vec_literal(v: list[float]) -> str:
    return "[" + ",".join("%.6f" % x for x in v) + "]"


def upsert_repo(repo: str, items: list[dict], progress=None) -> int:
    ensure_schema()
    items = [it for it in items if node_text(it)]
    if not items:
        return 0
    texts = [node_text(it) for it in items]
    vecs = embed_texts(texts)
    with _conn() as cn, cn.cursor() as cur:
        for it, v in zip(items, vecs):
            cur.execute(
                "INSERT INTO code_vectors (repo_name, node_id, embedding, kind, file, name, qualified_name, document) "
                "VALUES (%s,%s,%s::vector,%s,%s,%s,%s,%s) "
                "ON CONFLICT (repo_name, node_id) DO UPDATE SET embedding=EXCLUDED.embedding, document=EXCLUDED.document",
                (repo, it["id"], _vec_literal(v), it.get("kind", ""), it.get("file", ""),
                 it.get("name", ""), it.get("qualified_name", ""), node_text(it)[:1000]),
            )
    return len(items)


def _resolve_repo(cur, repo: str) -> str:
    """索引名('pallets/click')与查询名('click')可能不一致:精确优先,否则按 basename 后缀匹配。"""
    cur.execute("SELECT 1 FROM code_vectors WHERE repo_name=%s LIMIT 1", (repo,))
    if cur.fetchone():
        return repo
    base = repo.rstrip("/").split("/")[-1]
    cur.execute("SELECT repo_name FROM code_vectors WHERE repo_name=%s OR repo_name LIKE %s LIMIT 1",
                (base, "%/" + base))
    r = cur.fetchone()
    return r[0] if r else repo


@_functools.lru_cache(maxsize=512)
def _embed_query_cached(text: str) -> tuple:
    """query 嵌入缓存:同一问句(含多仓 fan-out 同问句、重复请求)只调一次嵌入 API,省 ~370ms。"""
    return tuple(embed_texts([text])[0])


def query(repo: str, query_text: str, top_k: int = 25) -> list[dict]:
    vec = list(_embed_query_cached(query_text))  # 命中缓存则跳过嵌入 API
    lit = _vec_literal(vec)
    with _conn() as cn, cn.cursor() as cur:
        repo = _resolve_repo(cur, repo)
        cur.execute("SET hnsw.ef_search = %d" % _EF_SEARCH)
        cur.execute(
            "SELECT node_id, name, kind, file, qualified_name, 1 - (embedding <=> %s::vector) AS score "
            "FROM code_vectors WHERE repo_name = %s ORDER BY embedding <=> %s::vector LIMIT %s",
            (lit, repo, lit, top_k),
        )
        rows = cur.fetchall()
    return [{"node_id": r[0], "name": r[1], "kind": r[2], "file": r[3],
             "qualified_name": r[4], "score": round(float(r[5]), 4)} for r in rows]


def rerank(query_text: str, documents: list[str], top_n=None, timeout=30):
    docs = [(d or " ")[:_RERANK_MAX_CHARS] for d in documents]
    if not docs:
        return []
    payload = json.dumps({"model": RERANK_MODEL,
                          "query": (query_text or " ")[:_RERANK_MAX_CHARS],
                          "documents": docs, "top_n": top_n or len(docs)}).encode()
    try:
        req = urllib.request.Request(
            _SF_BASE.rstrip("/") + "/rerank", data=payload,
            headers={"Authorization": "Bearer " + _SF_KEY, "Content-Type": "application/json"},
            method="POST")
        d = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
        return [(r["index"], float(r["relevance_score"])) for r in d.get("results", [])]
    except Exception:
        return [(i, 1.0) for i in range(len(docs))]


def count(repo: str) -> int:
    try:
        with _conn() as cn, cn.cursor() as cur:
            repo = _resolve_repo(cur, repo)
            cur.execute("SELECT COUNT(*) FROM code_vectors WHERE repo_name=%s", (repo,))
            return int(cur.fetchone()[0])
    except Exception:
        return 0


def delete_repo(repo: str) -> None:
    with _conn() as cn, cn.cursor() as cur:
        cur.execute("DELETE FROM code_vectors WHERE repo_name=%s", (repo,))
