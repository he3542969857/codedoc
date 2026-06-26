"""选仓 search —— 统一到 codedoc 真索引(pg_vectors,真 pgvector + 嵌入缓存),不再用 lexical 桩。

仓级打分:对每个仓跑 pv.query(真向量检索)取 top-N 节点,**仓分 = top-N 节点余弦分均值**
(真有相关代码的仓,最近邻节点余弦分明显更高)→ 排序 → 相对阈值取候选。
可选 rerank:复用 pv.rerank 对候选仓的代表节点再精排。供 SelectorAgent 当 search_repos 工具。
"""
from __future__ import annotations

from codedoc.index import pg_vectors as pv
from codedoc.multi_agent import make_selector, ReActAgent


def indexed_repos() -> list[str]:
    with pv._conn() as cn, cn.cursor() as cur:
        cur.execute("SELECT DISTINCT repo_name FROM code_vectors")
        return [r[0] for r in cur.fetchall()]


def repo_scores(query: str, repos: list[str], top_nodes: int = 8) -> list[tuple[str, float]]:
    out = []
    for repo in repos:
        try:
            hits = pv.query(repo, query, top_k=top_nodes)
        except Exception:
            hits = []
        if not hits:
            continue
        s = sum(h["score"] for h in hits[:top_nodes]) / min(len(hits), top_nodes)
        out.append((repo, round(s, 4)))
    out.sort(key=lambda x: -x[1])
    return out


def repo_scores_reranked(query: str, repos: list[str], top_nodes: int = 8) -> list[tuple[str, float]]:
    """仓级 rerank:每个仓用 top 节点名/全限定名当代表文档,交叉编码精排(比裸余弦区分度高)。
    轻量:只用节点名(选仓保持浅,不取源码)。"""
    cand = []
    for repo in repos:
        try:
            hits = pv.query(repo, query, top_k=top_nodes)
        except Exception:
            hits = []
        if hits:
            doc = " ".join((h.get("qualified_name") or h.get("name") or "") for h in hits[:top_nodes])
            cand.append((repo, doc))
    if not cand:
        return []
    rr = pv.rerank(query, [d for _, d in cand])          # [(idx, score)] 真交叉编码
    scored = sorted([(cand[i][0], round(float(s), 4)) for i, s in rr], key=lambda x: -x[1])
    return scored


def make_codedoc_search(top_nodes: int = 8, top_repos: int = 4, ratio: float = 0.85,
                        use_rerank: bool = True):
    """返回 search_repos_tool(query, repos) —— 走真索引,聚合到仓级、排序、相对阈值取候选。"""
    def search_repos_tool(query: str, repos: list[str] | None = None) -> list[str]:
        repos = repos or indexed_repos()
        scored = (repo_scores_reranked if use_rerank else repo_scores)(query, repos, top_nodes)
        if not scored:
            return []
        top = scored[0][1]
        return [r for r, s in scored[:top_repos] if s >= top * ratio]
    return search_repos_tool


# ────────────────────────── 生产装配:接 multi_agent ──────────────────────────
def make_codedoc_selector(top_repos: int = 3, ratio: float = 0.85):
    """生产 SelectorAgent —— 真索引 search + rerank 已在 search 内完成判别,judge 直通。"""
    search = make_codedoc_search(top_repos=top_repos, ratio=ratio, use_rerank=True)
    return make_selector(lambda q: search(q, None), lambda repo, q: True)


def make_codedoc_repo_agent_factory(question: str, top_nodes: int = 5):
    """生产 RepoAgent 工厂 —— 用真 pv.query 在仓内取证据节点(真索引,选仓后深挖)。"""
    def factory(repo: str):
        def policy(goal, scratch):
            if not scratch:
                return {"tool": "retrieve", "args": {}}
            return {"tool": "finish", "result": {"repo": repo, "nodes": scratch[0]["obs"], "deps": []}}
        tools = {"retrieve": lambda a, c: [
            h.get("qualified_name") or h.get("name")
            for h in pv.query(c.get("repo", repo), question, top_k=top_nodes)]}
        return ReActAgent("repo:%s" % repo, policy, tools, max_iter=3)
    return factory
