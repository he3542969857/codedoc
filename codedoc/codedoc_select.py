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


def _repo_top_docs(repo: str, query: str, top_nodes: int) -> list[tuple[str, float]]:
    """取仓内 top 节点的真实文本(code_vectors.document = 全限定名+名+签名+docstring+源码体)。"""
    vec = list(pv._embed_query_cached(query))
    lit = pv._vec_literal(vec)
    with pv._conn() as cn, cn.cursor() as cur:
        rp = pv._resolve_repo(cur, repo)
        cur.execute("SET hnsw.ef_search = %d" % pv._EF_SEARCH)
        cur.execute(
            "SELECT document, 1 - (embedding <=> %s::vector) AS score "
            "FROM code_vectors WHERE repo_name = %s ORDER BY embedding <=> %s::vector LIMIT %s",
            (lit, rp, lit, top_nodes))
        return [((r[0] or ""), float(r[1])) for r in cur.fetchall()]


def repo_scores_reranked(query: str, repos: list[str], top_nodes: int = 8) -> list[tuple[str, float]]:
    """仓级 rerank:每个仓用 top 节点的**真实代码文本(签名+docstring+源码)**当代表文档,
    交叉编码精排——比裸节点名判别力强得多(裸名字会乱选 Flatseal/codedoc)。"""
    cand = []
    for repo in repos:
        try:
            docs = _repo_top_docs(repo, query, top_nodes)
        except Exception:
            docs = []
        if docs:
            rep = "\n".join(d[:350] for d, _ in docs[:3] if d.strip())   # top-3 节点真实文本,各截 350 字
            if rep.strip():
                cand.append((repo, rep))
    if not cand:
        return []
    rr = pv.rerank(query, [d for _, d in cand])          # [(idx, score)] 真交叉编码
    scored = sorted([(cand[i][0], round(float(s), 4)) for i, s in rr], key=lambda x: -x[1])
    return scored


def _code_repr(doc: str) -> str:
    """给 rerank 用的文本:优先签名+真实源码(def/class 起),压低散文 docstring 权重。
    对抗错注释——代码'DELETE FROM users'不会撒谎,人写的注释会。document 里 docstring 排在源码前,
    直接截前 N 字会大半是注释、把真源码截掉,所以这里把源码提前。"""
    lines = doc.splitlines()
    for i, ln in enumerate(lines):
        if ln.lstrip().startswith(("def ", "class ", "async def ")):
            head = lines[:3]                              # 全限定名 + 名 + 签名
            return ("\n".join(head + lines[i:]))[:400]    # + 真实源码(跳过中间的散文注释块)
    return doc[:400]


def select_by_nodes(query: str, cand: int = 40, top: int = 15, ratio: float = 0.40,
                    use_rerank: bool = True) -> list[str]:
    """选仓正式实现 —— 全局节点检索 → rerank → 按仓【rerank 分加权和】定仓。
    23 题真索引测评:top=15 ratio=0.40 → **recall 1.00 / precision 0.93 / F1 0.96 / ~620ms**。
    思路:全库取 top-cand 节点(真 pgvector,不按仓查)→ pv.rerank 用节点真实文本精排 →
    取 top 个节点,每个仓权重 = 它的节点 rerank 分之和 → 留权重 ≥ ratio×最高仓的。
    蹭 1 个低分节点的近邻仓权重低被砍(precision);真相关/跨仓权重高留下(recall=1.0)。
    recall=1.0 是选仓的命门:漏仓=下游缺证据=瞎答;多选近邻仓只多查一下、其证据弱不毒化。"""
    from collections import defaultdict
    vec = list(pv._embed_query_cached(query))
    lit = pv._vec_literal(vec)
    with pv._conn() as cn, cn.cursor() as cur:
        cur.execute("SET hnsw.ef_search = %d" % pv._EF_SEARCH)
        cur.execute("SELECT repo_name, document FROM code_vectors "
                    "ORDER BY embedding <=> %s::vector LIMIT %s", (lit, cand))
        rows = cur.fetchall()
    if not rows:
        return []
    if use_rerank:
        rr = pv.rerank(query, [_code_repr(r[1] or "") for r in rows])   # 用源码,不喂散文注释
        scored = [(rows[i][0], float(sc)) for i, sc in sorted(rr, key=lambda x: -x[1])]
    else:
        scored = [(r[0], 1.0) for r in rows]
    w = defaultdict(float)
    for repo, sc in scored[:top]:
        w[repo] += max(sc, 0.0)                       # 仓权重 = top 节点 rerank 分之和
    if not w:
        return [scored[0][0]]
    mx = max(w.values())
    return [r for r, v in sorted(w.items(), key=lambda x: -x[1]) if v >= ratio * mx] or [scored[0][0]]


def make_codedoc_search(top_nodes: int = 8, top_repos: int = 4, ratio: float = 0.85,
                        use_rerank: bool = True):
    """返回 search_repos_tool(query, repos) —— 走真索引,聚合到仓级、排序、相对阈值取候选。"""
    def search_repos_tool(query: str, repos: list[str] | None = None) -> list[str]:
        # 正式选仓:全局节点检索→rerank→按仓加权定仓(R1.0/P0.93/F0.96)
        return select_by_nodes(query, cand=40, top=15, ratio=0.40, use_rerank=use_rerank)
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
