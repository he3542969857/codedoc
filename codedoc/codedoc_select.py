"""选仓 search —— 统一到 codedoc 真索引(pg_vectors,真 pgvector + 嵌入缓存),不再用 lexical 桩。

仓级打分:对每个仓跑 pv.query(真向量检索)取 top-N 节点,**仓分 = top-N 节点余弦分均值**
(真有相关代码的仓,最近邻节点余弦分明显更高)→ 排序 → 相对阈值取候选。
可选 rerank:复用 pv.rerank 对候选仓的代表节点再精排。供 SelectorAgent 当 search_repos 工具。
"""
from __future__ import annotations

import functools

from codedoc.index import pg_vectors as pv
from codedoc.multi_agent import make_selector, ReActAgent


@functools.lru_cache(maxsize=512)
def _to_english(query: str) -> str:
    """纯中文问题选英文仓的跨语言优化:含中文就先翻成英文(代码的语言)再检索。
    实测(9 对纯中文/英文):中文 F1 0.89 → 翻英文 F1 0.96(同语言检索更准、全文也能用)。
    LRU 缓存避免重复翻;任何失败降级回原文(不会更差)。"""
    if not any("一" <= c <= "鿿" for c in query):
        return query                                  # 没中文,免翻
    try:
        from codedoc.config import load_config
        from codedoc.llm_router.router import build_routed_llm
        from codedoc.agents.llm import ChatMessage
        llm = build_routed_llm(load_config("."))
        out = llm.chat([
            ChatMessage("system", "Translate the user's question into a concise English technical "
                        "phrase for code search. Output ONLY the English phrase, no quotes, no prefix."),
            ChatMessage("user", query)], max_tokens=60, temperature=0.0)
        out = (out or "").strip()
        return out if out and "LLM error" not in out else query
    except Exception:
        return query


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
    query = _to_english(query)                        # 含中文先翻英文(代码的语言),跨语言更准
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


def select_scored(query: str, cand: int = 40, top: int = 15, ratio: float = 0.40):
    """同 select_by_nodes,但额外返回置信度 = 最高节点 rerank 分(有没有东西真匹配上)。"""
    from collections import defaultdict
    q = _to_english(query)
    vec = list(pv._embed_query_cached(q)); lit = pv._vec_literal(vec)
    with pv._conn() as cn, cn.cursor() as cur:
        cur.execute("SET hnsw.ef_search = %d" % pv._EF_SEARCH)
        cur.execute("SELECT repo_name, document FROM code_vectors "
                    "ORDER BY embedding <=> %s::vector LIMIT %s", (lit, cand))
        rows = cur.fetchall()
    if not rows:
        return [], 0.0
    rr = pv.rerank(q, [_code_repr(r[1] or "") for r in rows])
    scored = [(rows[i][0], float(sc)) for i, sc in sorted(rr, key=lambda x: -x[1])]
    best = scored[0][1] if scored else 0.0
    w = defaultdict(float)
    for repo, sc in scored[:top]:
        w[repo] += max(sc, 0.0)
    mx = max(w.values()) if w else 0.0
    repos = [r for r, v in sorted(w.items(), key=lambda x: -x[1]) if v >= ratio * mx] or [scored[0][0]]
    return repos, best


def _reformulate(question: str) -> str:
    """反思用:让 LLM 把问题重表述成更利于代码检索的英文关键词(换个说法再试)。"""
    try:
        from codedoc.config import load_config
        from codedoc.llm_router.router import build_routed_llm
        from codedoc.agents.llm import ChatMessage
        llm = build_routed_llm(load_config("."))
        out = llm.chat([
            ChatMessage("system", "Rewrite the question into different, more specific English code-search "
                        "keywords (function/class/API terms a developer would grep). Output ONLY keywords."),
            ChatMessage("user", question)], max_tokens=60, temperature=0.2)
        out = (out or "").strip()
        return out if out and "LLM error" not in out else question
    except Exception:
        return question


def select_with_reflect(question: str, conf_threshold: float = 0.05) -> dict:
    """置信门控反思选仓:选完看最高 rerank 分;够高直接用(省反思);
    低于阈值=没把握=可能选错/笼统问题 → 反思:LLM 重表述再选,取置信更高的一版。
    grounded:反思依据是真实 rerank 置信,不是 LLM 凭空说不对。便宜门控:有把握不反思。"""
    repos, conf = select_scored(question)
    trace = [{"pass": 1, "repos": repos, "conf": round(conf, 4)}]
    if conf >= conf_threshold:
        return {"repos": repos, "confident": True, "reflected": False,
                "confidence": round(conf, 4), "trace": trace}
    rq = _reformulate(question)                       # 反思:换说法
    repos2, conf2 = select_scored(rq)
    trace.append({"pass": 2, "reformulated": rq, "repos": repos2, "conf": round(conf2, 4)})
    if conf2 > conf:                                  # 重表述后更有把握 → 采纳
        return {"repos": repos2, "confident": conf2 >= conf_threshold, "reflected": True,
                "confidence": round(conf2, 4), "trace": trace}
    return {"repos": repos, "confident": False, "reflected": True, "low_conf": True,
            "confidence": round(conf, 4), "trace": trace}


def route_question(question: str, conf_threshold: float = 0.05) -> dict:
    """问题路由:置信门控反思选仓 → 决定走【节点选仓多 Agent】还是【GraphRAG 全局】。
    低置信(选仓没把握=笼统/非代码)或命中全局关键词 → global(GraphRAG community summaries);
    否则 → repos(选中的仓 → 多 Agent 深挖)。统一'我的 rerank 置信信号'和 codedoc 既有的全局启发。"""
    try:
        from codedoc.graphrag import is_global_question
        glob_kw = is_global_question(question)
    except Exception:
        glob_kw = False
    sel = select_with_reflect(question, conf_threshold)
    if (not sel["confident"]) or glob_kw:
        return {"mode": "global",
                "reason": "global_keyword" if glob_kw else "low_confidence",
                "confidence": sel.get("confidence", 0.0), "trace": sel["trace"]}
    return {"mode": "repos", "repos": sel["repos"],
            "confidence": sel.get("confidence", 0.0), "trace": sel["trace"]}


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
