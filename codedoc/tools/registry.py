"""统一原子工具层 —— 一份工具实现,Web / MCP / CLI / Agent 共用。

7 个工具(ToolSpec: name / description / input_schema / handler):
  search    混合检索(向量 + 全文 TF-IDF,融合 + 精确名加权)
  context   节点 + 源码片段 + 邻居(callers/callees 名)
  callers   谁调用了它(入边遍历,排除结构边)
  callees   它调用了谁(出边遍历)
  impact    反向传递闭包(改了它会波及谁)
  explore   邻域子图(中心 + 周边节点/边,给图谱交互)
  get_body  按 file+行号从磁盘现读完整函数体(取源码统一原语)

后端可注入:build_registry(cfg, gq, vec_search=None, repo_root=None)
  gq         MemoryGraphQuery(内存图,Web 后端)—— 也可换 Neo4j GraphQuery(MCP/CLI)
  vec_search callable(query, top_k)->[{node_id,score}](Web 注入 pg_vectors.query;无则纯全文)
  repo_root  仓库磁盘根,用于 get_body 现读源码
get_body 恒在;其余 6 个是简历点名的 Agent 工具。
"""
from __future__ import annotations

import os
import io
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: dict
    handler: Callable[[dict], dict]


@dataclass
class ToolRegistry:
    cfg: Any
    gq: Any
    vec_search: Callable | None = None
    repo_root: str | None = None
    tools: dict[str, ToolSpec] = field(default_factory=dict)

    def register(self, spec: ToolSpec) -> None:
        self.tools[spec.name] = spec

    @property
    def tool_names(self) -> list[str]:
        return list(self.tools)

    def specs(self) -> list[dict]:
        return [{"name": t.name, "description": t.description, "input_schema": t.input_schema}
                for t in self.tools.values()]

    def call(self, name: str, args: dict | None = None) -> dict:
        spec = self.tools.get(name)
        if not spec:
            return {"ok": False, "error": "unknown tool: %s" % name, "items": []}
        try:
            return spec.handler(args or {})
        except Exception as e:
            return {"ok": False, "error": "%s: %s" % (type(e).__name__, e), "items": []}


# ---------- 公共辅助 ----------

def _node_item(n) -> dict:
    return {"node_id": getattr(n, "id", None), "name": getattr(n, "name", ""),
            "qualified_name": getattr(n, "qualified_name", ""), "kind": getattr(n, "kind", ""),
            "file": getattr(n, "file", ""), "signature": getattr(n, "signature", ""),
            "docstring": (getattr(n, "docstring", "") or "")[:200]}


def _norm(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    lo, hi = min(scores.values()), max(scores.values())
    if hi <= lo:
        return {k: 1.0 for k in scores}
    return {k: (v - lo) / (hi - lo) for k, v in scores.items()}


def _read_body(gq, node, max_lines: int, repo_root: str | None) -> str:
    f = getattr(node, "file", "") or ""
    s, e = getattr(node, "start_line", None), getattr(node, "end_line", None)
    if repo_root and f and s and e:
        path = f if os.path.isabs(f) else os.path.join(repo_root, f)
        try:
            lines = io.open(path, encoding="utf-8", errors="replace").read().splitlines()
            end = min(int(e), int(s) - 1 + max_lines)
            body = "\n".join(lines[int(s) - 1:end])
            if body.strip():
                return body
        except Exception:
            pass
    return "\n".join((getattr(node, "source", "") or "").splitlines()[:max_lines])


def _resolve_id(gq, args: dict):
    """get_body 入参可给 id / qualified_name / name —— 逐级解析到节点。"""
    nid = args.get("id") or args.get("node_id")
    if nid:
        n = gq.get_node(nid)
        if n:
            return n
    qn = args.get("qualified_name")
    nm = args.get("name")
    for cand_id, d in gq.store.nodes.items():
        if qn and d.get("qualified_name") == qn:
            return gq.get_node(cand_id)
    if nm:
        for cand_id, d in gq.store.nodes.items():
            if d.get("name") == nm:
                return gq.get_node(cand_id)
    return None


# ---------- 工具构建 ----------

def build_registry(cfg, gq, vec_search: Callable | None = None, repo_root: str | None = None) -> ToolRegistry:
    reg = ToolRegistry(cfg=cfg, gq=gq, vec_search=vec_search, repo_root=repo_root)

    def t_search(args: dict) -> dict:
        query = (args.get("query") or "").strip()
        top_k = int(args.get("top_k") or 10)
        if not query:
            return {"ok": True, "items": [], "summary": "empty query"}
        q_terms = {t.lower() for t in query.replace("(", " ").replace(")", " ").split()}
        # 向量召回
        vec_scores: dict[str, float] = {}
        if vec_search:
            try:
                for h in vec_search(query, top_k * 3):
                    vec_scores[h["node_id"]] = float(h.get("score") or 0)
            except Exception:
                pass
        # 全文 TF-IDF
        fts_scores: dict[str, float] = {}
        for n, sc in gq.fulltext_search(query, limit=top_k * 3):
            fts_scores[n.id] = float(sc)
        vn, fn = _norm(vec_scores), _norm(fts_scores)
        fused: dict[str, float] = {}
        for nid in set(vn) | set(fn):
            s = 0.6 * vn.get(nid, 0.0) + 0.4 * fn.get(nid, 0.0)
            # 精确名/token 命中加权(融合归一会冲淡精确信号,补回)
            d = gq.store.nodes.get(nid, {})
            nm = (d.get("name") or "").lower()
            if nm and nm in q_terms:
                s += 0.5
            fused[nid] = s
        ranked = sorted(fused.items(), key=lambda kv: -kv[1])[:top_k]
        items = []
        for nid, sc in ranked:
            n = gq.get_node(nid)
            if n:
                it = _node_item(n); it["score"] = round(sc, 4)
                items.append(it)
        return {"ok": True, "items": items, "summary": "%d hits" % len(items)}

    def t_context(args: dict) -> dict:
        n = gq.get_node(args.get("node_id") or args.get("id"))
        if not n:
            return {"ok": False, "items": [], "summary": "node not found"}
        max_lines = int(args.get("max_lines") or 45)
        callers = [c.name for c in gq.callers(n.id)[:8]]
        callees = [c.name for c in gq.callees(n.id)[:8]]
        return {"ok": True, "node": _node_item(n),
                "snippet": _read_body(gq, n, max_lines, repo_root),
                "callers": callers, "callees": callees,
                "summary": "context for %s" % n.name}

    def t_callers(args: dict) -> dict:
        nid = args.get("node_id") or args.get("id")
        depth = int(args.get("depth") or 1)
        items = [_node_item(c) for c in gq.callers(nid, depth=depth)]
        return {"ok": True, "items": items, "summary": "%d callers" % len(items)}

    def t_callees(args: dict) -> dict:
        nid = args.get("node_id") or args.get("id")
        depth = int(args.get("depth") or 1)
        items = [_node_item(c) for c in gq.callees(nid, depth=depth)]
        return {"ok": True, "items": items, "summary": "%d callees" % len(items)}

    def t_impact(args: dict) -> dict:
        """改了 seed 会波及谁:反向(callers 方向)传递闭包。"""
        nid = args.get("node_id") or args.get("id")
        max_nodes = int(args.get("max_nodes") or 200)
        seen, frontier = set(), [nid]
        edges = []
        while frontier and len(seen) < max_nodes:
            nxt = []
            for cur in frontier:
                for c in gq.callers(cur):
                    edges.append({"src": c.id, "dst": cur, "kind": "impacts"})
                    if c.id not in seen and c.id != nid:
                        seen.add(c.id); nxt.append(c.id)
            frontier = nxt
        items = [_node_item(gq.get_node(i)) for i in seen if gq.get_node(i)]
        return {"ok": True, "items": items, "edges": edges,
                "summary": "%d impacted nodes" % len(items)}

    def t_explore(args: dict) -> dict:
        """邻域子图:中心节点 + 周边(出/入边各 kind),给图谱交互。"""
        anchor = args.get("anchor") or args.get("node_id") or args.get("id")
        center = gq.get_node(anchor)
        if not center:
            return {"ok": False, "items": [], "summary": "anchor not found"}
        nodes = {anchor: _node_item(center)}
        edges = []
        for e in gq._out.get(anchor, []):
            dst = e["dst"]
            edges.append({"src": anchor, "dst": dst, "kind": e.get("kind", "")})
            dn = gq.get_node(dst)
            if dn:
                nodes[dst] = _node_item(dn)
        for e in gq._in.get(anchor, []):
            src = e["src"]
            edges.append({"src": src, "dst": anchor, "kind": e.get("kind", "")})
            sn = gq.get_node(src)
            if sn:
                nodes[src] = _node_item(sn)
        return {"ok": True, "center": anchor, "nodes": list(nodes.values()),
                "edges": edges, "summary": "%d nodes / %d edges" % (len(nodes), len(edges))}

    def t_get_body(args: dict) -> dict:
        n = _resolve_id(gq, args)
        if not n:
            return {"ok": False, "body": "", "summary": "node not found"}
        max_lines = int(args.get("max_lines") or 50)
        return {"ok": True, "node_id": n.id, "name": n.name, "file": n.file,
                "start_line": n.start_line, "end_line": n.end_line,
                "body": _read_body(gq, n, max_lines, repo_root),
                "summary": "body of %s" % n.name}

    reg.register(ToolSpec("search", "Hybrid full-text + vector search over code symbols",
                          {"query": "str", "top_k": "int"}, t_search))
    reg.register(ToolSpec("context", "Node detail + source snippet + caller/callee names",
                          {"node_id": "str", "max_lines": "int"}, t_context))
    reg.register(ToolSpec("callers", "Functions/methods that call the given node",
                          {"node_id": "str", "depth": "int"}, t_callers))
    reg.register(ToolSpec("callees", "Functions/methods the given node calls",
                          {"node_id": "str", "depth": "int"}, t_callees))
    reg.register(ToolSpec("impact", "Reverse transitive closure: what a change ripples to",
                          {"node_id": "str", "max_nodes": "int"}, t_impact))
    reg.register(ToolSpec("explore", "Neighborhood subgraph around an anchor node",
                          {"anchor": "str"}, t_explore))
    reg.register(ToolSpec("get_body", "Read full function body fresh from disk by file+lines",
                          {"id": "str", "qualified_name": "str", "name": "str", "max_lines": "int"}, t_get_body))

    # ===== 技能层(skills):原子工具之上的高阶组合 =====
    # 每个 skill 把多个原子工具/图遍历编排成一个"任务级"结果,事实层全从图谱/源码确定性抽取(无 LLM)。
    # 经 reg.call 供 Web/CLI 调用,并随 specs() 自动暴露到 MCP(Claude Code / Cursor)。
    def _resolve_for_skill(args: dict):
        n = _resolve_id(gq, args)
        if n:
            return n
        q = args.get("name") or args.get("query") or ""
        if q:
            hits = t_search({"query": q, "top_k": 1}).get("items") or []
            if hits:
                return gq.get_node(hits[0]["node_id"])
        return None

    def sk_explain_symbol(args: dict) -> dict:
        """[skill] 一个符号的完整解释包:定义体 + 上游调用者 + 下游被调 + 元数据。
        组合 search/get_body/callers/callees。"""
        n = _resolve_for_skill(args)
        if not n:
            return {"ok": False, "summary": "symbol not found"}
        max_lines = int(args.get("max_lines") or 50)
        callers = [_node_item(c) for c in gq.callers(n.id)[:5]]
        callees = [_node_item(c) for c in gq.callees(n.id)[:5]]
        return {"ok": True, "node": _node_item(n),
                "body": _read_body(gq, n, max_lines, repo_root),
                "callers": callers, "callees": callees,
                "summary": "explained %s (%d callers / %d callees)" % (n.name, len(callers), len(callees))}

    def sk_trace_chain(args: dict) -> dict:
        """[skill] 按调用方向逐层 BFS 出调用链。direction: callers(向上)/callees(向下);depth 默认 3。"""
        n = _resolve_for_skill(args)
        if not n:
            return {"ok": False, "summary": "symbol not found"}
        direction = (args.get("direction") or "callees").lower()
        depth = max(1, min(int(args.get("depth") or 3), 6))
        step = gq.callers if direction == "callers" else gq.callees
        layers, seen, frontier = [], {n.id}, [n.id]
        for _ in range(depth):
            nxt, layer = [], []
            for cur in frontier:
                for c in step(cur):
                    if c.id not in seen:
                        seen.add(c.id); nxt.append(c.id); layer.append(_node_item(c))
            if not layer:
                break
            layers.append(layer); frontier = nxt
        return {"ok": True, "root": _node_item(n), "direction": direction, "layers": layers,
                "summary": "%s chain: %d layers from %s" % (direction, len(layers), n.name)}

    def sk_impact_report(args: dict) -> dict:
        """[skill] 变更影响报告:反向传递闭包 + 按文件归类。组合 impact。"""
        n = _resolve_for_skill(args)
        if not n:
            return {"ok": False, "summary": "symbol not found"}
        imp = t_impact({"node_id": n.id, "max_nodes": int(args.get("max_nodes") or 200)})
        by_file: dict[str, list] = {}
        for it in imp.get("items", []):
            by_file.setdefault(it.get("file", "?"), []).append(it.get("qualified_name") or it.get("name"))
        return {"ok": True, "target": _node_item(n), "impacted_count": len(imp.get("items", [])),
                "by_file": by_file,
                "summary": "changing %s impacts %d symbols across %d files"
                           % (n.name, len(imp.get("items", [])), len(by_file))}

    def sk_onboarding_brief(args: dict) -> dict:
        """[skill] 新人上手简报:用调用图 **PageRank**(对标 aider repo-map)挑核心类/函数(入口点)。
        PageRank 比纯度数更能压住 wsgi_app 这类枢纽噪声、突出真正"重要"的符号;networkx 不可用时退化为度数。"""
        top_k = max(1, min(int(args.get("top_k") or 10), 30))
        kinds = {"class", "method", "function", "route_handler"}
        # 纯 Python PageRank 幂迭代(无 scipy/networkx 依赖;几千节点毫秒级),含悬挂节点处理
        pr = {}
        try:
            node_ids = list(gq.store.nodes.keys())
            N = len(node_ids)
            if N:
                out = {n: [] for n in node_ids}
                for nid, outs in gq._out.items():
                    if nid in out:
                        for e in outs:
                            dst = e.get("dst")
                            if dst in out:
                                out[nid].append(dst)
                d = 0.85
                pr = {n: 1.0 / N for n in node_ids}
                dangling = [n for n in node_ids if not out[n]]
                for _ in range(40):
                    leak = d * sum(pr[n] for n in dangling) / N
                    base = (1.0 - d) / N + leak
                    new = {n: base for n in node_ids}
                    for n in node_ids:
                        outs = out[n]
                        if outs:
                            share = d * pr[n] / len(outs)
                            for m in outs:
                                new[m] += share
                    pr = new
        except Exception:
            pr = {}
        scored = []
        for nid, d in gq.store.nodes.items():
            if d.get("kind") in kinds:
                score = pr.get(nid) if pr else (len(gq._out.get(nid, [])) + len(gq._in.get(nid, [])))
                scored.append((score or 0, nid))
        scored.sort(key=lambda x: -x[0])
        entry = []
        for score, nid in scored[:top_k]:
            nd = gq.get_node(nid)
            if nd:
                it = _node_item(nd)
                it["score"] = round(score, 6) if pr else score
                entry.append(it)
        method = "PageRank" if pr else "degree"
        return {"ok": True, "entry_points": entry, "ranking": method,
                "summary": "top %d core symbols by %s" % (len(entry), method)}

    reg.register(ToolSpec("explain_symbol",
                          "[skill] Full explanation of a symbol: body + callers + callees + metadata",
                          {"id": "str", "name": "str", "qualified_name": "str", "max_lines": "int"}, sk_explain_symbol))
    reg.register(ToolSpec("trace_chain",
                          "[skill] BFS the call chain up (callers) or down (callees) for N layers",
                          {"id": "str", "name": "str", "direction": "str", "depth": "int"}, sk_trace_chain))
    reg.register(ToolSpec("impact_report",
                          "[skill] Change-impact report: reverse transitive closure grouped by file",
                          {"id": "str", "name": "str", "max_nodes": "int"}, sk_impact_report))
    reg.register(ToolSpec("onboarding_brief",
                          "[skill] Onboarding brief: core entry-point symbols ranked by graph degree",
                          {"top_k": "int"}, sk_onboarding_brief))
    return reg
