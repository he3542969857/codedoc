"""内存代码图谱 + 检索。

MemoryGraphStore   持图:nodes(id→dict)、edges(list[{src,dst,kind}])。
MemoryGraphQuery   查图:get_node、callers/callees(沿边遍历)、fulltext_search(TF-IDF 全文倒排)。

契约取自 server.py 用法:
- store.nodes 是 {id: node_dict};node_dict 含 id/name/qualified_name/kind/signature/docstring/file/start_line/end_line/source
- gq.get_node(id) 返回 Node 对象(属性访问);fulltext_search(q, limit) 返回 [(Node, score)]
- gq.callers(id, depth=1) / callees(id, depth=1) 返回 [Node]
"""
from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Any

# 结构边不参与"调用关系"遍历
_STRUCTURAL = {"contains", "child", "member"}
_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")


class Node:
    """节点对象:把 node_dict 包成属性访问。"""
    __slots__ = ("id", "name", "qualified_name", "kind", "signature",
                 "docstring", "file", "path", "start_line", "end_line", "source", "_d")

    def __init__(self, d: dict[str, Any]):
        self._d = d
        self.id = d.get("id")
        self.name = d.get("name", "")
        self.qualified_name = d.get("qualified_name", "")
        self.kind = d.get("kind", "")
        self.signature = d.get("signature", "")
        self.docstring = d.get("docstring", "")
        self.file = d.get("file", "")
        self.path = d.get("file", "")
        self.start_line = d.get("start_line")
        self.end_line = d.get("end_line")
        self.source = d.get("source", "")

    def get(self, k, default=None):
        return self._d.get(k, default)


class MemoryGraphStore:
    def __init__(self, cfg=None):
        self.cfg = cfg
        self.nodes: dict[str, dict] = {}
        self.edges: list[dict] = []

    def upsert_nodes(self, nodes: list[dict]) -> None:
        for n in nodes:
            nid = n.get("id")
            if nid:
                self.nodes[nid] = n

    def upsert_edges(self, edges: list[dict]) -> None:
        self.edges.extend(e for e in edges if e.get("src") and e.get("dst"))

    # 给增量同步预留(后续可接 watchdog)
    def remove_file(self, file: str) -> None:
        drop = {nid for nid, n in self.nodes.items() if n.get("file") == file}
        for nid in drop:
            self.nodes.pop(nid, None)
        if drop:
            self.edges = [e for e in self.edges if e["src"] not in drop and e["dst"] not in drop]


def _node_text(n: dict) -> str:
    return " ".join(str(x) for x in (
        n.get("qualified_name") or n.get("name") or "",
        n.get("name") or "",
        n.get("signature") or "",
        n.get("docstring") or "",
    ) if x)


class MemoryGraphQuery:
    def __init__(self, cfg, store: MemoryGraphStore):
        self.cfg = cfg
        self.store = store
        # 邻接索引
        self._out: dict[str, list[dict]] = defaultdict(list)  # src -> edges
        self._in: dict[str, list[dict]] = defaultdict(list)   # dst -> edges
        for e in store.edges:
            self._out[e["src"]].append(e)
            self._in[e["dst"]].append(e)
        # TF-IDF 倒排
        self._build_index()

    # ---- 全文倒排(TF-IDF)----
    def _build_index(self) -> None:
        self._tf: dict[str, dict[str, int]] = {}      # nid -> {term: count}
        df: dict[str, int] = defaultdict(int)
        for nid, n in self.store.nodes.items():
            terms = [t.lower() for t in _TOKEN_RE.findall(_node_text(n))]
            tf: dict[str, int] = defaultdict(int)
            for t in terms:
                tf[t] += 1
            self._tf[nid] = tf
            for t in set(terms):
                df[t] += 1
        n_docs = max(1, len(self.store.nodes))
        self._idf = {t: math.log((n_docs + 1) / (c + 1)) + 1.0 for t, c in df.items()}

    def fulltext_search(self, query: str, limit: int = 10) -> list[tuple[Node, float]]:
        q_terms = [t.lower() for t in _TOKEN_RE.findall(query or "")]
        if not q_terms:
            return []
        scores: dict[str, float] = defaultdict(float)
        for nid, tf in self._tf.items():
            s = 0.0
            for t in q_terms:
                if t in tf:
                    s += tf[t] * self._idf.get(t, 1.0)
            if s > 0:
                # 名字精确命中加权(代码问答常点名符号)
                n = self.store.nodes[nid]
                nm = (n.get("name") or "").lower()
                qn = (n.get("qualified_name") or "").lower()
                if nm in q_terms or qn.split(".")[-1] in q_terms:
                    s *= 2.0
                scores[nid] = s
        ranked = sorted(scores.items(), key=lambda kv: -kv[1])[:limit]
        return [(Node(self.store.nodes[nid]), sc) for nid, sc in ranked]

    # ---- 图遍历 ----
    def get_node(self, node_id: str) -> Node | None:
        d = self.store.nodes.get(node_id)
        return Node(d) if d else None

    def callers(self, node_id: str, depth: int = 1) -> list[Node]:
        """谁调用了 node_id:入边的 src(排除结构边)。"""
        seen, out, frontier = set(), [], [node_id]
        for _ in range(max(1, depth)):
            nxt = []
            for nid in frontier:
                for e in self._in.get(nid, []):
                    if e.get("kind") in _STRUCTURAL:
                        continue
                    src = e["src"]
                    if src in seen or src == node_id:
                        continue
                    seen.add(src)
                    n = self.get_node(src)
                    if n:
                        out.append(n); nxt.append(src)
            frontier = nxt
        return out

    def callees(self, node_id: str, depth: int = 1) -> list[Node]:
        """node_id 调用了谁:出边的 dst(排除结构边)。"""
        seen, out, frontier = set(), [], [node_id]
        for _ in range(max(1, depth)):
            nxt = []
            for nid in frontier:
                for e in self._out.get(nid, []):
                    if e.get("kind") in _STRUCTURAL:
                        continue
                    dst = e["dst"]
                    if dst in seen or dst == node_id:
                        continue
                    seen.add(dst)
                    n = self.get_node(dst)
                    if n:
                        out.append(n); nxt.append(dst)
            frontier = nxt
        return out
