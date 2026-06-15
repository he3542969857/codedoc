"""GraphRAG —— 全局/架构类问题走社区摘要(map-reduce)。

局部问题(某函数怎么实现)用向量/全文召回就够;但"主要子系统有哪些""整体架构"
这类全局问题,零散召回拼不出全貌。GraphRAG:
  1. 在内存图上跑 Louvain 社区划分(networkx)
  2. 每个大社区让 LLM 一句话概括职责(map)
  3. 汇总各社区摘要回答整体问题(reduce)
"""
from __future__ import annotations

import re

import networkx as nx

from codedoc.agents.llm import ChatMessage

_GLOBAL_RE = re.compile(
    r"(架构|整体|总体|主要(的)?(子系统|模块|组件|部分|功能)|有哪些(模块|子系统|组件|部分|功能)"
    r"|项目结构|怎么(组织|划分)|大致|概览|overview|architecture|high.?level|main (components|modules|subsystems)"
    r"|structure|organized|bird.?s.?eye)", re.I)


def is_global_question(q: str) -> bool:
    return bool(_GLOBAL_RE.search(q or ""))


def community_summaries(store, llm, top_k: int = 12) -> list[dict]:
    g = nx.Graph()
    g.add_nodes_from(store.nodes.keys())
    for e in store.edges:
        if e["src"] in store.nodes and e["dst"] in store.nodes:
            g.add_edge(e["src"], e["dst"])
    if g.number_of_edges() == 0:
        return []
    comms = sorted(nx.community.louvain_communities(g, seed=42), key=len, reverse=True)[:top_k]
    out = []
    for i, comm in enumerate(comms):
        members = [store.nodes[n] for n in comm if n in store.nodes]
        names = [(m.get("qualified_name") or m.get("name") or "") for m in members]
        files = sorted({m.get("file", "") for m in members if m.get("file")})
        prompt = ("代码库的一个社区(联系紧密的一组符号)。\n文件:%s\n代表符号:%s\n"
                  "用一句话(<=30字)概括这个子系统负责什么。" % (files[:8], names[:25]))
        s = llm.chat([ChatMessage("system", "你是代码架构分析助手,一句话概括子系统职责。"),
                      ChatMessage("user", prompt)], max_tokens=120)
        out.append({"id": i, "size": len(comm), "files": files[:6], "summary": (s or "").strip()})
    return out


def answer_global(question: str, summaries: list[dict], llm) -> str:
    ctx = "\n".join("- 子系统%d(%d 个符号,文件 %s):%s"
                    % (s["id"], s["size"], "、".join(s["files"][:3]), s["summary"]) for s in summaries)
    return llm.chat([
        ChatMessage("system", "你是代码架构讲解助手。基于下面各子系统摘要,回答用户对整体架构的提问;"
                              "给出主要子系统及职责,提到具体文件/符号时用反引号。"),
        ChatMessage("user", "问题:%s\n\n各子系统摘要:\n%s" % (question, ctx)),
    ], max_tokens=800)
