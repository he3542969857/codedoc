# -*- coding: utf-8 -*-
"""记忆 ablation:多轮短追问下,对比 无记忆 / 仅 last-anchor / 全套结构化记忆 的关系召回。
用数据回答:结构化 focus(衰减/强化/hub降权)对**检索**到底有没有增益?还是只 anchor 起作用?
"""
import os, sys
for k in ("CODEDOC_REPO_WORKER", "CODEDOC_DOCGEN_WORKER", "CODEDOC_SUMMARY_WORKER", "CODEDOC_WATCHDOG"):
    os.environ.setdefault(k, "0")
sys.path.insert(0, "/home/ubuntu/apps/codedoc")
import server
from codedoc.memory.manager import MemoryManager
from codedoc.graph.memory_backend import MemoryGraphQuery

CASES = [
    ("pallets/flask", "full_dispatch_request"),
    ("pallets/flask", "preprocess_request"),
    ("pallets/flask", "finalize_request"),
    ("pallets/flask", "process_response"),
]
FOLLOWUP = "它的调用者呢"   # 短追问(<16字,触发 dual-path 指代消解)

def short(x):
    return (x or "").split(".")[-1]

agg = {"no-mem": [], "anchor-only": [], "full": []}
print("=== 记忆 ablation:短追问关系召回(GT=图谱真实 callers) ===")
print("追问:'%s'(省略主语,靠记忆消解'它')\n" % FOLLOWUP)
for repo, sym in CASES:
    info = server._ensure_repo_indexed(repo)
    store, cfg = info["store"], info["cfg"]
    gq = MemoryGraphQuery(cfg, store)
    reg = server._registry_for(gq, repo, repo_root=info.get("path"))
    # 定位符号
    hits = reg.call("search", {"query": sym, "top_k": 8}).get("items", [])
    nid = next((h["node_id"] for h in hits if short(h.get("qualified_name") or h.get("name")) == sym), None)
    if not nid:
        print("  [skip]", repo, sym); continue
    nname = next((h.get("name") for h in hits if h["node_id"] == nid), sym)
    deg = len(reg.call("callers", {"node_id": nid}).get("items", [])) + \
          len(reg.call("callees", {"node_id": nid}).get("items", []))
    gt = {short(c.get("qualified_name") or c.get("name"))
          for c in reg.call("callers", {"node_id": nid}).get("items", [])} - {""}
    if not gt:
        print("  [skip 无 caller GT]", repo, sym); continue
    # 三档记忆
    m_anchor = MemoryManager(); m_anchor.push_anchor(nid, nname)
    m_full = MemoryManager(); m_full.push_anchor(nid, nname); m_full.touch_focus(nname, nid, degree=deg)
    conds = [("no-mem", None), ("anchor-only", m_anchor), ("full", m_full)]
    line = "  %s/%s (GT callers=%d): " % (repo, sym, len(gt))
    for label, smem in conds:
        hh = server._retrieve_context(store, gq, FOLLOWUP, max_nodes=25, repo=repo, smem=smem)
        got = {short(getattr(h["node"], "qualified_name", "") or getattr(h["node"], "name", "")) for h in hh}
        rec = len(gt & got) / len(gt)
        agg[label].append(rec)
        line += "%s=%.0f%% " % (label, rec * 100)
    print(line)

print("\n>>> 平均关系召回:")
for label in ("no-mem", "anchor-only", "full"):
    xs = agg[label]
    if xs:
        print("   %-12s %.1f%%  (%d 例)" % (label, sum(xs) / len(xs) * 100, len(xs)))
print("\n>>> 解读:anchor-only 与 full 若相等 → 结构化 focus 对【检索】无增益(只影响 prompt 理解);")
print("    no-mem 若明显低 → 记忆的检索价值来自 last-anchor 的 dual-path 指代消解。")
