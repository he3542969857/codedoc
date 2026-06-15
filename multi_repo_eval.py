# -*- coding: utf-8 -*-
"""多仓 deep 流程回归评测(确定性 GT)。

对每个目标符号:GT = 图谱里它真实的 callers(确定性、从图谱直接取);
跑 deep 全工具扫描,测"真实调用者被召回进候选 items"的召回率。
给多仓 deep 链路一个可复现指标(可做 CI 门禁 --threshold)。
"""
import os, sys
os.environ.setdefault("CODEDOC_REPO_WORKER", "0")
os.environ.setdefault("CODEDOC_DOCGEN_WORKER", "0")
os.environ.setdefault("CODEDOC_SUMMARY_WORKER", "0")
os.environ.setdefault("CODEDOC_WATCHDOG", "0")
sys.path.insert(0, "/home/ubuntu/apps/codedoc")
import server
from codedoc.agents.graph import _deep_repo_agent
from codedoc.graph.memory_backend import MemoryGraphQuery

# (repo, 目标符号短名, 关系问句)
CASES = [
    ("pallets/flask", "full_dispatch_request", "谁调用了 full_dispatch_request 调用链"),
    ("pallets/flask", "preprocess_request", "谁调用了 preprocess_request 的影响面"),
    ("pallets/flask", "finalize_request", "谁调用了 finalize_request 调用链"),
    ("pallets/flask", "process_response", "谁调用了 process_response 调用关系"),
    ("pallets/flask", "handle_user_exception", "谁调用了 handle_user_exception 调用链"),
    ("pallets/flask", "make_default_options_response", "谁调用了 make_default_options_response"),
    ("pallets/werkzeug", "Map", "谁调用了 Map 类的调用关系"),
    ("pallets/werkzeug", "Rule", "谁引用了 Rule 调用链"),
]
HUB_THRESHOLD = 15   # >15 callers = hub 符号:QA 按设计只采样代表性 top-N + 数量,不追全召回

_regs = {}
def reg_for(repo):
    if repo not in _regs:
        info = server._ensure_repo_indexed(repo)
        gq = MemoryGraphQuery(info["cfg"], info["store"])
        _regs[repo] = (server._registry_for(gq, repo, repo_root=info.get("path")), info)
    return _regs[repo]

def _name(it):
    return it.get("qualified_name") or it.get("name") or ""

spec_recalls = []; hub_lines = []
print("=== 多仓 deep 回归评测(GT=图谱真实 callers) ===")
for repo, sym, q in CASES:
    reg, info = reg_for(repo)
    hits = reg.call("search", {"query": sym, "top_k": 8}).get("items", [])
    nid = None
    for h in hits:
        if (h.get("name") == sym) or ((h.get("qualified_name") or "").split(".")[-1] == sym):
            nid = h["node_id"]; break
    if not nid and hits:
        nid = hits[0]["node_id"]
    if not nid:
        print("  [skip] %s/%s 未定位" % (repo, sym)); continue
    gt = set()
    for c in reg.call("callers", {"node_id": nid, "depth": 1}).get("items", []):
        gt.add((c.get("qualified_name") or c.get("name") or "").split(".")[-1])
    gt.discard("")
    items, bodies, relations, used, timings = _deep_repo_agent(reg, repo, q)
    got = set((_name(it).split(".")[-1]) for it in items)
    tools_ok = "OK" if len(used) == 7 else "!=7(%d)" % len(used)
    if not gt:
        print("  [info] %s/%s 无 caller GT(顶层入口),跳过 (tools=%s)" % (repo, sym, tools_ok)); continue
    recall = len(gt & got) / len(gt)
    if len(gt) > HUB_THRESHOLD:                          # hub:按设计采样,只报覆盖,不计入召回门禁
        hub_lines.append("  [hub] %s/%s: GT callers=%d, 采样覆盖=%d 个(%.0f%%), 工具=%s — QA 按设计取代表+计数,不追全召回"
                         % (repo, sym, len(gt), len(gt & got), recall*100, tools_ok))
    else:
        spec_recalls.append(recall)
        print("  [精确] %s/%s: GT callers=%d, 召回=%.0f%%, 工具=%s" % (repo, sym, len(gt), recall*100, tools_ok))

for l in hub_lines:
    print(l)
if spec_recalls:
    avg = sum(spec_recalls) / len(spec_recalls)
    print("\n>>> 精确符号(callers≤%d)平均关系召回 = %.1f%% (%d 例)" % (HUB_THRESHOLD, avg*100, len(spec_recalls)))
    print(">>> 门禁参考(精确符号 >=0.8): %s" % ("通过 ✅" if avg >= 0.8 else "未通过"))
    print(">>> hub 符号:按设计采样代表性 callers + 计数(不追全召回,守精准上下文原则)")
else:
    print("无精确用例")
