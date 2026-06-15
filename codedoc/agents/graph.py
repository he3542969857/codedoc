"""多仓问答的 LangGraph 多 Agent 架构(意图路由:简单走 fast、复杂走真 ReAct 多 agent)。

Planner →(Send 扇出)RepoAgent → Merger → Synthesiser  —— 四个 LLM 驱动的协作 agent:

- Planner    选仓 + 意图分流(简单查询 fast 规则检索;关系/跨仓/深问 → react 真 agent)
- RepoAgent  **真 ReAct**:LLM 按已观察自主决定下一个工具(search/callers/callees/impact/
             explore/dossier),步数未知;dossier 工具一步取某符号全邻域,让 agent 既自主又能完整。
             防死循环:max_iter + (工具,参数)签名去重 + 无新节点早停 + LangGraph recursion_limit。
- Merger     **LLM 跨仓推理**:据各仓发现 + 确定性同名线索,推理跨仓关系(共享接口/调用边界)。
- Synthesiser LLM 汇总,严格 grounding、按仓标注。

每个 super-step 经 PostgresSaver 落 PG、可按 thread_id 崩溃续传。fast / 任一 agent 失败均优雅降级。
"""
from __future__ import annotations

import concurrent.futures as _cf
import json
import operator
import re
import time
from typing import Annotated, Any, TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.types import Send

from codedoc.agents.llm import ChatMessage


# ---- LangGraph 持久化检查点(PostgresSaver):每个 super-step 落 PG,崩溃可 resume ----
_SAVER = None

def _get_saver():
    global _SAVER
    if _SAVER is not None:
        return _SAVER or None
    try:
        from psycopg_pool import ConnectionPool
        from langgraph.checkpoint.postgres import PostgresSaver
        from codedoc.graph.graph_persist import PG_DSN
        pool = ConnectionPool(conninfo=PG_DSN, max_size=4,
                              kwargs={"autocommit": True, "prepare_threshold": 0}, open=True)
        saver = PostgresSaver(pool)
        saver.setup()
        _SAVER = saver
    except Exception as e:
        print("[checkpoint] PostgresSaver 初始化失败,降级为无 checkpoint:", e)
        _SAVER = False
    return _SAVER or None


_REL_RE = re.compile(
    r"调用|caller|callee|被调|调用链|call ?chain|影响|impact|依赖|depend|继承|extends|"
    r"实现|implements|引用|reference|跨仓|关系|流程|链路|怎么.{0,6}(连|串|交互|协作|调)",
    re.IGNORECASE)
_TOOL_TIMEOUT = float(__import__("os").environ.get("CODEDOC_TOOL_TIMEOUT", "4.0"))
_MAX_ITEMS = 40
_REACT_MAX_ITER = int(__import__("os").environ.get("CODEDOC_REACT_MAX_ITER", "6"))
_NAV = ("search", "callers", "callees", "impact", "explore", "dossier")


def _parse_action(text: str) -> dict:
    if not text:
        return {}
    for pat in (r"```json\s*(\{.*?\})\s*```", r"```\s*(\{.*?\})\s*```", r"(\{.*\})"):
        m = re.search(pat, text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(1))
                if isinstance(obj, dict):
                    return obj
            except (json.JSONDecodeError, ValueError):
                continue
    return {}


def _dossier_on(reg, repo: str, sid: str, seed_qn: str, kind: str = "") -> tuple:
    """对一个符号做全工具邻域档案:context/callers/callees/impact/explore 并行 + get_body。
    既被确定性 fallback 用、也作为 ReAct 的 dossier 工具。返回 (items, bodies, relations_text, used)。"""
    used = ["context", "callers", "callees", "impact", "explore"]
    specs = [("context", {"node_id": sid, "max_lines": 30}),
             ("callers", {"node_id": sid, "depth": 1}),
             ("callees", {"node_id": sid, "depth": 1}),
             ("impact", {"node_id": sid}),
             ("explore", {"anchor": seed_qn})]
    nav: dict[str, dict] = {}
    with _cf.ThreadPoolExecutor(max_workers=5) as ex:
        fut_map = {ex.submit(reg.call, t, a): t for t, a in specs}
        try:
            for fut in _cf.as_completed(fut_map, timeout=_TOOL_TIMEOUT * 1.5):
                try:
                    nav[fut_map[fut]] = fut.result() or {}
                except Exception as e:
                    nav[fut_map[fut]] = {"error": str(e)}
        except _cf.TimeoutError:
            pass
    items: dict[str, dict] = {}
    for t in ("callers", "callees", "impact"):
        for it in (nav.get(t, {}).get("items") or []):
            nid = it.get("node_id") or it.get("id")
            if nid and len(items) < _MAX_ITEMS:
                items.setdefault(nid, it)
    bodies = []
    gb = reg.call("get_body", {"id": sid, "max_lines": 40})
    if gb.get("ok") and gb.get("body"):
        bodies.append({"repo": repo, "name": seed_qn, "body": gb["body"]})
    used.append("get_body")

    def _names(t, k=6):
        xs = nav.get(t, {}).get("items") or []
        return ", ".join((x.get("qualified_name") or x.get("name") or "?") for x in xs[:k]) or "无"

    ex2 = nav.get("explore", {})
    nbr = ex2.get("nodes") or ex2.get("items") or []
    relations = ("核心符号 `%s` (%s)\n" % (seed_qn, kind) +
                 "  · 调用者: %s\n" % _names("callers") +
                 "  · 被调: %s\n" % _names("callees") +
                 "  · 改动影响: %d 个节点\n" % len(nav.get("impact", {}).get("items") or []) +
                 "  · 邻域: %d 节点 / %d 边" % (len(nbr), len(ex2.get("edges") or [])))
    return list(items.values()), bodies, relations, used


def _deep_repo_agent(reg, repo: str, question: str):
    """确定性 fallback:search 定位核心符号 → 全工具档案。react agent 失败时退回这条。"""
    res = reg.call("search", {"query": question, "top_k": 8})
    raw = res.get("items") or []
    if not raw:
        return [], [], "", ["search"]
    _qtok = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", question))
    seed = raw[0]
    for it in raw:
        if (it.get("name") or "") in _qtok or (it.get("qualified_name") or "").split(".")[-1] in _qtok:
            seed = it
            break
    items, bodies, relations, used = _dossier_on(reg, repo, seed["node_id"],
                                                 seed.get("qualified_name") or seed.get("name") or seed["node_id"],
                                                 seed.get("kind", ""))
    base = {it["node_id"]: it for it in raw if it.get("node_id")}
    for it in items:
        base.setdefault(it.get("node_id") or it.get("id"), it)
    return list(base.values()), bodies, relations, ["search"] + used


def _react_repo_agent(reg, repo: str, question: str, llm, max_iter: int = _REACT_MAX_ITER):
    """真 ReAct multi-agent 的单仓 agent:LLM 按观察自主决定工具,dossier 可一步取全邻域。
    防死循环:max_iter + 调用签名去重 + 无新节点早停。任何异常 → 退回确定性 fallback。"""
    sysmsg = (
        "你是探索单个代码仓的 agent,目标:回答问题。每步只输出一个 JSON,决定下一个工具;"
        "看够了就 {\"action\":\"DONE\"}。可用工具:\n"
        "- search   {\"query\":\"...\"}      语义检索(起点/换方向)\n"
        "- callers  {\"node_id\":\"...\"}   谁调用它\n"
        "- callees  {\"node_id\":\"...\"}   它调谁\n"
        "- impact   {\"node_id\":\"...\"}   改它影响谁\n"
        "- explore  {\"anchor\":\"...\"}    邻域子图\n"
        "- dossier  {\"node_id\":\"...\"}   一步取该符号的完整邻域档案(调用者+被调+影响+源码)\n"
        "node_id 必须用『已观察』里出现过的真实 id。定位到核心符号后,优先用 dossier 一步取全。"
        "只输出 JSON:{\"thought\":\"\",\"action\":\"...\",\"args\":{}}")
    used: list[str] = []
    items: dict[str, dict] = {}
    bodies: list[dict] = []
    relations = ""
    history: list[str] = []
    seen: set[str] = set()
    trace: list[dict] = []
    dup = 0
    try:
        for step in range(max_iter):
            obs = "\n".join(history[-6:]) or "(还没有观察)"
            raw = llm.chat([ChatMessage("system", sysmsg),
                            ChatMessage("user", "问题:%s\n已观察:\n%s\n\n下一步 JSON:" % (question, obs))],
                           max_tokens=300)
            act = _parse_action(raw)
            a = act.get("action", "DONE")
            if a not in _NAV:
                break
            args = act.get("args") if isinstance(act.get("args"), dict) else {}
            if a == "search":
                args.setdefault("query", question)
                args.setdefault("top_k", 8)
            sig = a + "|" + json.dumps(args, sort_keys=True, ensure_ascii=False)
            if sig in seen:
                dup += 1
                if dup >= 2:
                    break
                continue
            seen.add(sig)
            if a == "dossier":
                nid = args.get("node_id")
                if not nid:
                    break
                qn = items.get(nid, {}).get("qualified_name") or items.get(nid, {}).get("name") or nid
                di, db, dr, du = _dossier_on(reg, repo, nid, qn, items.get(nid, {}).get("kind", ""))
                new = 0
                for it in di:
                    k = it.get("node_id") or it.get("id")
                    if k and k not in items:
                        items[k] = it
                        new += 1
                bodies += db
                relations = relations or dr
                used += du
                history.append("步骤%d dossier(%s) → +%d 节点 + 关系档案" % (step + 1, str(nid)[:28], new))
                trace.append({"step": step + 1, "action": "dossier", "hits": new})
            else:
                try:
                    res = reg.call(a, args)
                except Exception as e:
                    res = {"error": str(e)}
                got = res.get("items") or []
                new = 0
                for it in got:
                    k = it.get("node_id") or it.get("id")
                    if k and k not in items:
                        items[k] = it
                        new += 1
                sample = ", ".join("%s(%s)" % ((it.get("node_id") or it.get("id")), it.get("name") or "")
                                   for it in got[:5])
                history.append("步骤%d %s → %d 节点: %s" % (step + 1, a, len(got), sample or "无"))
                trace.append({"step": step + 1, "action": a, "hits": len(got)})
                used.append(a)
                if new == 0 and a != "search":
                    break
    except Exception:
        pass
    if not items:                                          # agent 没探到 → 退回确定性 fallback
        return _deep_repo_agent(reg, repo, question) + (trace,)
    if not bodies:                                          # 补 get_body
        for it in list(items.values())[:3]:
            nid = it.get("node_id") or it.get("id")
            gb = reg.call("get_body", {"id": nid, "max_lines": 40})
            used.append("get_body")
            if gb.get("ok") and gb.get("body"):
                bodies.append({"repo": repo, "name": it.get("qualified_name") or it.get("name"), "body": gb["body"]})
    return list(items.values()), bodies, relations, list(dict.fromkeys(used)), trace


class QAState(TypedDict, total=False):
    question: str
    repos: list[str]
    repo: str
    mode: str
    selected: list[str]
    repo_results: Annotated[list[dict], operator.add]
    merged: list[dict]
    cross_text: str
    answer: str
    need_more: bool
    gap: str
    reflect_rounds: int
    trace: Annotated[list[dict], operator.add]


def build_qa_graph(repo_registries: dict, llm, memory=None, max_repos: int = 4):

    def planner(state: QAState) -> dict:
        repos = state.get("repos") or list(repo_registries)
        if len(repos) <= max_repos:
            selected = list(repos)
        else:
            scored = []
            for r in repos:
                reg = repo_registries.get(r)
                n = len(reg.call("search", {"query": state["question"], "top_k": 5}).get("items", [])) if reg else 0
                scored.append((n, r))
            selected = [r for _, r in sorted(scored, reverse=True)[:max_repos]]
        try:
            from codedoc.index import pg_vectors
            pg_vectors._embed_query_cached(state.get("question", ""))   # 预热嵌入缓存
        except Exception:
            pass
        mode = "react" if _REL_RE.search(state.get("question", "")) else "fast"
        return {"selected": selected, "mode": mode,
                "trace": [{"stage": "planner", "selected": selected, "mode": mode}]}

    def fan_out(state: QAState):
        m = state.get("mode", "fast")
        return [Send("repo_agent", {"question": state["question"], "repo": r, "mode": m})
                for r in state["selected"]]

    def repo_agent(state: QAState) -> dict:
        repo, q, mode = state["repo"], state["question"], state.get("mode", "fast")
        reg = repo_registries.get(repo)
        if not reg:
            return {"repo_results": [], "trace": [{"stage": "repo_agent", "repo": repo, "hits": 0}]}
        if mode == "react":                                # 真 ReAct agent(LLM 自主决定工具)
            items, bodies, relations, used, rtrace = _react_repo_agent(reg, repo, q, llm)
            return {"repo_results": [{"repo": repo, "items": items, "bodies": bodies, "relations": relations}],
                    "trace": [{"stage": "repo_agent", "repo": repo, "mode": "react",
                               "steps": len(rtrace), "tools_used": used, "hits": len(items),
                               "react": rtrace}]}
        items = reg.call("search", {"query": q, "top_k": 8}).get("items", [])  # fast:规则检索
        bodies = []
        for it in items[:3]:
            gb = reg.call("get_body", {"id": it["node_id"], "max_lines": 40})
            if gb.get("ok") and gb.get("body"):
                bodies.append({"repo": repo, "name": it["qualified_name"] or it["name"], "body": gb["body"]})
        return {"repo_results": [{"repo": repo, "items": items, "bodies": bodies}],
                "trace": [{"stage": "repo_agent", "repo": repo, "mode": "fast", "hits": len(items)}]}

    def merger(state: QAState) -> dict:
        """Merger agent:确定性同名线索 + LLM 跨仓关系推理。"""
        results = state.get("merged_results") or state.get("repo_results", [])
        seen: dict[str, set] = {}
        for rr in results:
            for it in rr["items"]:
                key = it.get("qualified_name") or it.get("name")
                if key:
                    seen.setdefault(key, set()).add(rr["repo"])
        cross = {k: sorted(v) for k, v in seen.items() if len(v) > 1}
        if len(results) < 2:                               # 单仓不需要跨仓推理
            return {"merged": results, "cross_text": "",
                    "trace": [{"stage": "merger", "repos": len(results), "cross_links": 0}]}
        # 给 Merger agent 的素材:各仓核心发现 + 确定性同名线索
        facts = []
        for rr in results:
            names = ", ".join((it.get("qualified_name") or it.get("name") or "?") for it in rr["items"][:8])
            facts.append("仓 %s 命中: %s" % (rr["repo"], names))
            if rr.get("relations"):
                facts.append(rr["relations"])
        hint = "; ".join("`%s`∈{%s}" % (k, ",".join(v)) for k, v in list(cross.items())[:12]) or "无同名符号"
        cross_text = ""
        try:
            ans = llm.chat([
                ChatMessage("system", "你是跨仓关系分析 agent。据各仓发现和『同名符号线索』,推理这些仓之间"
                                      "的真实关系(共享接口/调用边界/分层依赖);只依据给定事实、不编造。"
                                      "输出简短 Markdown,标题用『## 跨仓关系』。"),
                ChatMessage("user", "各仓发现:\n%s\n\n同名符号线索:%s" % ("\n".join(facts)[:3000], hint))
            ], max_tokens=500) or ""
            cross_text = ans
        except Exception:
            cross_text = ("## 跨仓关系(同名符号)\n" +
                          "\n".join("- `%s` 同时出现在 %s" % (k, ", ".join(v)) for k, v in list(cross.items())[:12]))
        return {"merged": results, "cross_text": cross_text,
                "trace": [{"stage": "merger", "repos": len(results), "cross_links": len(cross),
                           "llm_reconcile": bool(cross_text)}]}

    def synthesiser(state: QAState) -> dict:
        results = state.get("merged", [])
        parts = []
        for rr in results:
            parts.append("### 仓库 %s" % rr["repo"])
            if rr.get("relations"):
                parts.append(rr["relations"])
            for it in rr["items"][:6]:
                parts.append("- [%s] %s `%s`" % (rr["repo"], it.get("kind", ""),
                                                 it.get("qualified_name") or it.get("name")))
            for b in rr["bodies"]:
                parts.append("```\n# %s (%s)\n%s\n```" % (b["name"], b["repo"], b["body"][:1400]))
        context = "\n".join(parts)
        if state.get("cross_text"):
            context += "\n\n" + state["cross_text"]
        sys = ("你是多仓代码问答助手。只依据下面各仓库上下文 + 跨仓关系回答;提到代码符号用反引号并标注所属仓库;"
               "不要编造上下文里没有的符号。")
        ans = llm.chat([ChatMessage("system", sys),
                        ChatMessage("user", "问题:%s\n\n上下文:\n%s" % (state["question"], context))],
                       max_tokens=900)
        return {"answer": ans, "trace": [{"stage": "synthesiser", "len": len(ans or "")}]}

    _HEDGE = ("未找到", "无法回答", "没有足够", "not found", "insufficient", "没有相关", "无法确定", "上下文中没有")

    def reflect(state: QAState) -> dict:
        """反思 agent:自评答案是否充分且有据;不够则指出缺口、触发一轮定向补检索。仅深问、有界 1 轮。"""
        if state.get("mode") != "react":
            return {"need_more": False, "trace": [{"stage": "reflect", "skip": "fast"}]}
        rounds = state.get("reflect_rounds", 0)
        ans = state.get("answer", "") or ""
        suff, gap = True, ""
        try:
            raw = llm.chat([
                ChatMessage("system", "你是审查 agent,判断答案是否**充分且有据**地回答了问题。"
                                      "只输出 JSON:{\"sufficient\":true/false,\"gap\":\"还缺哪个具体符号/信息(一句话,没缺留空)\"}"),
                ChatMessage("user", "问题:%s\n\n答案:%s" % (state["question"], ans[:1500]))], max_tokens=200)
            j = _parse_action(raw)
            suff = bool(j.get("sufficient", True))
            gap = str(j.get("gap", "") or "")
        except Exception:
            pass
        hedge = any(w in ans for w in _HEDGE)
        need = (not suff or hedge) and rounds < 1 and bool(gap or hedge)
        return {"need_more": need, "gap": gap, "reflect_rounds": rounds + 1,
                "trace": [{"stage": "reflect", "sufficient": suff, "need_more": need, "gap": gap[:60]}]}

    def refine(state: QAState) -> dict:
        """据反思缺口,在选中仓做定向补检索,append 进 merged,交回 synthesiser 重答。"""
        gap = state.get("gap", "") or state["question"]
        extra = []
        for r in state.get("selected", []):
            reg = repo_registries.get(r)
            if not reg:
                continue
            try:
                items = reg.call("search", {"query": gap, "top_k": 5}).get("items", [])
            except Exception:
                items = []
            if items:
                extra.append({"repo": r, "items": items, "bodies": []})
        merged = (state.get("merged") or []) + extra
        return {"merged": merged,
                "trace": [{"stage": "refine", "gap": gap[:60], "added_repos": len(extra)}]}

    def _after_reflect(state: QAState):
        return "refine" if state.get("need_more") else "end"

    g = StateGraph(QAState)
    g.add_node("planner", planner)
    g.add_node("repo_agent", repo_agent)
    g.add_node("merger", merger)
    g.add_node("synthesiser", synthesiser)
    g.add_node("reflect", reflect)
    g.add_node("refine", refine)
    g.add_edge(START, "planner")
    g.add_conditional_edges("planner", fan_out, ["repo_agent"])
    g.add_edge("repo_agent", "merger")
    g.add_edge("merger", "synthesiser")
    g.add_edge("synthesiser", "reflect")
    g.add_conditional_edges("reflect", _after_reflect, {"refine": "refine", "end": END})
    g.add_edge("refine", "synthesiser")          # 补检索后回合成(reflect_rounds 守住、只 1 轮)
    saver = _get_saver()
    return g.compile(checkpointer=saver) if saver else g.compile()
