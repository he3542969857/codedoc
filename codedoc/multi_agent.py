"""codedoc 多仓 —— 真·多 agent + supervisor 状态机(含 SelectorAgent 选仓)。

4 种真 agent,全自主 ReAct;事实由确定性图谱工具供给(agent 导航,不让 LLM 猜):
  · SelectorAgent : 选仓——search 候选(确定性)+ 沿跨仓依赖图扩(确定性),自主判断扩哪条
  · Supervisor    : 主 agent,非线性路由(select→investigate→扩仓→merge/跳过→synthesize→done)
  · RepoAgent×N   : 每仓自主 ReAct(search/dossier + repo_deps 暴露跨仓依赖)
  · MergeAgent    : 跨仓多步核验

  START → supervisor ─┬ select     → SelectorAgent ───────┐
                      ├ investigate → RepoAgent×(pending) ─┤
                      ├ merge       → MergeAgent ──────────┤
                      ├ synthesize  → Synth ───────────────┤
                      └ done        → END                  │
            ↑──────────(每阶段回 supervisor 看进展再决策,有环)┘

有界:supervisor max_steps + 每 agent max_iter + 已查仓去重。
policy(各 agent 大脑)与 tools 可插拔:生产 LLM + codedoc 真图谱,测试注入确定性,可复现可单测。
"""
from __future__ import annotations

import operator
from typing import Annotated, Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

SUP_MAX_STEPS = 10
REACT_MAX_ITER = 6


class ReActAgent:
    """真 agent:观察→policy 自选(工具,参数)→执行→喂回→循环,直到 finish 或 max_iter。"""

    def __init__(self, name, policy, tools, max_iter=REACT_MAX_ITER):
        self.name, self.policy, self.tools, self.max_iter = name, policy, tools, max_iter

    def run(self, goal, ctx=None):
        ctx = ctx or {}
        scratch, seen = [], set()
        for _ in range(self.max_iter):
            act = self.policy(goal, scratch) or {"tool": "finish", "result": {}}
            if act.get("tool") == "finish":
                return {"agent": self.name, "result": act.get("result", {}),
                        "steps": len(scratch), "trace": scratch}
            sig = (act.get("tool"), str(act.get("args")))
            if sig in seen:
                break
            seen.add(sig)
            obs = self.tools.get(act["tool"], lambda a, c: {"error": "unknown"})(act.get("args", {}), ctx)
            scratch.append({"tool": act["tool"], "args": act.get("args", {}), "obs": obs})
        return {"agent": self.name, "result": {"findings": [s["obs"] for s in scratch]},
                "steps": len(scratch), "trace": scratch, "bailed": True}


def _merge_dict(a, b):
    out = dict(a or {}); out.update(b or {}); return out


def _union(a, b):
    out = list(a or [])
    for x in (b or []):
        if x not in out:
            out.append(x)
    return out


class MultiRepoState(TypedDict, total=False):
    question: str
    all_repos: list[str]                           # 仓全集(SelectorAgent 从里面选)
    selected: bool
    seed_repos: list[str]                          # SelectorAgent 选出的种子
    to_investigate: list[str]
    investigated: Annotated[list, _union]
    repo_findings: Annotated[dict, _merge_dict]
    discovered_deps: Annotated[list, _union]
    merged: dict
    answer: str
    step: int
    next: str
    trace: Annotated[list, operator.add]
    _repo: str


def build_multi_agent_graph(selector: ReActAgent, supervisor: ReActAgent,
                            repo_agent_factory: Callable, merge_agent: ReActAgent,
                            synthesizer: Callable, sup_max_steps=SUP_MAX_STEPS):

    def supervisor_node(state):
        step = state.get("step", 0) + 1
        if not state.get("selected"):                       # 先选仓(前置条件)
            return {"next": "select", "step": step,
                    "trace": [{"supervisor": step, "decided": "select"}]}
        investigated = state.get("investigated", [])
        pending = [r for r in _union(state.get("seed_repos", []), state.get("discovered_deps", []))
                   if r not in investigated]
        goal = ("问题:%s | pending未查:%s | 已查:%d | 已合并:%s | 已成稿:%s | step:%d/%d"
                % (state.get("question", ""), pending, len(investigated),
                   bool(state.get("merged")), bool(state.get("answer")), step, sup_max_steps))
        if step > sup_max_steps:
            nxt = "done" if state.get("answer") else "synthesize"
        else:
            nxt = (supervisor.run(goal, ctx=dict(state)).get("result") or {}).get("next", "done")
        out = {"next": nxt, "step": step,
               "trace": [{"supervisor": step, "decided": nxt, "pending": pending}]}
        if nxt == "investigate":
            out["to_investigate"] = pending or state.get("seed_repos", [])
        return out

    def select_node(state):
        out = selector.run(state.get("question", ""),
                           ctx={"all_repos": state.get("all_repos", []),
                                "question": state.get("question", "")})
        repos = (out.get("result") or {}).get("repos", []) or state.get("all_repos", [])
        return {"seed_repos": repos, "selected": True,
                "trace": [{"selector_steps": out.get("steps", 0), "selected": repos}]}

    def route(state):
        return state.get("next", "done")

    def dispatch_repos(state):
        return [Send("repo_agent", {**state, "_repo": r}) for r in state.get("to_investigate", [])]

    def repo_agent_node(state):
        repo = state["_repo"]
        out = repo_agent_factory(repo).run(
            "在仓 %s 找与问题相关的代码:%s" % (repo, state.get("question", "")), ctx={"repo": repo})
        res = out.get("result", {}) or {}
        deps = res.get("deps", []) or []
        return {"repo_findings": {repo: res}, "investigated": [repo], "discovered_deps": deps,
                "trace": [{"repo_agent": repo, "steps": out.get("steps", 0), "deps": deps}]}

    def merge_node(state):
        out = merge_agent.run("跨仓推理真实关系:%s" % state.get("question", ""),
                              ctx={"repo_findings": state.get("repo_findings", {})})
        return {"merged": out.get("result", {}), "trace": [{"merge_agent_steps": out.get("steps", 0)}]}

    def synthesize_node(state):
        return {"answer": synthesizer(state.get("question", ""), state.get("repo_findings", {}),
                                      state.get("merged", {})), "trace": [{"synthesized": True}]}

    g = StateGraph(MultiRepoState)
    g.add_node("supervisor", supervisor_node)
    g.add_node("select", select_node)
    g.add_node("dispatch", lambda s: {})
    g.add_node("repo_agent", repo_agent_node)
    g.add_node("merge", merge_node)
    g.add_node("synthesize", synthesize_node)
    g.add_edge(START, "supervisor")
    g.add_conditional_edges("supervisor", route, {
        "select": "select", "investigate": "dispatch",
        "merge": "merge", "synthesize": "synthesize", "done": END})
    g.add_conditional_edges("dispatch", dispatch_repos, ["repo_agent"])
    g.add_edge("select", "supervisor")
    g.add_edge("repo_agent", "supervisor")
    g.add_edge("merge", "supervisor")
    g.add_edge("synthesize", "supervisor")
    return g.compile()


def make_selector(search_repos_tool, judge_relevance_tool, max_iter=6):
    """生产选仓 agent —— 据选仓测评(eval/select_eval.py)结论:**搜 + rerank 相关性判断,不做依赖图扩**。
    测评(13 题真实微服务多仓):纯定义搜 F1=0.94;任何依赖扩 precision 崩到 0.25~0.49(编排/hub 仓拉爆全图)。
    所以选仓 = 搜候选 + 逐个 rerank 判断相关性(压"编排仓 docstring 蹭词"的误命中),**不沿依赖图扩**。
    依赖只在 RepoAgent 深挖时按"确认调用"暴露,由 supervisor 运行时扩 —— 那是确认依赖,不是选仓时的投机扩。

    search_repos_tool(query) -> [repo, ...](按命中分排序的候选)
    judge_relevance_tool(repo, query) -> bool(该仓是否真和问题相关)
    """
    def policy(goal, scratch):
        if not scratch:
            return {"tool": "search_repos", "args": {"query": goal}}
        cands = scratch[0]["obs"] or []
        judged = [s for s in scratch if s["tool"] == "judge"]
        if len(judged) < len(cands):                     # 对候选逐个 rerank 判断
            return {"tool": "judge", "args": {"repo": cands[len(judged)], "query": goal}}
        keep = [cands[i] for i, s in enumerate(judged) if s["obs"]]   # 只留判为相关的
        return {"tool": "finish", "result": {"repos": keep}}
    tools = {"search_repos": lambda a, c: search_repos_tool(a["query"]),
             "judge": lambda a, c: judge_relevance_tool(a["repo"], a["query"])}
    return ReActAgent("selector", policy, tools, max_iter=max_iter)


def run_multi_agent(question, all_repos, selector, supervisor, repo_agent_factory,
                    merge_agent, synthesizer, sup_max_steps=SUP_MAX_STEPS):
    graph = build_multi_agent_graph(selector, supervisor, repo_agent_factory, merge_agent,
                                    synthesizer, sup_max_steps)
    final = graph.invoke({"question": question, "all_repos": all_repos, "step": 0},
                         {"recursion_limit": 6 * sup_max_steps + 20})
    return {
        "question": question, "answer": final.get("answer", ""),
        "selected": final.get("seed_repos", []), "investigated": final.get("investigated", []),
        "discovered_deps": final.get("discovered_deps", []), "merged": final.get("merged", {}),
        "supervisor_steps": final.get("step", 0), "trace": final.get("trace", []),
    }
