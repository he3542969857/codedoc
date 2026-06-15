"""QAAgent —— 多仓问答 StateGraph 的封装。

用法:
    agent = QAAgent(cfg, repo_registries, llm)
    out = agent.ask("两个仓库怎么协作的?", repos=["a", "b"])
    # out = {answer, trace, merged}
"""
from __future__ import annotations

from typing import Any

from codedoc.agents.graph import build_qa_graph


class QAAgent:
    def __init__(self, cfg, repo_registries: dict, llm, memory=None, max_repos: int = 4):
        self.cfg = cfg
        self.repo_registries = repo_registries
        self.llm = llm
        self.memory = memory
        self.graph = build_qa_graph(repo_registries, llm, memory=memory, max_repos=max_repos)

    def ask(self, question: str, repos: list[str] | None = None, thread_id: str | None = None) -> dict:
        state: dict[str, Any] = {"question": question, "repos": repos or list(self.repo_registries)}
        if thread_id:
            out = self.graph.invoke(state, config={"configurable": {"thread_id": thread_id}})
        else:
            out = self.graph.invoke(state)
        return {"answer": out.get("answer", ""),
                "trace": out.get("trace", []),
                "merged": out.get("merged", []),
                "selected": out.get("selected", [])}
