"""会话级结构化记忆 —— 锚点链 / 焦点 / 笔记。

- anchors  话题转移链(最近聊的核心符号);current_anchor 给指代消解("它的调用者")
- focus    最近活跃符号 + 权重(每轮衰减,复现强化;hub 降权);喂检索增补与上下文
- notes    每轮一句话笔记

更新是选择性的(衰减 + 强化 + 门控换 anchor),不是无脑灌入,这样维护(裁剪)才有意义。
可 to_json/from_json 持久化到 conversation_memory 表,跨请求复用。
"""
from __future__ import annotations

import math


class MemoryManager:
    def __init__(self, anchor_history: int = 4, semantic_focus: int = 8):
        self.anchor_history = anchor_history
        self.semantic_focus = semantic_focus
        self.anchors: list[dict] = []          # [{id, name}]
        self.focus: dict[str, dict] = {}        # name -> {score, node_id}
        self.summary: str = ""                  # 溢出轮异步滚动摘要(超 token 窗口的老对话折叠到这里)

    # ---- 更新 ----
    def decay(self, factor: float = 0.7) -> None:
        for f in self.focus.values():
            f["score"] *= factor

    def touch_focus(self, name: str, node_id: str, degree: int = 1, base: float = 1.0) -> None:
        if not name:
            return
        w = base / math.log(2 + max(0, degree))   # hub(高度数)降权
        ex = self.focus.get(name)
        if ex:
            ex["score"] += w + 1.0                 # 复现强化
        else:
            self.focus[name] = {"score": w, "node_id": node_id}
        # 裁剪到 semantic_focus*2(给衰减留缓冲)
        if len(self.focus) > self.semantic_focus * 2:
            for k in sorted(self.focus, key=lambda k: self.focus[k]["score"])[:len(self.focus) - self.semantic_focus * 2]:
                self.focus.pop(k, None)

    def push_anchor(self, node_id: str, name: str) -> None:
        if self.anchors and self.anchors[-1].get("id") == node_id:
            return
        self.anchors.append({"id": node_id, "name": name})
        self.anchors = self.anchors[-self.anchor_history:]

    # ---- 读 ----
    def current_anchor(self) -> dict | None:
        return self.anchors[-1] if self.anchors else None

    def top_focus(self, k: int = 5, floor: float = 0.0) -> list[tuple[str, float]]:
        items = [(n, d["score"]) for n, d in self.focus.items() if d["score"] >= floor]
        return sorted(items, key=lambda kv: -kv[1])[:k]

    def snapshot(self) -> dict:
        return {"anchors": list(self.anchors),
                "focus": {n: d for n, d in self.focus.items()}}

    # ---- 持久化 ----
    def to_json(self) -> dict:
        return {"anchors": self.anchors, "focus": self.focus,
                "summary": self.summary}

    @classmethod
    def from_json(cls, d: dict, anchor_history: int = 4, semantic_focus: int = 8) -> "MemoryManager":
        m = cls(anchor_history, semantic_focus)
        if isinstance(d, dict):
            m.anchors = d.get("anchors", []) or []
            m.focus = d.get("focus", {}) or {}
            m.summary = d.get("summary", "") or ""
        return m
