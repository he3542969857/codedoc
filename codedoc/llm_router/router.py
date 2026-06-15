"""路由 + 失败转移。

choose_model(requested):
  - 指定且健康 -> 用它
  - auto / 不健康 -> 健康池里挑 in-flight 最低(负载均衡),tie-break 最少使用
RoutedLLM.chat():切 cfg -> build_llm -> 追踪 -> 失败记降级并转移到健康模型重试。
"""
from __future__ import annotations

import time

from codedoc.llm_router.registry import (MODEL_REGISTRY, switch_cfg_to, configured, default_model_id)
from codedoc.llm_router.tracker import TRACKER
from codedoc.llm_router.degrader import DEGRADER


def _healthy_pool() -> list[str]:
    return [mid for mid, spec in MODEL_REGISTRY.items()
            if configured(spec) and not DEGRADER.is_degraded(mid)]


def choose_model(requested: str | None = None) -> str:
    if requested and requested != "auto":
        spec = MODEL_REGISTRY.get(requested)
        if spec and configured(spec) and not DEGRADER.is_degraded(requested):
            return requested
    pool = _healthy_pool()
    if not pool:
        return default_model_id()
    # 负载均衡:in-flight 最低 -> 最少完成(均匀使用)
    return min(pool, key=lambda m: (TRACKER.in_flight(m), TRACKER.completed(m)))


class RoutedLLM:
    """与 build_llm 返回的 LlmClient 同接口(.chat),但带模型路由 + 失败转移。"""

    def __init__(self, cfg, requested: str | None = None, max_failover: int = 2):
        self._cfg = cfg
        self._requested = requested
        self._max_failover = max_failover
        self.last_model = None

    def _call_once(self, model_id: str, messages, max_tokens, temperature):
        from codedoc.agents.llm import build_llm
        c = switch_cfg_to(self._cfg, model_id)
        llm = build_llm(c)
        TRACKER.start(model_id)
        t0 = time.time()
        try:
            resp = llm.chat(messages, max_tokens=max_tokens, temperature=temperature)
            ok = bool(resp) and not str(resp).startswith("[LLM error")
            TRACKER.finish(model_id, time.time() - t0, ok)
            if ok:
                DEGRADER.record_success(model_id)
            else:
                DEGRADER.record_error(model_id)
            return resp, ok
        except Exception:
            TRACKER.finish(model_id, time.time() - t0, False)
            DEGRADER.record_error(model_id)
            return None, False

    def chat(self, messages, max_tokens: int = 800, temperature=None):
        tried = set()
        model_id = choose_model(self._requested)
        for _ in range(self._max_failover + 1):
            if model_id in tried:
                # 换一个没试过的健康模型
                alt = [m for m in _healthy_pool() if m not in tried]
                if not alt:
                    break
                model_id = alt[0]
            tried.add(model_id)
            self.last_model = model_id
            resp, ok = self._call_once(model_id, messages, max_tokens, temperature)
            if ok:
                return resp
            model_id = choose_model(None)  # 失败转移:重新挑健康模型
        return "[LLM error: all candidates failed]"


def build_routed_llm(cfg, requested: str | None = None) -> RoutedLLM:
    return RoutedLLM(cfg, requested=requested)
