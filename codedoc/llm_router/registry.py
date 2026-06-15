"""7 模型注册表 —— codedoc 前端的多模型网关。

跨 SiliconFlow / DeepSeek 两个 OpenAI 兼容 provider,分档(flagship / balanced / reasoning / code)。
前端 GET /api/v1/models 列出 + auto 智能路由。switch_cfg_to 按模型切 base_url/key/model。

诚实说明:实际只有 SiliconFlow 的 key 可用,deepseek-official 那条没配 key 会调不通
——正好用来演示降级失败转移(degrader 自动踢出 auto 池、转健康模型)。
"""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass

_SF_BASE = "https://api.siliconflow.cn/v1"
_SF_KEY = os.environ.get("SILICONFLOW_API_KEY", "YOUR_SILICONFLOW_API_KEY")
_DS_BASE = "https://api.deepseek.com/v1"
_DS_KEY = os.environ.get("DEEPSEEK_API_KEY", "")  # 没配 -> 该模型会被降级


@dataclass
class ModelSpec:
    id: str
    model: str          # provider 侧真实模型名
    provider: str
    base_url: str
    api_key: str
    tier: str           # flagship / balanced / reasoning / code
    note: str = ""


_SPECS = [
    ModelSpec("deepseek-v3", "deepseek-ai/DeepSeek-V3", "siliconflow", _SF_BASE, _SF_KEY,
              "balanced", "默认主力,综合问答"),
    ModelSpec("qwen2.5-72b", "Qwen/Qwen2.5-72B-Instruct", "siliconflow", _SF_BASE, _SF_KEY,
              "flagship", "大参数,复杂综合"),
    ModelSpec("deepseek-r1", "deepseek-ai/DeepSeek-R1", "siliconflow", _SF_BASE, _SF_KEY,
              "reasoning", "思维链,多步推理"),
    ModelSpec("qwen3-coder", "Qwen/Qwen3-Coder-30B-A3B-Instruct", "siliconflow", _SF_BASE, _SF_KEY,
              "code", "代码专用 MoE"),
    ModelSpec("glm-4.5-air", "zai-org/GLM-4.5-Air", "siliconflow", _SF_BASE, _SF_KEY,
              "balanced", "轻量均衡"),
    ModelSpec("qwen2.5-7b", "Qwen/Qwen2.5-7B-Instruct", "siliconflow", _SF_BASE, _SF_KEY,
              "balanced", "快/省,简单问答"),
    ModelSpec("deepseek-v3-official", "deepseek-chat", "deepseek", _DS_BASE, _DS_KEY,
              "balanced", "DeepSeek 官方直连(需 DEEPSEEK_API_KEY,缺则降级演示失败转移)"),
]

MODEL_REGISTRY: dict[str, ModelSpec] = {s.id: s for s in _SPECS}
_DEFAULT = "deepseek-v3"


def default_model_id() -> str:
    return _DEFAULT


def configured(spec: ModelSpec) -> bool:
    """provider key 是否配齐(没配的视为不健康,不进 auto 池)。"""
    return bool(spec.api_key)


def switch_cfg_to(cfg, model_id: str):
    """返回一份把 llm.base_url/api_key/model 切到目标模型的 cfg 副本(不改原 cfg)。"""
    spec = MODEL_REGISTRY.get(model_id)
    if not spec:
        return cfg
    c = copy.copy(cfg)
    c.llm = copy.copy(cfg.llm)
    c.llm.base_url = spec.base_url
    c.llm.api_key = spec.api_key
    c.llm.model = spec.model
    return c


def list_models() -> list[dict]:
    from codedoc.llm_router.degrader import DEGRADER
    from codedoc.llm_router.tracker import TRACKER
    out = []
    for mid, spec in MODEL_REGISTRY.items():
        st = TRACKER.stats().get(mid, {})
        out.append({
            "id": mid, "model": spec.model, "provider": spec.provider, "tier": spec.tier,
            "note": spec.note, "configured": configured(spec),
            "degraded": DEGRADER.is_degraded(mid),
            "healthy": configured(spec) and not DEGRADER.is_degraded(mid),
            "in_flight": st.get("in_flight", 0), "completed": st.get("completed", 0),
            "errors": st.get("errors", 0), "avg_latency_ms": st.get("avg_latency_ms", 0),
        })
    return out
