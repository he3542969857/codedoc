"""配置:load_config(repo_dir) 返回一个 Config。

Config 被 parse_repo / MemoryGraphStore / MemoryGraphQuery / build_llm 共用。
LLM 走 OpenAI 兼容接口(默认 SiliconFlow 的 DeepSeek-V3),key/base_url 可被环境变量覆盖。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class LlmCfg:
    provider: str = "openai"
    base_url: str = os.environ.get("CODEDOC_LLM_BASE_URL", "https://api.siliconflow.cn/v1")
    api_key: str = os.environ.get(
        "CODEDOC_LLM_API_KEY",
        os.environ.get("SILICONFLOW_API_KEY", "YOUR_SILICONFLOW_API_KEY"),
    )
    model: str = os.environ.get("CODEDOC_LLM_MODEL", "deepseek-ai/DeepSeek-V3")
    temperature: float = 0.2
    timeout: int = 60


@dataclass
class Config:
    repo_path: Path
    languages: list[str] = field(default_factory=lambda: ["python", "java", "javascript"])
    # 不解析这些目录/文件
    exclude: list[str] = field(default_factory=lambda: [
        "**/.git/**", "**/node_modules/**", "**/__pycache__/**", "**/dist/**",
        "**/build/**", "**/.venv/**", "**/venv/**", "**/target/**", "**/.idea/**",
    ])
    llm: LlmCfg = field(default_factory=LlmCfg)
    # 检索/记忆调参(预留,内存核心暂用默认)
    semantic_focus: int = 8
    anchor_history: int = 16


def load_config(repo_dir: str | Path) -> Config:
    """从仓库目录构造 Config。目录即解析根。"""
    return Config(repo_path=Path(repo_dir))
