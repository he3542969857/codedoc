"""LLM 客户端:OpenAI 兼容 chat completions(默认 SiliconFlow DeepSeek-V3)。

接口契约(取自 server.py 用法):
    from codedoc.agents.llm import ChatMessage, build_llm
    llm = build_llm(cfg)
    text = llm.chat([ChatMessage("system", ...), ChatMessage("user", ...)], max_tokens=800)
chat() 返回**字符串**(助手回复正文);失败返回以 "[LLM error" 开头的字符串,调用方可识别。
"""
from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass

from ..config import Config


@dataclass
class ChatMessage:
    role: str
    content: str


class LlmClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._lc = cfg.llm

    def chat(self, messages: list[ChatMessage], max_tokens: int = 800,
             temperature: float | None = None) -> str:
        payload = {
            "model": self._lc.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": max_tokens,
            "temperature": self._lc.temperature if temperature is None else temperature,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._lc.base_url.rstrip("/") + "/chat/completions",
            data=data,
            headers={
                "Authorization": f"Bearer {self._lc.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._lc.timeout) as r:
                body = json.loads(r.read().decode("utf-8", errors="replace"))
            return body["choices"][0]["message"]["content"] or ""
        except urllib.error.HTTPError as e:
            return f"[LLM error {e.code}: {e.read().decode(errors='replace')[:200]}]"
        except Exception as e:
            return f"[LLM error: {e}]"


def build_llm(cfg: Config) -> LlmClient:
    return LlmClient(cfg)
