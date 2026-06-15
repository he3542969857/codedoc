"""降级器:连续失败到阈值 -> 降级冷却(踢出 auto 池),冷却到期自动回归。"""
from __future__ import annotations

import threading
import time

DEGRADE_FAIL_THRESHOLD = 3
DEGRADE_COOLDOWN_SEC = 300  # 5 分钟


class Degrader:
    def __init__(self):
        self._lock = threading.Lock()
        self._fails: dict[str, int] = {}
        self._until: dict[str, float] = {}

    def record_error(self, model_id: str) -> None:
        with self._lock:
            self._fails[model_id] = self._fails.get(model_id, 0) + 1
            if self._fails[model_id] >= DEGRADE_FAIL_THRESHOLD:
                self._until[model_id] = time.time() + DEGRADE_COOLDOWN_SEC

    def record_success(self, model_id: str) -> None:
        with self._lock:
            self._fails[model_id] = 0
            self._until.pop(model_id, None)

    def is_degraded(self, model_id: str) -> bool:
        with self._lock:
            u = self._until.get(model_id)
            if u is None:
                return False
            if time.time() >= u:                 # 冷却到期,回归
                self._until.pop(model_id, None)
                self._fails[model_id] = 0
                return False
            return True


DEGRADER = Degrader()
