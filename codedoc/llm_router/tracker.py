"""每模型实时统计:in_flight / completed / errors / 平均时延。线程安全。"""
from __future__ import annotations

import threading
from collections import defaultdict


class Tracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._s = defaultdict(lambda: {"in_flight": 0, "completed": 0, "errors": 0, "lat_sum": 0.0})

    def start(self, model_id: str) -> None:
        with self._lock:
            self._s[model_id]["in_flight"] += 1

    def finish(self, model_id: str, latency: float, ok: bool) -> None:
        with self._lock:
            st = self._s[model_id]
            st["in_flight"] = max(0, st["in_flight"] - 1)
            if ok:
                st["completed"] += 1
                st["lat_sum"] += latency
            else:
                st["errors"] += 1

    def in_flight(self, model_id: str) -> int:
        with self._lock:
            return self._s[model_id]["in_flight"]

    def completed(self, model_id: str) -> int:
        with self._lock:
            return self._s[model_id]["completed"]

    def stats(self) -> dict:
        with self._lock:
            out = {}
            for mid, st in self._s.items():
                c = st["completed"]
                out[mid] = {"in_flight": st["in_flight"], "completed": c, "errors": st["errors"],
                            "avg_latency_ms": round(1000 * st["lat_sum"] / c) if c else 0}
            return out


TRACKER = Tracker()
