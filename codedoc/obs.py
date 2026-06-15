"""轻量可观测 + 限流(零第三方依赖)。

- 限流:Postgres 固定窗口计数,按 user(JWT sub)或 IP 维度,【跨 worker 精确】
  (与三条任务队列同样的"共享 PG 单一真相"思路,不用进程内令牌桶——那在多 worker 下额度会 ×N)。
  限流子系统自身异常时 fail-open(放行),绝不因限流故障拖垮主流程。
- Metrics:进程内计数/时延/在途,/metrics 以 Prometheus 文本暴露(多 worker 下在 Prometheus 端 sum)。
- log_request:每请求一行结构化(JSON)日志,输出到 stdout→/var/log/codedoc.log。
"""
from __future__ import annotations

import os
import json
import logging
import threading

import psycopg
try:
    from codedoc.graph.graph_persist import PG_DSN
except Exception:
    PG_DSN = os.environ["CODEDOC_PG_DSN"]

# 结构化访问日志:确保有 handler 输出到 stdout(被 systemd 收到 /var/log/codedoc.log)
_log = logging.getLogger("codedoc.access")
if not _log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(name)s %(message)s"))
    _log.addHandler(_h)
    _log.setLevel(logging.INFO)
    _log.propagate = False


# ----------------------------- 限流:PG 固定窗口(跨 worker)-----------------------------

RL_LIMIT = int(os.environ.get("CODEDOC_RL_LIMIT", "20"))      # 每窗口每 key 允许次数
RL_WINDOW = int(os.environ.get("CODEDOC_RL_WINDOW", "10"))    # 窗口秒数


def ensure_rl_schema() -> None:
    try:
        with psycopg.connect(PG_DSN, autocommit=True) as conn:
            conn.cursor().execute(
                "CREATE TABLE IF NOT EXISTS rl_counters (k TEXT, win BIGINT, n INT, PRIMARY KEY(k, win))")
    except Exception:
        pass


def pg_allow(key: str, limit: int, window: int, now: float) -> tuple[bool, float]:
    """固定窗口计数:本窗口内该 key 第 n 次访问,n>limit 则拒。返回 (放行?, Retry-After 秒)。
    任何异常 fail-open(放行)。"""
    try:
        win = int(now // window)
        with psycopg.connect(PG_DSN, autocommit=True) as conn:
            cur = conn.cursor()
            n = cur.execute(
                "INSERT INTO rl_counters (k, win, n) VALUES (%s,%s,1) "
                "ON CONFLICT (k, win) DO UPDATE SET n = rl_counters.n + 1 RETURNING n",
                (key, win)).fetchone()[0]
            if n == 1:   # 新窗口首次命中时顺手清理旧窗口,表保持很小
                cur.execute("DELETE FROM rl_counters WHERE win < %s", (win - 1,))
            if n > limit:
                return False, float(window - (now % window))
            return True, 0.0
    except Exception:
        return True, 0.0


ensure_rl_schema()

_LIMITED_PREFIXES = ("/api/v1/ask", "/api/v1/docgen", "/api/v1/repos")


def is_limited(method: str, path: str) -> bool:
    """只限流"贵"的写/计算型端点:问答、文档生成、加仓/上传。"""
    if method != "POST":
        return False
    return any(p in path for p in _LIMITED_PREFIXES)


# ----------------------------- 指标 -----------------------------

class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.req_total: dict[tuple, int] = {}     # (method, path, status) -> n
        self.dur_sum: dict[str, float] = {}        # path -> 累计秒
        self.dur_count: dict[str, int] = {}        # path -> n
        self.rl_rejects = 0                          # 429 次数
        self.in_flight = 0

    def inc_inflight(self, d: int):
        with self._lock:
            self.in_flight += d

    def observe(self, method: str, path: str, status: int, dur: float):
        with self._lock:
            k = (method, path, str(status))
            self.req_total[k] = self.req_total.get(k, 0) + 1
            self.dur_sum[path] = self.dur_sum.get(path, 0.0) + dur
            self.dur_count[path] = self.dur_count.get(path, 0) + 1

    def inc_reject(self):
        with self._lock:
            self.rl_rejects += 1

    def render(self) -> str:
        with self._lock:
            out = []
            out.append("# HELP http_requests_total Total HTTP requests")
            out.append("# TYPE http_requests_total counter")
            for (m, p, s), n in sorted(self.req_total.items()):
                out.append('http_requests_total{method="%s",path="%s",status="%s"} %d' % (m, _esc(p), s, n))
            out.append("# HELP http_request_duration_seconds Request duration summary")
            out.append("# TYPE http_request_duration_seconds summary")
            for p, tot in sorted(self.dur_sum.items()):
                out.append('http_request_duration_seconds_sum{path="%s"} %.6f' % (_esc(p), tot))
                out.append('http_request_duration_seconds_count{path="%s"} %d' % (_esc(p), self.dur_count.get(p, 0)))
            out.append("# HELP http_in_flight In-flight requests")
            out.append("# TYPE http_in_flight gauge")
            out.append("http_in_flight %d" % self.in_flight)
            out.append("# HELP http_ratelimit_rejected_total Requests rejected by rate limit (429)")
            out.append("# TYPE http_ratelimit_rejected_total counter")
            out.append("http_ratelimit_rejected_total %d" % self.rl_rejects)
            return "\n".join(out) + "\n"


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


METRICS = Metrics()


def log_request(rid: str, method: str, path: str, status: int, dur_ms: float, who: str):
    """每请求一行 JSON 结构化日志。"""
    try:
        _log.info(json.dumps({
            "rid": rid, "method": method, "path": path, "status": status,
            "dur_ms": round(dur_ms, 1), "who": who,
        }, ensure_ascii=False))
    except Exception:
        pass
