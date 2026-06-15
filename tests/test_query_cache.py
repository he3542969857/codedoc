# -*- coding: utf-8 -*-
"""图查询层缓存(按 store 对象身份缓存,store 换了自动失效)单测。
被测 API(待实现于 codedoc/graph/query_cache.py):
    QueryCache(builder).get(repo, cfg, store) / .invalidate(repo=None)
"""
import pytest
from codedoc.graph.query_cache import QueryCache


class _Counter:
    """builder:每次构造计数,返回新对象。"""
    def __init__(self):
        self.calls = 0
        self.seen = None
    def __call__(self, cfg, store):
        self.calls += 1
        self.seen = (cfg, store)
        return object()


def test_same_store_built_once():
    b = _Counter(); qc = QueryCache(b); s = object()
    q1 = qc.get("flask", None, s)
    q2 = qc.get("flask", None, s)
    assert q1 is q2 and b.calls == 1          # 同一 store -> 只建一次,复用


def test_rebuilds_when_store_replaced():
    b = _Counter(); qc = QueryCache(b)
    q1 = qc.get("flask", None, object())
    q2 = qc.get("flask", None, object())      # store 被换(重索引/watchdog)
    assert q1 is not q2 and b.calls == 2       # 身份变 -> 自动重建(不返回过期)


def test_per_repo_independent():
    b = _Counter(); qc = QueryCache(b)
    s1, s2 = object(), object()
    qc.get("a", None, s1); qc.get("b", None, s2); qc.get("a", None, s1)
    assert b.calls == 2                         # a/b 各建一次,a 第二次命中


def test_invalidate_one_repo():
    b = _Counter(); qc = QueryCache(b); s = object()
    qc.get("a", None, s); qc.invalidate("a"); qc.get("a", None, s)
    assert b.calls == 2


def test_invalidate_all():
    b = _Counter(); qc = QueryCache(b); s1, s2 = object(), object()
    qc.get("a", None, s1); qc.get("b", None, s2)
    qc.invalidate()
    qc.get("a", None, s1); qc.get("b", None, s2)
    assert b.calls == 4


def test_builder_receives_cfg_store():
    b = _Counter(); qc = QueryCache(b)
    cfg, s = object(), object()
    qc.get("a", cfg, s)
    assert b.seen == (cfg, s)
