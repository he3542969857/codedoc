# -*- coding: utf-8 -*-
"""图查询层缓存:按 repo 缓存 MemoryGraphQuery,避免每请求重建边索引。

关键:缓存条目记下当时的 store 对象;重索引 / watchdog 会把 store 换成【新对象】,
身份(is)一变即缓存未命中 -> 自动重建,因此不会返回过期结果,无需额外失效钩子。
按 worker 进程各自持有(每 repo 一份),内存有界。
"""
from __future__ import annotations


class QueryCache:
    def __init__(self, builder):
        self._builder = builder          # builder(cfg, store) -> query 对象
        self._cache: dict = {}           # repo -> (store, query)

    def get(self, repo, cfg, store):
        ent = self._cache.get(repo)
        if ent is not None and ent[0] is store:   # 同一个 store 对象 -> 索引已建,复用
            return ent[1]
        query = self._builder(cfg, store)          # store 变了 / 首次 -> 构建并缓存
        self._cache[repo] = (store, query)
        return query

    def invalidate(self, repo=None):
        if repo is None:
            self._cache.clear()
        else:
            self._cache.pop(repo, None)
