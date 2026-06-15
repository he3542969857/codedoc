# -*- coding: utf-8 -*-
"""调用边作用域消歧(纯逻辑,可单测)。

callee 经 AST 只剩末段名(无 receiver),重名时按 Python LEGB 作用域决定连哪条:
- 全仓唯一同名            -> 连,confidence="unique"
- 重名:同文件唯一 / 同容器唯一 / 调用者所在闭包  -> 连,confidence="scope"
- 跨无关文件同名(无类型信息分不了)             -> 丢弃,宁缺毋滥

node_id 形如 '文件::全限定名';qn2kind: 全限定名 -> 节点 kind。
"""
from __future__ import annotations


def qn(node_id: str) -> str:
    """取 node_id 的全限定名部分。"""
    return node_id.split("::", 1)[1] if "::" in node_id else node_id


def container(node_id: str) -> str:
    """取所属容器的全限定名(去掉最后一段)。"""
    q = qn(node_id)
    return q.rsplit(".", 1)[0] if "." in q else q


def in_scope(src_id: str, cand_id: str, qn2kind: dict) -> bool:
    """候选能否被调用者以裸名解析到(Python LEGB):
    模块级 / 类成员 -> 任何同模块处可达;否则容器是函数时,仅调用者在该闭包内可达。
    """
    cont = container(cand_id)
    if qn2kind.get(cont) in ("module", "class"):
        return True
    cq = qn(src_id)
    return cq == cont or cq.startswith(cont + ".")


def resolve_call(src_id: str, short: str, by_short: dict, qn2kind: dict):
    """解析一条调用边的目标。返回 (dst_id|None, confidence)。"""
    cands = [c for c in by_short.get(short, ())
             if c != src_id and in_scope(src_id, c, qn2kind)]
    if not cands:
        return None, ""                       # 外部 / stdlib / 够不着:不连
    if len(cands) == 1:
        return cands[0], "unique"             # 全仓(在可见域内)唯一
    cf = src_id.split("::")[0]
    cc = container(src_id)
    same_file = [c for c in cands if c.split("::")[0] == cf]
    if len(same_file) == 1:
        return same_file[0], "scope"          # 同文件唯一
    if len(same_file) > 1:                     # 同文件多个 -> 取同容器唯一
        sc = [c for c in same_file if container(c) == cc]
        return (sc[0], "scope") if len(sc) == 1 else (None, "")
    sc = [c for c in cands if container(c) == cc]   # 跨文件 -> 同容器唯一
    return (sc[0], "scope") if len(sc) == 1 else (None, "")
