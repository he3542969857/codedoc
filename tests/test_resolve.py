# -*- coding: utf-8 -*-
"""调用边作用域消歧(LEGB)纯逻辑单测。
被测 API(待实现于 codedoc/parser/resolve.py):
    resolve_call(src_id, short, by_short, qn2kind) -> (dst_id|None, confidence)
      confidence ∈ {"unique","scope",""};"" 表示不连边。
node_id 形如 '文件::全限定名'。
"""
import pytest
from codedoc.parser.resolve import resolve_call


def _bs(*ids):
    """把若干 node_id 按"末段名"归到 by_short。"""
    d = {}
    for i in ids:
        short = i.split("::", 1)[1].rsplit(".", 1)[-1]
        d.setdefault(short, []).append(i)
    return d


def test_external_no_candidate():
    # 调用一个仓里不存在的名字 -> 不连(外部/stdlib)
    by_short = _bs("a.py::pkg.a.caller")
    assert resolve_call("a.py::pkg.a.caller", "len", by_short, {"pkg.a": "module"}) == (None, "")


def test_unique_module_level():
    # 全仓唯一同名(模块级)-> unique
    ids = ["a.py::pkg.a.caller", "a.py::pkg.a.helper"]
    qn2kind = {"pkg.a": "module"}
    assert resolve_call("a.py::pkg.a.caller", "helper", _bs(*ids), qn2kind) == ("a.py::pkg.a.helper", "unique")


def test_scope_same_file():
    # 重名,但只有一个在调用者所在文件 -> scope
    ids = ["a.py::pkg.a.caller", "a.py::pkg.a.run", "b.py::pkg.b.run"]
    qn2kind = {"pkg.a": "module", "pkg.b": "module"}
    assert resolve_call("a.py::pkg.a.caller", "run", _bs(*ids), qn2kind) == ("a.py::pkg.a.run", "scope")


def test_scope_same_container_within_file():
    # 同文件多个同名 -> 取同容器(类)那个 -> scope
    ids = ["a.py::pkg.C.m1", "a.py::pkg.C.helper", "a.py::pkg.D.helper"]
    qn2kind = {"pkg.C": "class", "pkg.D": "class"}
    assert resolve_call("a.py::pkg.C.m1", "helper", _bs(*ids), qn2kind) == ("a.py::pkg.C.helper", "scope")


def test_ambiguous_cross_file_dropped():
    # 重名分散在无关文件、既不同文件也不同容器 -> 宁缺毋滥丢弃
    ids = ["a.py::pkg.a.caller", "b.py::pkg.b.run", "c.py::pkg.c.run"]
    qn2kind = {"pkg.a": "module", "pkg.b": "module", "pkg.c": "module"}
    assert resolve_call("a.py::pkg.a.caller", "run", _bs(*ids), qn2kind) == (None, "")


def test_nested_in_unrelated_function_dropped():
    # 候选嵌套在【别的】函数里(按 LEGB 调用者够不着)-> 必须丢弃(就是修过的那个 bug)
    ids = ["t.py::tests.t.test_a", "t.py::tests.t.test_b.get"]
    qn2kind = {"tests.t": "module", "tests.t.test_a": "function",
               "tests.t.test_b": "function", "tests.t.test_b.get": "function"}
    assert resolve_call("t.py::tests.t.test_a", "get", _bs(*ids), qn2kind) == (None, "")


def test_closure_self_call_kept():
    # 候选是调用者自身闭包里的嵌套函数 -> 保留(LEGB 可达)
    ids = ["t.py::tests.t.test_dispatch", "t.py::tests.t.test_dispatch.dispatch"]
    qn2kind = {"tests.t": "module", "tests.t.test_dispatch": "function",
               "tests.t.test_dispatch.dispatch": "function"}
    dst, conf = resolve_call("t.py::tests.t.test_dispatch", "dispatch", _bs(*ids), qn2kind)
    assert dst == "t.py::tests.t.test_dispatch.dispatch" and conf in ("unique", "scope")


def test_self_excluded():
    # 不能把自己连给自己
    ids = ["a.py::pkg.a.f"]
    assert resolve_call("a.py::pkg.a.f", "f", _bs(*ids), {"pkg.a": "module"}) == (None, "")
