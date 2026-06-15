# -*- coding: utf-8 -*-
"""JS / TS / JSX 解析(tree-sitter 官方 binding)——抽 函数 / 箭头函数 / 类 / 方法 节点,
contains / calls / imports 边。

替代旧正则版:用真 AST(tree-sitter,容错解析)、统一覆盖 js/ts/tsx,
并**新增 call/imports 边**(旧正则版只有 contains、没有调用关系)。契约与 python/java 解析器一致:
parse(rel_path, text) -> (nodes, edges, calls);node 字段同 runner 约定。
tree-sitter 不可用时优雅退化(只产出 module 节点),不阻断整库解析。
"""
from __future__ import annotations

_PARSERS = {}


def _get_parser(grammar: str):
    if grammar not in _PARSERS:
        from tree_sitter import Language, Parser
        if grammar == "typescript":
            import tree_sitter_typescript as ts
            lang = Language(ts.language_typescript())
        elif grammar == "tsx":
            import tree_sitter_typescript as ts
            lang = Language(ts.language_tsx())
        else:
            import tree_sitter_javascript as tsj
            lang = Language(tsj.language())
        _PARSERS[grammar] = Parser(lang)
    return _PARSERS[grammar]


def _grammar_for(rel_path: str) -> str:
    if rel_path.endswith(".tsx"):
        return "tsx"
    if rel_path.endswith(".ts"):
        return "typescript"
    return "javascript"


def parse(rel_path: str, text: str):
    nodes: list[dict] = []
    edges: list[dict] = []
    calls: list[tuple[str, str, str]] = []
    mod_qn = rel_path.rsplit(".", 1)[0].replace("/", ".")
    mod_id = rel_path + "::" + mod_qn
    nodes.append({"id": mod_id, "name": mod_qn.split(".")[-1], "qualified_name": mod_qn,
                  "kind": "module", "signature": "", "docstring": "", "file": rel_path,
                  "start_line": 1, "end_line": text.count("\n") + 1, "source": ""})

    try:
        parser = _get_parser(_grammar_for(rel_path))
    except Exception:
        return nodes, edges, calls   # tree-sitter 不可用 -> 退化只给 module

    src = bytes(text, "utf-8")
    tree = parser.parse(src)

    def txt(node) -> str:
        return src[node.start_byte:node.end_byte].decode("utf-8", "replace")

    def field(node, name):
        return node.child_by_field_name(name)

    def params_text(node) -> str:
        p = field(node, "parameters")
        return txt(p) if p else "()"

    def emit(node, name, kind, parent_qn, parent_id):
        qn = (parent_qn + "." + name) if parent_qn else name
        nid = rel_path + "::" + qn
        sig = ("class " + name) if kind == "class" else (name + params_text(node))
        nodes.append({"id": nid, "name": name, "qualified_name": qn, "kind": kind,
                      "signature": sig, "docstring": "", "file": rel_path,
                      "start_line": node.start_point[0] + 1, "end_line": node.end_point[0] + 1,
                      "source": ""})
        edges.append({"src": parent_id, "dst": nid, "kind": "contains"})
        return nid, qn

    def walk(node, cur_id, cur_qn):
        for ch in node.children:
            t = ch.type
            if t == "function_declaration":
                nm = field(ch, "name")
                if nm:
                    nid, qn = emit(ch, txt(nm), "function", cur_qn, cur_id)
                    walk(ch, nid, qn)
                else:
                    walk(ch, cur_id, cur_qn)
            elif t == "class_declaration":
                nm = field(ch, "name")
                if nm:
                    nid, qn = emit(ch, txt(nm), "class", cur_qn, cur_id)
                    walk(ch, nid, qn)
                else:
                    walk(ch, cur_id, cur_qn)
            elif t == "method_definition":
                nm = field(ch, "name")
                if nm:
                    nid, qn = emit(ch, txt(nm), "method", cur_qn, cur_id)
                    walk(ch, nid, qn)
                else:
                    walk(ch, cur_id, cur_qn)
            elif t in ("lexical_declaration", "variable_declaration"):
                for d in ch.children:
                    if d.type == "variable_declarator":
                        val = field(d, "value")
                        nmn = field(d, "name")
                        if nmn and val is not None and val.type in (
                                "arrow_function", "function", "function_expression"):
                            p = field(val, "parameters")
                            ptext = txt(p) if p else "()"
                            nm = txt(nmn)
                            qn = (cur_qn + "." + nm) if cur_qn else nm
                            nid = rel_path + "::" + qn
                            nodes.append({"id": nid, "name": nm, "qualified_name": qn,
                                          "kind": "function", "signature": nm + ptext,
                                          "docstring": "", "file": rel_path,
                                          "start_line": d.start_point[0] + 1,
                                          "end_line": d.end_point[0] + 1, "source": ""})
                            edges.append({"src": cur_id, "dst": nid, "kind": "contains"})
                            walk(val, nid, qn)
                        else:
                            walk(d, cur_id, cur_qn)
                    else:
                        walk(d, cur_id, cur_qn)
            elif t == "call_expression":
                fn = field(ch, "function")
                callee = None
                if fn is not None:
                    if fn.type == "identifier":
                        callee = txt(fn)
                    elif fn.type == "member_expression":
                        pr = field(fn, "property")
                        callee = txt(pr) if pr else None
                if callee:
                    calls.append((cur_id, callee, "calls"))
                walk(ch, cur_id, cur_qn)
            elif t == "import_statement":
                s = field(ch, "source")
                if s is not None:
                    mod = txt(s).strip("'\"`")
                    calls.append((cur_id, mod.split("/")[-1], "imports"))
                walk(ch, cur_id, cur_qn)
            else:
                walk(ch, cur_id, cur_qn)

    walk(tree.root_node, mod_id, mod_qn)
    return nodes, edges, calls
