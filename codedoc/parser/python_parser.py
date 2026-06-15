"""Python 解析器(基于标准库 ast,确定性)。

产出:
- nodes: module / class / function / method / route_handler
- edges: contains(结构)、imports
- calls: (src_id, callee_name, "calls") 交给 runner 跨文件解析
框架感知:Flask/FastAPI 路由装饰器(@app.route / @app.get / @router.post / @bp.route)
         标成 route_handler 并抽出 path。
"""
from __future__ import annotations

import ast
from .framework import pyweb


def _module_qn(rel_path: str) -> str:
    qn = rel_path[:-3] if rel_path.endswith(".py") else rel_path
    qn = qn.replace("/", ".")
    if qn.endswith(".__init__"):
        qn = qn[: -len(".__init__")]
    return qn


def _sig(fn: ast.AST) -> str:
    try:
        args = []
        a = fn.args
        for arg in a.args:
            args.append(arg.arg)
        if a.vararg:
            args.append("*" + a.vararg.arg)
        if a.kwarg:
            args.append("**" + a.kwarg.arg)
        return f"{fn.name}({', '.join(args)})"
    except Exception:
        return getattr(fn, "name", "")


def _call_name(node: ast.Call) -> str | None:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return None


def parse(rel_path: str, text: str):
    nodes: list[dict] = []
    edges: list[dict] = []
    calls: list[tuple[str, str, str]] = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return nodes, edges, calls
    lines = text.splitlines()
    mod_qn = _module_qn(rel_path)
    mod_id = rel_path + "::" + mod_qn
    nodes.append({
        "id": mod_id, "name": mod_qn.split(".")[-1], "qualified_name": mod_qn,
        "kind": "module", "signature": "", "docstring": ast.get_docstring(tree) or "",
        "file": rel_path, "start_line": 1, "end_line": len(lines), "source": "",
    })

    def src_of(node) -> str:
        a = getattr(node, "lineno", None)
        b = getattr(node, "end_lineno", None)
        if a and b and b >= a:
            return "\n".join(lines[a - 1:b])[:4000]
        return ""

    def walk(scope_node, scope_qn, scope_id, in_class):
        for child in scope_node.body:
            if isinstance(child, ast.ClassDef):
                qn = f"{scope_qn}.{child.name}"
                cid = rel_path + "::" + qn
                bases = [b.id for b in child.bases if isinstance(b, ast.Name)]
                nodes.append({
                    "id": cid, "name": child.name, "qualified_name": qn, "kind": "class",
                    "signature": f"class {child.name}" + (f"({', '.join(bases)})" if bases else ""),
                    "docstring": ast.get_docstring(child) or "", "file": rel_path,
                    "start_line": child.lineno, "end_line": getattr(child, "end_lineno", child.lineno),
                    "source": src_of(child),
                })
                edges.append({"src": scope_id, "dst": cid, "kind": "contains"})
                for b in bases:
                    calls.append((cid, b, "extends"))
                walk(child, qn, cid, True)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qn = f"{scope_qn}.{child.name}"
                fid = rel_path + "::" + qn
                kind = "method" if in_class else "function"
                route = pyweb.detect_route(child)
                if route:
                    kind = "route_handler"
                nd = {
                    "id": fid, "name": child.name, "qualified_name": qn, "kind": kind,
                    "signature": _sig(child), "docstring": ast.get_docstring(child) or "",
                    "file": rel_path, "start_line": child.lineno,
                    "end_line": getattr(child, "end_lineno", child.lineno), "source": src_of(child),
                }
                if route:
                    nd["route"] = route
                nodes.append(nd)
                edges.append({"src": scope_id, "dst": fid, "kind": "contains"})
                # 函数体内的调用
                for sub in ast.walk(child):
                    if isinstance(sub, ast.Call):
                        cn = _call_name(sub)
                        if cn:
                            calls.append((fid, cn, "calls"))
                # 嵌套函数/类
                walk(child, qn, fid, False)

    # imports
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                edges.append({"src": mod_id, "dst": "ext::" + a.name, "kind": "imports"})
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            for a in node.names:
                edges.append({"src": mod_id, "dst": "ext::" + (base + "." + a.name if base else a.name),
                              "kind": "imports"})

    walk(tree, mod_qn, mod_id, False)
    return nodes, edges, calls
