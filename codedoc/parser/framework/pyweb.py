"""Python Web 框架感知:Flask / FastAPI 路由装饰器。

detect_route(funcdef) -> {"method": ..., "path": ...} 或 None
识别:@app.route("/x")、@app.get/post/...(FastAPI)、@router.post(...)、@bp.route(...)、@blueprint.route(...)
"""
from __future__ import annotations

import ast

_HTTP = {"route", "get", "post", "put", "delete", "patch", "head", "options"}


def detect_route(fn: ast.AST) -> dict | None:
    for dec in getattr(fn, "decorator_list", []) or []:
        call = dec if isinstance(dec, ast.Call) else None
        attr = None
        if call and isinstance(call.func, ast.Attribute):
            attr = call.func.attr
        elif isinstance(dec, ast.Attribute):
            attr = dec.attr
        if attr in _HTTP:
            path = ""
            method = "GET" if attr == "get" else (attr.upper() if attr != "route" else "ANY")
            if call:
                # 第一个字符串实参当 path
                for a in call.args:
                    if isinstance(a, ast.Constant) and isinstance(a.value, str):
                        path = a.value
                        break
                # methods=[...] 关键字
                for kw in call.keywords or []:
                    if kw.arg == "methods" and isinstance(kw.value, (ast.List, ast.Tuple)):
                        ms = [e.value for e in kw.value.elts
                              if isinstance(e, ast.Constant) and isinstance(e.value, str)]
                        if ms:
                            method = "/".join(ms)
            return {"method": method, "path": path}
    return None
