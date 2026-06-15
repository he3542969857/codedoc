"""Java 解析器(基于 javalang)。基础版:类 / 方法 / 字段,calls + extends + Spring 注解。

产出 nodes(class/method/route_handler)、edges(contains/bean_inject)、calls((src,callee,kind))。
javalang 不可用或解析失败时返回空(不影响其它语言)。
"""
from __future__ import annotations

from .framework import spring

try:
    import javalang
except Exception:
    javalang = None


def _pkg(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("package ") and s.endswith(";"):
            return s[len("package "):-1].strip()
    return ""


def parse(rel_path: str, text: str):
    nodes: list[dict] = []
    edges: list[dict] = []
    calls: list[tuple[str, str, str]] = []
    if javalang is None:
        return nodes, edges, calls
    try:
        tree = javalang.parse.parse(text)
    except Exception:
        return nodes, edges, calls
    pkg = _pkg(text)
    lines = text.splitlines()

    for _, cls in tree.filter(javalang.tree.TypeDeclaration):
        cname = cls.name
        cqn = f"{pkg}.{cname}" if pkg else cname
        cid = rel_path + "::" + cqn
        cnames = spring.anno_names(cls)
        kind = "class"
        ln = getattr(getattr(cls, "position", None), "line", 1) or 1
        nodes.append({
            "id": cid, "name": cname, "qualified_name": cqn,
            "kind": "component" if spring.is_component(cnames) else kind,
            "signature": f"class {cname}", "docstring": getattr(cls, "documentation", "") or "",
            "file": rel_path, "start_line": ln, "end_line": ln, "source": "",
        })
        # extends
        ext = getattr(cls, "extends", None)
        if ext is not None:
            en = getattr(ext, "name", None)
            if en:
                calls.append((cid, en, "extends"))
        # 字段注入(@Autowired 等)-> bean_inject 到字段类型
        for fld in getattr(cls, "fields", []) or []:
            if spring.is_inject(spring.anno_names(fld)):
                tname = getattr(getattr(fld, "type", None), "name", None)
                if tname:
                    calls.append((cid, tname, "bean_inject"))
        # 方法
        for m in getattr(cls, "methods", []) or []:
            mqn = f"{cqn}.{m.name}"
            mid = rel_path + "::" + mqn
            mnames = spring.anno_names(m)
            route = spring.route_of(m)
            mkind = "route_handler" if route else "method"
            params = ", ".join(getattr(getattr(p, "type", None), "name", "?") for p in (m.parameters or []))
            mln = getattr(getattr(m, "position", None), "line", ln) or ln
            nd = {
                "id": mid, "name": m.name, "qualified_name": mqn, "kind": mkind,
                "signature": f"{m.name}({params})", "docstring": getattr(m, "documentation", "") or "",
                "file": rel_path, "start_line": mln, "end_line": mln, "source": "",
            }
            if route:
                nd["route"] = route
            nodes.append(nd)
            edges.append({"src": cid, "dst": mid, "kind": "contains"})
            # 方法体内的调用
            try:
                for _, inv in m.filter(javalang.tree.MethodInvocation):
                    if inv.member:
                        calls.append((mid, inv.member, "calls"))
            except Exception:
                pass
    return nodes, edges, calls
