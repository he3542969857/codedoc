"""Java Spring 框架感知(供 java_parser 调用)。

- @RestController/@Controller/@Component/@Service/@Repository/@Configuration -> 组件(bean)
- @RequestMapping/@GetMapping/@PostMapping/... -> route_handler + path
- @Autowired/@Resource/@Inject -> bean_inject(由 java_parser 连边)
注解信息从 javalang 的 annotations 列表里取。
"""
from __future__ import annotations

_STEREOTYPE = {"RestController", "Controller", "Component", "Service",
               "Repository", "Configuration"}
_MAPPING = {"RequestMapping": "ANY", "GetMapping": "GET", "PostMapping": "POST",
            "PutMapping": "PUT", "DeleteMapping": "DELETE", "PatchMapping": "PATCH"}
_INJECT = {"Autowired", "Resource", "Inject"}


def anno_names(node) -> list[str]:
    out = []
    for a in getattr(node, "annotations", []) or []:
        nm = getattr(a, "name", None)
        if nm:
            out.append(nm)
    return out


def is_component(names: list[str]) -> bool:
    return any(n in _STEREOTYPE for n in names)


def is_inject(names: list[str]) -> bool:
    return any(n in _INJECT for n in names)


def route_of(node):
    """返回 {'method':..., 'path':...} 或 None。path 尽力从注解元素里取字符串。"""
    for a in getattr(node, "annotations", []) or []:
        nm = getattr(a, "name", None)
        if nm in _MAPPING:
            path = ""
            el = getattr(a, "element", None)
            try:
                if el is not None:
                    if isinstance(el, list):
                        for e in el:
                            v = getattr(getattr(e, "value", None), "value", None)
                            if isinstance(v, str):
                                path = v.strip('"'); break
                    else:
                        v = getattr(el, "value", None)
                        if isinstance(v, str):
                            path = v.strip('"')
            except Exception:
                path = ""
            return {"method": _MAPPING[nm], "path": path}
    return None
