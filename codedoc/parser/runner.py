"""parse_repo(cfg) -> (nodes, edges)。

按文件后缀分派到 Python(ast)/ Java(javalang)/ JS(正则)解析器,汇总节点与边,
最后做一遍跨文件的调用名解析(把 calls 边的目标名解析到已知节点 id)。

node: {id, name, qualified_name, kind, signature, docstring, file, start_line, end_line, source}
edge: {src, dst, kind}   kind ∈ calls/imports/references/extends/route_handler/bean_inject/contains
"""
from __future__ import annotations

import fnmatch
import os
from pathlib import Path

from ..config import Config
from . import python_parser, java_parser, js_parser

_EXT_LANG = {".py": "python", ".java": "java", ".js": "javascript",
             ".jsx": "javascript", ".ts": "javascript", ".tsx": "javascript"}


def _excluded(path: Path, root: Path, patterns: list[str]) -> bool:
    rel = str(path.relative_to(root)).replace(os.sep, "/")
    full = str(path).replace(os.sep, "/")
    for p in patterns:
        if fnmatch.fnmatch(full, p) or fnmatch.fnmatch(rel, p):
            return True
    return False


def parse_repo(cfg: Config):
    root = Path(cfg.repo_path)
    langs = set(cfg.languages)
    nodes: list[dict] = []
    raw_calls: list[tuple[str, str, str]] = []  # (src_id, callee_name, kind)
    edges: list[dict] = []

    for dirpath, dirnames, filenames in os.walk(root):
        d = Path(dirpath)
        # 跳过被排除目录
        dirnames[:] = [dn for dn in dirnames
                       if not _excluded(d / dn, root, cfg.exclude)]
        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower()
            lang = _EXT_LANG.get(ext)
            if not lang or lang not in langs:
                continue
            fp = d / fn
            if _excluded(fp, root, cfg.exclude):
                continue
            rel = str(fp.relative_to(root)).replace(os.sep, "/")
            try:
                text = fp.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            try:
                if lang == "python":
                    fnodes, fedges, fcalls = python_parser.parse(rel, text)
                elif lang == "java":
                    fnodes, fedges, fcalls = java_parser.parse(rel, text)
                else:
                    fnodes, fedges, fcalls = js_parser.parse(rel, text)
            except Exception:
                continue
            nodes.extend(fnodes)
            edges.extend(fedges)
            raw_calls.extend(fcalls)

    # 跨文件调用名解析:callee 只有末段名(无 receiver),重名时用作用域消歧。
    # 全仓唯一 -> 直接连(confidence=unique);重名 -> 同文件唯一 / 同容器唯一才连
    # (confidence=scope);仍无法区分则丢弃,宁缺毋滥(避免类型缺失下的错边)。
    by_short: dict[str, list[str]] = {}
    qn2kind: dict[str, str] = {}
    for n in nodes:
        short = (n.get("qualified_name") or n.get("name") or "").split(".")[-1]
        if short:
            by_short.setdefault(short, []).append(n["id"])
        if n.get("qualified_name"):
            qn2kind[n["qualified_name"]] = n.get("kind", "")

    def _qn(node_id: str) -> str:
        return node_id.split("::", 1)[1] if "::" in node_id else node_id

    def _container(node_id: str) -> str:
        qn = _qn(node_id)
        return qn.rsplit(".", 1)[0] if "." in qn else qn

    def _in_scope(src_id: str, cand_id: str) -> bool:
        # 裸名调用按 Python LEGB 只能解析到:模块级 / 类成员 / 调用者所在闭包。
        # 嵌套在【别的】函数里的局部名够不着 -> 排除(否则把同名嵌套 helper 误连)。
        cont = _container(cand_id)
        if qn2kind.get(cont) in ("module", "class"):
            return True
        cq = _qn(src_id)
        return cq == cont or cq.startswith(cont + ".")

    def _resolve(src_id: str, short: str):
        cands = [c for c in by_short.get(short, ())
                 if c != src_id and _in_scope(src_id, c)]
        if not cands:
            return None, ""                       # 外部 / stdlib:不连
        if len(cands) == 1:
            return cands[0], "unique"             # 全仓唯一同名
        cf = src_id.split("::")[0]
        cc = _container(src_id)
        same_file = [c for c in cands if c.split("::")[0] == cf]
        if len(same_file) == 1:
            return same_file[0], "scope"          # 同文件唯一
        if len(same_file) > 1:                     # 同文件多个 -> 同容器(类/函数)唯一
            sc = [c for c in same_file if _container(c) == cc]
            return (sc[0], "scope") if len(sc) == 1 else (None, "")
        sc = [c for c in cands if _container(c) == cc]  # 跨文件 -> 同容器唯一
        return (sc[0], "scope") if len(sc) == 1 else (None, "")

    for src_id, callee, kind in raw_calls:
        dst, conf = _resolve(src_id, callee.split(".")[-1])
        if dst:
            edges.append({"src": src_id, "dst": dst, "kind": kind, "confidence": conf})

    return nodes, edges
