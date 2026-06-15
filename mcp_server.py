"""codedoc MCP server — 把 ToolRegistry 的原子工具(search/context/callers/callees/
impact/explore/get_body)通过 stdio 暴露给 Claude Code / Cursor。

一份工具实现多处复用:Web QA / CLI / 本 MCP server 都走 codedoc.tools.registry.build_registry。
这里只做 MCP 传输层 + 按仓懒构建 registry,不改动运行中的 web 服务(server.py)。

挂载示例(Claude Code / Cursor 的 mcpServers 配置):
  {
    "mcpServers": {
      "codedoc": {
        "command": "/home/ubuntu/apps/codedoc/.venv/bin/python",
        "args": ["/home/ubuntu/apps/codedoc/mcp_server.py"],
        "env": {"CODEDOC_MCP_REPO": "flask"}
      }
    }
  }

自检:python mcp_server.py --selftest   (构建 flask registry 并实调 search,不起 stdio)
"""
from __future__ import annotations

import os
import sys
import json
import asyncio

# 让 `import codedoc` 可用(本文件在 /home/ubuntu/apps/codedoc/ 下)
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# 密钥 / DSN 复用底层模块的单一来源(pg_vectors / graph_persist / config),此处不再重复明文。
# 如需覆盖,挂载时在 MCP 配置的 env 里传 CODEDOC_PG_DSN / SILICONFLOW_API_KEY。

REPOS_DIR = os.environ.get("CODEDOC_REPOS_DIR", os.path.join(_HERE, "repos"))
DEFAULT_REPO = os.environ.get("CODEDOC_MCP_REPO", "flask")

_TYPE = {"str": "string", "int": "integer", "float": "number", "bool": "boolean"}
_REG_CACHE: dict[str, object] = {}


def _repo_dir(repo: str) -> str:
    """容忍 owner/repo 与裸 repo 两种写法,落到磁盘真实目录。"""
    cands = []
    if "/" in repo:
        cands.append(os.path.join(REPOS_DIR, repo.split("/", 1)[1]))
    cands.append(os.path.join(REPOS_DIR, repo))
    cands.append(os.path.join(REPOS_DIR, repo.replace("/", "_")))
    for d in cands:
        if os.path.isdir(d):
            return d
    raise ValueError("repo dir not found on disk: %s" % repo)


def _load_nodes_edges(repo: str, cfg):
    """先试落库快照(键名兜底 repo / pallets+repo),无快照再从磁盘 parse。"""
    from codedoc.graph import graph_persist
    for key in (repo, "pallets/" + repo) if "/" not in repo else (repo,):
        try:
            snap = graph_persist.load_graph(key)
        except Exception:
            snap = None
        if snap:
            nodes, edges = snap
            return nodes, edges
    # 回退:从磁盘解析(click/werkzeug 当前没有图谱快照)
    from codedoc.parser.runner import parse_repo
    return parse_repo(cfg)


def _make_vec_search(repo: str):
    """pg_vectors 的仓键当前多为 pallets/<repo>;两种键都试,返回首个非空。"""
    from codedoc.index import pg_vectors
    keys = (repo, "pallets/" + repo) if "/" not in repo else (repo,)

    def _vec(q, k):
        for key in keys:
            try:
                hits = pg_vectors.query(key, q, top_k=k)
            except Exception:
                hits = None
            if hits:
                return hits
        return []

    return _vec


def _registry(repo: str):
    repo = repo or DEFAULT_REPO
    if repo in _REG_CACHE:
        return _REG_CACHE[repo]
    from codedoc.config import load_config
    from codedoc.graph.memory_backend import MemoryGraphStore, MemoryGraphQuery
    from codedoc.tools.registry import build_registry

    repo_dir = _repo_dir(repo)
    cfg = load_config(repo_dir)
    nodes, edges = _load_nodes_edges(repo, cfg)
    store = MemoryGraphStore(cfg)
    store.upsert_nodes(nodes)
    store.upsert_edges(edges)
    gq = MemoryGraphQuery(cfg, store)
    reg = build_registry(cfg, gq, vec_search=_make_vec_search(repo), repo_root=repo_dir)
    _REG_CACHE[repo] = reg
    return reg


def _json_schema(input_schema: dict) -> dict:
    props = {k: {"type": _TYPE.get(v, "string")} for k, v in input_schema.items()}
    props["repo"] = {"type": "string",
                     "description": "目标仓(如 flask / click / werkzeug),默认 %s" % DEFAULT_REPO}
    return {"type": "object", "properties": props}


def _specs() -> list[dict]:
    """用默认仓构建一次拿到工具清单(7 个工具的 name/description/input_schema)。"""
    return _registry(DEFAULT_REPO).specs()


# ----------------------------- MCP stdio server -----------------------------

async def _serve():
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types

    server = Server("codedoc")
    specs = _specs()

    @server.list_tools()
    async def list_tools():
        return [types.Tool(name=s["name"], description=s["description"],
                           inputSchema=_json_schema(s["input_schema"])) for s in specs]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict | None):
        args = dict(arguments or {})
        repo = args.pop("repo", None) or DEFAULT_REPO
        # registry 构建/调用是同步、可能耗时,丢到线程池避免阻塞事件循环
        result = await asyncio.to_thread(lambda: _registry(repo).call(name, args))
        return [types.TextContent(type="text",
                                  text=json.dumps(result, ensure_ascii=False))]

    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


def _selftest():
    print("[selftest] building registry for repo=%s ..." % DEFAULT_REPO)
    reg = _registry(DEFAULT_REPO)
    print("[selftest] tools:", reg.tool_names)
    out = reg.call("search", {"query": "dispatch_request", "top_k": 3})
    print("[selftest] search ok=%s items=%d summary=%s"
          % (out.get("ok"), len(out.get("items", [])), out.get("summary")))
    for it in out.get("items", [])[:3]:
        print("   -", it.get("kind"), it.get("qualified_name") or it.get("name"))
    print("[selftest] DONE")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        asyncio.run(_serve())
