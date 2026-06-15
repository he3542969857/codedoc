"""codedoc — 代码资产文档化与跨角色知识问答平台(重建版)。

本包是从 http_api/server.py 暴露的接口规格 + 面试手册的算法描述重建的核心层:
- config        : load_config(repo_dir) -> Config
- parser.runner : parse_repo(cfg) -> (nodes, edges)
- graph         : MemoryGraphStore / MemoryGraphQuery(内存图 + 全文召回 + 调用遍历)
- agents.llm    : build_llm(cfg) / ChatMessage(OpenAI 兼容,接 SiliconFlow)
"""
