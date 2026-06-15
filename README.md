# CodeDoc — 多仓代码知识图谱问答与文档平台

> 把多仓代码用 AST 建成「谁调用谁」的知识图谱，按意图分流混合检索，多仓问答用真多 Agent（自主 ReAct + LLM 跨仓推理 + 反思环），逐符号抗幻觉核验，并能自动生成设计文档。

面向新人上手、QA、PM、架构师在大规模多仓代码下的理解与协作。

## ✨ 特性

- **多语言代码知识图谱**：Python(`ast`) / Java(`javalang`) / JS-TS(`tree-sitter`) 统一成一套节点 + 边模型；除类/方法/字段外识别 Spring 路由与 IoC bean、Flask·FastAPI 路由等**框架节点**，可跨 IoC 注入与 HTTP 路由追踪调用链。
- **意图分流混合检索**：语义(向量 BGE-M3 + 全文倒排)、关系(图遍历 callers/callees，等价 LSP find-references)、全局(GraphRAG 社区摘要)三条链路；候选过 BGE-reranker 交叉编码精排。
- **调用边作用域消歧**：跨文件调用按 Python LEGB(同文件/同类/闭包)消歧解析，标注 `confidence`(unique/scope)，重名场景较「仅唯一名才连」召回大幅提升。
- **真多 Agent 多仓问答**：LangGraph 编排 Planner → 并行 RepoAgent(深问跑真 ReAct，LLM 自主选工具) → Merger(LLM 跨仓推理) → Synthesiser → Reflect(自评不足则定向补检索重答)；PostgresSaver 检查点可断点续传。
- **三层抗幻觉核验**：符号级(存在率 groundedness) / 关系级(查图谱边) / LLM 裁判；答案不足(hedge 或 groundedness<0.5)触发反思补检索。
- **文档自动生成**：单仓 17 章模板(事实从图谱抽、PlantUML 从签名生成、仅少量 LLM 写散文) + 多仓系统级文档(还原跨仓真实依赖方向)。
- **全栈 PostgreSQL**：pgvector(HNSW) + 代码图谱(JSONB) + 任务队列(`SKIP LOCKED`) + 会话记忆(CAS) + Agent 检查点。

## 🏗 架构

```
建库(离线)  代码仓 → AST 解析 → 统一代码知识图谱(9 类节点/8 类边, 作用域消歧)
            → 三套索引(图 / 向量 / 全文) → PostgreSQL

问答(在线)  问题 → 意图分流 + 5 路混合召回 + reranker + 图谱锚定
            → 单仓:确定性工作流 + 反思  |  多仓:Planner→RepoAgent(ReAct)→Merger→Synthesiser→Reflect
            → 三层抗幻觉核验 → 答案
```

## 🧰 技术栈

LangGraph · FastAPI · PostgreSQL + pgvector · BGE-M3 / BGE-reranker · tree-sitter / javalang · NetworkX(Louvain/PageRank) · MCP

## 🚀 快速开始

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt          # 若无,见 import 安装 fastapi uvicorn psycopg pgvector 等

cp .env.example .env                      # 填入你的 SiliconFlow Key、PG 连接、内部 key
export $(grep -v '^#' .env | xargs)

uvicorn server:app --host 0.0.0.0 --port 8501 --workers 4
```

访问 `http://localhost:8501`(Web UI) / `http://localhost:8501/docs`(API)。

## 📡 主要接口

- `POST /api/v1/ask` — 代码问答(单仓 `repo` / 多仓 `repos`)
- `POST /api/v1/repos` — 提交仓库建索引
- `POST /api/v1/docgen` — 生成文档
- `POST /tools/{search,get_body,impact}` — 原子工具(内部 key)
- MCP：`search/context/callers/callees/impact/explore/get_body` 7 原子工具 + 4 技能

## ⚙️ 配置

见 `.env.example`。**所有密钥都从环境变量读取，仓库内不含任何真实密钥。**

## 📄 License

MIT
