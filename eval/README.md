# 评测脚本

- `multi_repo_eval.py` 多仓 deep 关系召回评测(GT=图谱真实 callers,可作 CI 门禁)
- `mem_ablation.py` 多轮记忆消融评测(无记忆 / 仅 anchor / 全套)

运行:`python eval/<script>.py`(需先配置 .env 与 PostgreSQL)。
