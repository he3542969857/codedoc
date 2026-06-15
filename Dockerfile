# codedoc 镜像(放到 /home/ubuntu/apps/codedoc/Dockerfile,构建上下文=该目录)
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r /app/requirements.lock
COPY server.py /app/server.py
COPY codedoc /app/codedoc
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 CODEDOC_WATCHDOG=0
EXPOSE 8501
# 容器内每副本起 2 个 worker;副本数由编排层(compose --scale / k8s replicas)弹
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8501", "--workers", "2"]
