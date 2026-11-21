FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 系统层面无需额外编译依赖：pyarrow / fastapi 均有 manylinux wheel
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# 0.0.0.0 让容器外可访问；单 worker 即可，核查在线程池中执行不阻塞事件循环
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
