# Parquet 排序契约核查台

基于 **FastAPI + PyArrow** 的 Web 工具：数据工程师上传含多个 row group 的
Parquet 文件，提交 1～3 个排序列及每列 `ASC/DESC`、`NULLS FIRST/LAST` 规则，
系统按**原文件行序**流式核对**全局字典序**，并跨 row group 比较边界，返回首个
违反顺序的两行、所在 row group、原文件行号与键值，同时给出每组键范围摘要。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

健康检查：`GET http://localhost:8000/healthz`。

## 本地直接运行（无需 Docker）

```bash
python3 -m venv --without-pip .venv        # 若 venv 自带 pip 可省略后续 bootstrap
python3 get-pip.py 路径或 .venv/bin/python -m ensurepip   # 视环境而定
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 使用流程（浏览器）

1. 选择 `.parquet` 文件，点击 **读取结构**：页面展示总行数、row group 数、列 schema，
   并标注每列是否支持比较。
2. 在 **排序规则** 区添加 1～3 个排序列（按优先级构成字典序），每列选择
   `ASC/DESC` 与 `NULLS FIRST/LAST`。
3. 点击 **开始核查**，页面展示：
   - 结论徽标：`PASS` / `VIOLATION` / `ERROR`；
   - 违反时给出**相邻两行**（上一行、当前行）的 row group（1-based 展示 + 0-based index）、
     原文件行号（1-based，与物理行序一致）、复合键值与中文违反原因；
   - 每个 row group 的行数、首尾行号、最小/最大复合键、各单列 min/max 与 NULL 计数。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 浏览器页面 |
| `GET` | `/healthz` | 健康检查 |
| `POST` | `/api/inspect` | multipart 上传 `file`，只读元数据，返回 row group 与列 schema |
| `POST` | `/api/check` | multipart 上传 `file` 与 `rules`（JSON 字符串），执行核查 |

`rules` 载荷示例：

```json
{
  "rules": [
    {"column": "user_id", "direction": "ASC", "nulls": "NULLS FIRST"},
    {"column": "event_at", "direction": "DESC", "nulls": "NULLS LAST"}
  ]
}
```

`/api/check` 的响应 `status` 取值：

- `pass`：满足契约（相同键允许，原行序天然保留）；
- `violation`：发现首个违反，`violation` 字段给出相邻两行详情；
- `error`：读取失败 / 列类型不支持 / 无法按规则比较等，**不提供通过结论**，
  此时 HTTP 状态码仍为 200，错误以 `error_code` + `error_message` 表达
  （便于携带已扫描部分的摘要）；`/api/inspect` 的同类错误返回 HTTP 422。

## 核查语义

- **流式处理**：`ParquetFile.iter_batches` 按 row group、按批（默认 5 万行/批）
  仅读取排序列，不把整表载入内存；全局仅保留"上一行"的键，因此 row group 内部
  与跨 row group 边界使用同一套相邻比较逻辑；发现首个违反即提前终止。
- **字典序**：复合键按规则列顺序逐列比较，第一列相等才比较下一列；全部列相等
  视为相等键，允许出现且不构成违反（稳定排序契约）。
- **NULL 规则与方向正交**：`NULLS FIRST` 时 NULL 小于任何非 NULL 值，
  `NULLS LAST` 时大于任何非 NULL 值，与 ASC/DESC 无关。
- **浮点 NaN 约定**：NaN 视为大于所有非 NaN 有限值（同 Spark 排序约定），
  两个 NaN 相等；响应中 NaN 序列化为字符串 `"NaN"`。
- **支持类型**：有/无符号整数、float16/32/64、布尔、Decimal128/256、
  string/large string、binary/large/fixed-size binary、date、time、
  timestamp（含时区）、duration。
- **不支持类型**：list、struct、map、union、dictionary、null、extension 等，
  命中即返回 `unsupported_column_type` 错误，不给出通过结论。
- **其他 JSON 约定**：Decimal/date/time/timestamp 输出 ISO 或字符串形式，
  二进制输出 `{"binary_hex": "..."}`。

## 项目结构

```
app/
  checker.py            # 核心核查引擎（流式、类型校验、比较、摘要）
  main.py               # FastAPI 路由与上传处理
  templates/index.html  # 单页浏览器 UI（原生 JS，无构建步骤）
tests/
  parquet_factory.py    # 多 row group 样本生成
  test_checker.py       # 引擎单元测试（20 个）
  test_api.py           # HTTP 端到端测试（9 个）
Dockerfile
docker-compose.yml
requirements.txt
run_tests.sh
```

## 自测

```bash
./run_tests.sh
# 或：.venv/bin/python -m unittest discover -s tests -v
```

## 说明与限制

- 上传文件落临时目录后由 PyArrow 随机读取，请求结束（含异常/超限）即删除；
  上传上限默认 512 MiB（`app/main.py:MAX_UPLOAD_BYTES`）。
- 本环境未安装容器运行时，Docker 镜像的实际构建/启动未在本机验证；
  Dockerfile 使用 python:3.11-slim 与官方 manylinux wheel，compose 配置已通过 YAML 校验。
