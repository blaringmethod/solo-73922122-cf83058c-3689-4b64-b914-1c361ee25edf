"""FastAPI Web 入口：浏览器页面 + 上传/核查 JSON API。"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Literal

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ValidationError

from . import checker

MAX_UPLOAD_BYTES = 512 * 1024 * 1024  # 512 MiB 上限
UPLOAD_SUFFIX = ".parquet"

app = FastAPI(title="Parquet 排序契约核查台", version="1.0.0")

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


class RuleIn(BaseModel):
    column: str = Field(..., min_length=1)
    direction: Literal["ASC", "DESC"] = "ASC"
    nulls: Literal["NULLS FIRST", "NULLS LAST"] = "NULLS FIRST"


class CheckRequest(BaseModel):
    rules: List[RuleIn] = Field(..., min_length=1, max_length=3)


def _spool_upload(upload: UploadFile) -> str:
    """把上传内容落到临时文件（PyArrow 需要文件路径随机读），并做大小限制。"""
    if not upload.filename:
        raise HTTPException(status_code=400, detail="缺少上传文件名。")
    tmp = tempfile.NamedTemporaryFile(suffix=UPLOAD_SUFFIX, delete=False)
    tmp_path = tmp.name
    total = 0
    try:
        tmp.close()
        with open(tmp_path, "wb") as fh:
            while True:
                chunk = upload.file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail="文件超过 %d MiB 的上传上限。"
                        % (MAX_UPLOAD_BYTES // (1024 * 1024)),
                    )
                fh.write(chunk)
    except HTTPException:
        # 超限等拒绝场景下删除半成品临时文件
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    finally:
        upload.file.close()
    return tmp_path


def _checker_error_response(exc: checker.CheckerError, status_code: int) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error_code": exc.code,
            "error_message": str(exc),
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {"max_mib": MAX_UPLOAD_BYTES // (1024 * 1024)},
    )


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/inspect")
async def api_inspect(file: UploadFile = File(...)) -> JSONResponse:
    tmp_path = _spool_upload(file)
    try:
        info = checker.inspect_parquet(tmp_path, file_name=file.filename)
        return JSONResponse(content=info)
    except checker.CheckerError as exc:
        return _checker_error_response(exc, 422)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


@app.post("/api/check")
async def api_check(
    file: UploadFile = File(...),
    rules: str = Form(...),
) -> JSONResponse:
    # multipart 表单：rules 为 JSON 字符串（前端 FormData 直接携带）
    try:
        payload = json.loads(rules)
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail="rules 字段不是合法的 JSON：%s" % exc,
        )

    try:
        req = CheckRequest(**payload)
    except ValidationError as exc:
        # 复用 FastAPI 的 422 请求校验错误格式
        raise RequestValidationError(jsonable_encoder(exc.errors()))

    raw_rules: List[Dict[str, Any]] = [r.model_dump() for r in req.rules]
    tmp_path = _spool_upload(file)
    try:
        result = checker.check_parquet(
            tmp_path, raw_rules, file_name=file.filename
        )
        return JSONResponse(content=result.as_dict())
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
