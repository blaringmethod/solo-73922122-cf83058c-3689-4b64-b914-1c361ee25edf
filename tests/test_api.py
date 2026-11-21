"""FastAPI 端到端测试：通过 TestClient 走真实 multipart 上传链路。"""

from __future__ import annotations

import io
import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app.main import app
from tests.parquet_factory import write_parquet

client = TestClient(app)


def file_upload(path: str, name: str = "sample.parquet"):
    with open(path, "rb") as fh:
        data = fh.read()
    return {"file": (name, io.BytesIO(data), "application/octet-stream")}


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        t = pa.table({"a": [1, 3, 5, 2, 4, 6], "b": ["a", "b", "c", "d", "e", "f"]})
        cls.path = write_parquet(t, row_group_size=3, name="api_sample.parquet")
        t2 = pa.table({"id": [1, 2], "nested": [{"x": 1}, {"x": 2}]})
        cls.struct_path = write_parquet(t2, row_group_size=1, name="api_struct.parquet")

    def test_healthz(self):
        resp = client.get("/healthz")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

    def test_index_page(self):
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Parquet 排序契约核查台", resp.text)

    def test_inspect_ok(self):
        resp = client.post("/api/inspect", files=file_upload(self.path))
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["num_row_groups"], 2)
        self.assertEqual(body["num_rows"], 6)
        names = {c["name"] for c in body["columns"]}
        self.assertEqual(names, {"a", "b"})
        self.assertTrue(all(c["supported"] for c in body["columns"]))

    def test_inspect_bad_file(self):
        files = {"file": ("x.parquet", io.BytesIO(b"not parquet" * 20), "application/octet-stream")}
        resp = client.post("/api/inspect", files=files)
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.json()["error_code"], "parquet_read_error")

    def test_check_pass_and_violation(self):
        # 按 b 升序是全序的（a~f）
        rules = json.dumps({"rules": [{"column": "b", "direction": "ASC", "nulls": "NULLS FIRST"}]})
        resp = client.post(
            "/api/check", files=file_upload(self.path), data={"rules": rules}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "pass")
        self.assertEqual(len(body["row_group_summaries"]), 2)

        # 按 a 升序：边界 5 -> 2 违反
        rules_bad = json.dumps({"rules": [{"column": "a", "direction": "ASC", "nulls": "NULLS FIRST"}]})
        resp2 = client.post(
            "/api/check", files=file_upload(self.path), data={"rules": rules_bad}
        )
        body2 = resp2.json()
        self.assertEqual(body2["status"], "violation")
        v = body2["violation"]
        self.assertEqual(v["prev"]["file_row"], 3)
        self.assertEqual(v["curr"]["file_row"], 4)
        self.assertEqual(v["prev"]["row_group_id"], 1)
        self.assertEqual(v["curr"]["row_group_id"], 2)
        self.assertEqual(v["prev"]["key"], [5])
        self.assertEqual(v["curr"]["key"], [2])

    def test_check_unsupported_type_no_pass_verdict(self):
        rules = json.dumps({"rules": [{"column": "nested", "direction": "ASC", "nulls": "NULLS FIRST"}]})
        resp = client.post(
            "/api/check", files=file_upload(self.struct_path), data={"rules": rules}
        )
        body = resp.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["error_code"], "unsupported_column_type")

    def test_check_rule_validation(self):
        # 0 条规则：pydantic 约束（min_length=1）拒绝
        resp = client.post(
            "/api/check",
            files=file_upload(self.path),
            data={"rules": json.dumps({"rules": []})},
        )
        self.assertEqual(resp.status_code, 422)

        # 4 条规则：pydantic 约束（max_length=3）拒绝
        resp2 = client.post(
            "/api/check",
            files=file_upload(self.path),
            data={"rules": json.dumps({"rules": [
                {"column": "a", "direction": "ASC", "nulls": "NULLS FIRST"}
            ] * 4})},
        )
        self.assertEqual(resp2.status_code, 422)

        # 非法方向：pydantic Literal 拒绝
        resp3 = client.post(
            "/api/check",
            files=file_upload(self.path),
            data={"rules": json.dumps({"rules": [
                {"column": "a", "direction": "SIDEWAYS", "nulls": "NULLS FIRST"}
            ]})},
        )
        self.assertEqual(resp3.status_code, 422)

        # 坏 JSON：应用层显式返回 400
        resp4 = client.post(
            "/api/check",
            files=file_upload(self.path),
            data={"rules": "{not json"},
        )
        self.assertEqual(resp4.status_code, 400)

    def test_check_unknown_column(self):
        rules = json.dumps({"rules": [{"column": "ghost", "direction": "ASC", "nulls": "NULLS FIRST"}]})
        resp = client.post(
            "/api/check", files=file_upload(self.path), data={"rules": rules}
        )
        body = resp.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["error_code"], "rule_validation_error")

    def test_temp_files_are_cleaned_up(self):
        import glob
        before = set(glob.glob("/tmp/tmp*.parquet"))
        client.post(
            "/api/check",
            files=file_upload(self.path),
            data={"rules": json.dumps({"rules": [
                {"column": "a", "direction": "ASC", "nulls": "NULLS FIRST"}
            ]})},
        )
        after = set(glob.glob("/tmp/tmp*.parquet"))
        self.assertEqual(before, after, "请求结束后存在未清理的临时 Parquet 文件")

    def test_oversize_upload_rejected_and_cleaned(self):
        import glob
        from app import main

        # 用小限额触发超限，避免在测试里真实构造 512 MiB 文件
        original_limit = main.MAX_UPLOAD_BYTES
        main.MAX_UPLOAD_BYTES = 4096
        try:
            big = b"x" * (4096 * 3)
            files = {"file": ("big.parquet", io.BytesIO(big), "application/octet-stream")}
            before = set(glob.glob("/tmp/tmp*.parquet"))
            resp = client.post("/api/inspect", files=files)
            self.assertEqual(resp.status_code, 413)
            after = set(glob.glob("/tmp/tmp*.parquet"))
            self.assertEqual(before, after, "超限上传后残留了临时文件")
        finally:
            main.MAX_UPLOAD_BYTES = original_limit


if __name__ == "__main__":
    unittest.main(verbosity=2)
