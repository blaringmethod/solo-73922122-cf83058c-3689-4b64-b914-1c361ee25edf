"""核查引擎的单元测试（不启动 Web 服务）。"""

from __future__ import annotations

import math
import unittest
from datetime import date

import pyarrow as pa

from app import checker
from tests.parquet_factory import write_parquet


def asc(col, nulls="NULLS FIRST"):
    return {"column": col, "direction": "ASC", "nulls": nulls}


def desc(col, nulls="NULLS LAST"):
    return {"column": col, "direction": "DESC", "nulls": nulls}


class EngineTests(unittest.TestCase):
    def test_sorted_multi_row_group_pass(self):
        # 3 个 row group，全局升序，含组边界
        t = pa.table({"a": [1, 2, 3, 4, 5, 6, 7], "b": ["x", "y", "z", "x", "y", "z", "x"]})
        path = write_parquet(t, row_group_size=3, name="pass.parquet")
        r = checker.check_parquet(path, [asc("a")], file_name="pass.parquet")
        self.assertEqual(r.status, "pass")
        self.assertEqual(r.num_row_groups, 3)
        self.assertEqual(r.total_rows, 7)
        # 摘要：行数、行号区间
        self.assertEqual([s.row_count for s in r.row_group_summaries], [3, 3, 1])
        self.assertEqual(
            [s.first_file_row for s in r.row_group_summaries], [1, 4, 7]
        )
        self.assertEqual(
            [s.last_file_row for s in r.row_group_summaries], [3, 6, 7]
        )
        # min/max 复合键
        self.assertEqual(r.row_group_summaries[0].min_key, [1])
        self.assertEqual(r.row_group_summaries[1].max_key, [6])
        # 单列 null_count
        self.assertEqual(r.row_group_summaries[0].columns[0].null_count, 0)

    def test_violation_at_row_group_boundary(self):
        # 组内均有序，但 RG2 的首行小于 RG1 的末行 -> 跨边界违反
        t = pa.table({"a": [1, 3, 5, 2, 4, 6]})
        path = write_parquet(t, row_group_size=3, name="boundary_violation.parquet")
        r = checker.check_parquet(path, [asc("a")])
        self.assertEqual(r.status, "violation")
        v = r.violation
        self.assertEqual(v.prev_row_group_index, 0)
        self.assertEqual(v.curr_row_group_index, 1)
        self.assertEqual(v.prev_file_row, 3)
        self.assertEqual(v.curr_file_row, 4)
        self.assertEqual(v.prev_key, [5])
        self.assertEqual(v.curr_key, [2])
        self.assertIn("a", v.reason)
        # 违反后提前终止：只应扫描到第 2 个 row group
        self.assertEqual(len(r.row_group_summaries), 2)

    def test_violation_inside_row_group(self):
        t = pa.table({"a": [1, 2, 0, 4, 5, 6]})
        path = write_parquet(t, row_group_size=3, name="inner_violation.parquet")
        r = checker.check_parquet(path, [asc("a")])
        self.assertEqual(r.status, "violation")
        v = r.violation
        self.assertEqual((v.prev_file_row, v.curr_file_row), (2, 3))
        self.assertEqual((v.prev_row_group_id, v.curr_row_group_id), (1, 1))

    def test_equal_keys_keep_original_order(self):
        # 全部相同键：允许通过（稳定性），原行序天然保留
        t = pa.table({"a": [5, 5, 5, 5], "b": [1, 1, 2, 2]})
        path = write_parquet(t, row_group_size=2, name="equal_keys.parquet")
        r = checker.check_parquet(path, [asc("a"), asc("b")])
        self.assertEqual(r.status, "pass")
        self.assertEqual(r.row_group_summaries[0].min_key, [5, 1])
        self.assertEqual(r.row_group_summaries[1].max_key, [5, 2])

    def test_nulls_first_asc(self):
        # ASC + NULLS FIRST：NULL, NULL, 1, 2（跨 row group 仍全局有序）
        t = pa.table({"a": pa.array([None, None, 1, 2], type=pa.int32())})
        path = write_parquet(t, row_group_size=2, name="nulls_first.parquet")
        r = checker.check_parquet(path, [asc("a", "NULLS FIRST")])
        self.assertEqual(r.status, "pass")
        # 调换为 NULLS LAST 后，两个前导 NULL 彼此相等不违反，
        # 直到第 3 行出现非 NULL 值 1（应排在 NULL 之前）才触发
        r2 = checker.check_parquet(path, [asc("a", "NULLS LAST")])
        self.assertEqual(r2.status, "violation")
        self.assertEqual(r2.violation.curr_file_row, 3)
        # 摘要记录每组 NULL 数量
        self.assertEqual(r2.row_group_summaries[0].columns[0].null_count, 2)

    def test_nulls_last_desc(self):
        # DESC + NULLS LAST：3, 2, 1, NULL, NULL
        t = pa.table({"a": pa.array([3, 2, 1, None, None], type=pa.int32())})
        path = write_parquet(t, row_group_size=2, name="nulls_last_desc.parquet")
        r = checker.check_parquet(path, [desc("a", "NULLS LAST")])
        self.assertEqual(r.status, "pass")
        # DESC + NULLS FIRST 要求 NULL 在前，当前布局必然违反
        r2 = checker.check_parquet(path, [desc("a", "NULLS FIRST")])
        self.assertEqual(r2.status, "violation")

    def test_composite_lexicographic(self):
        # 复合键：a 升序，a 相等时 b 降序
        t = pa.table({
            "a": [1, 1, 2, 2, 3],
            "b": [9, 8, 7, 7, 1],
        })
        path = write_parquet(t, row_group_size=2, name="composite.parquet")
        r = checker.check_parquet(path, [asc("a"), desc("b")])
        self.assertEqual(r.status, "pass")
        # 若 b 也要求升序，则 (1,8) 跟在 (1,9) 后违反
        r2 = checker.check_parquet(path, [asc("a"), asc("b")])
        self.assertEqual(r2.status, "violation")
        self.assertEqual(r2.violation.prev_key, [1, 9])
        self.assertEqual(r2.violation.curr_key, [1, 8])

    def test_descending_pass(self):
        t = pa.table({"a": [10.0, 9.5, 9.5, -1.0, -2.0]})
        path = write_parquet(t, row_group_size=2, name="desc.parquet")
        r = checker.check_parquet(path, [desc("a")])
        self.assertEqual(r.status, "pass")
        # 升序检查同一文件必然违反
        r2 = checker.check_parquet(path, [asc("a")])
        self.assertEqual(r2.status, "violation")

    def test_nan_ordering_convention(self):
        # 约定：NaN 大于所有非 NaN；ASC 时 1, 2, NaN 合规
        t = pa.table({"f": [1.0, 2.0, float("nan"), float("nan")]})
        path = write_parquet(t, row_group_size=2, name="nan.parquet")
        r = checker.check_parquet(path, [asc("f")])
        self.assertEqual(r.status, "pass")
        self.assertIsNotNone(r.nan_note)
        # NaN 出现在普通值之前 -> 违反
        t2 = pa.table({"f": [float("nan"), 1.0]})
        path2 = write_parquet(t2, row_group_size=1, name="nan_bad.parquet")
        r2 = checker.check_parquet(path2, [asc("f")])
        self.assertEqual(r2.status, "violation")
        self.assertTrue(math.isnan(r2.violation.prev_key[0]))

    def test_supported_types_pass(self):
        # 规则上限 3 列，选 3 种代表性类型；其余类型由 test_jsonable_helpers 与谓词覆盖
        t = pa.table({
            "s": pa.array(["a", "b", "c"]),
            "d": pa.array([date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]),
            "dec": pa.array(["1.0", "2.0", "3.0"]).cast(pa.decimal128(5, 2)),
        })
        path = write_parquet(t, row_group_size=2, name="types.parquet")
        r = checker.check_parquet(path, [asc("s"), asc("d"), asc("dec")])
        self.assertEqual(r.status, "pass")
        # 二进制与时间戳列也应被识别为支持类型
        import pyarrow.parquet as pq
        t2 = pa.table({
            "bin": pa.array([b"a", b"b", b"c"]),
            "ts": pa.array(
                ["2024-01-01T00:00:00", "2024-01-02T00:00:00", "2024-01-03T00:00:00"]
            ).cast(pa.timestamp("us")),
        })
        path2 = write_parquet(t2, row_group_size=2, name="types2.parquet")
        info = checker.inspect_parquet(path2)
        self.assertTrue(all(c["supported"] for c in info["columns"]))
        r2 = checker.check_parquet(path2, [asc("bin"), asc("ts")])
        self.assertEqual(r2.status, "pass")

    def test_unsupported_struct_column(self):
        t = pa.table({
            "id": [1, 2],
            "nested": [{"x": 1}, {"x": 2}],
        })
        path = write_parquet(t, row_group_size=1, name="struct.parquet")
        r = checker.check_parquet(path, [asc("id"), asc("nested")])
        self.assertEqual(r.status, "error")
        self.assertEqual(r.error_code, "unsupported_column_type")
        self.assertIn("nested", r.error_message)

    def test_unsupported_dictionary_column(self):
        t = pa.table({
            "d": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, 0]), pa.array(["x", "y"])
            )
        })
        path = write_parquet(t, row_group_size=2, name="dict.parquet")
        r = checker.check_parquet(path, [asc("d")])
        self.assertEqual(r.status, "error")
        self.assertEqual(r.error_code, "unsupported_column_type")

    def test_unknown_column(self):
        t = pa.table({"a": [1, 2]})
        path = write_parquet(t, row_group_size=1, name="unknown.parquet")
        r = checker.check_parquet(path, [asc("nope")])
        self.assertEqual(r.status, "error")
        self.assertEqual(r.error_code, "rule_validation_error")

    def test_rule_count_validation(self):
        t = pa.table({"a": [1]})
        path = write_parquet(t, row_group_size=1, name="rules.parquet")
        r0 = checker.check_parquet(path, [])
        self.assertEqual(r0.error_code, "rule_validation_error")
        r4 = checker.check_parquet(path, [asc("a")] * 4)
        self.assertEqual(r4.error_code, "rule_validation_error")
        # 重复列
        rd = checker.check_parquet(path, [asc("a"), desc("a")])
        self.assertEqual(rd.error_code, "rule_validation_error")

    def test_corrupt_file_is_read_error(self):
        import tempfile, os
        fd, bad = tempfile.mkstemp(suffix=".parquet")
        try:
            os.write(fd, b"this is definitely not a parquet file" * 10)
            os.close(fd)
            r = checker.check_parquet(bad, [asc("a")], file_name="bad.parquet")
            self.assertEqual(r.status, "error")
            self.assertEqual(r.error_code, "parquet_read_error")
        finally:
            os.unlink(bad)

    def test_empty_parquet(self):
        t = pa.table({"a": pa.array([], type=pa.int32())})
        path = write_parquet(t, row_group_size=2, name="empty.parquet")
        r = checker.check_parquet(path, [asc("a")])
        self.assertEqual(r.status, "pass")
        self.assertEqual(r.total_rows, 0)
        # 空文件没有可核对的数据行；即使存在 0 行 row group，其行号区间也应为 None
        for s in r.row_group_summaries:
            self.assertEqual(s.row_count, 0)
            self.assertIsNone(s.first_file_row)
            self.assertIsNone(s.last_file_row)
            self.assertIsNone(s.min_key)

    def test_batching_does_not_change_result(self):
        # 行数跨多个 batch，验证流式分批与小 batch_size 下结论一致
        t = pa.table({"a": list(range(0, 1000, 2)) + list(range(1, 1000, 2))})
        path = write_parquet(t, row_group_size=128, name="batch.parquet")
        r_small = checker.check_parquet(path, [asc("a")], batch_size=17)
        r_def = checker.check_parquet(path, [asc("a")])
        self.assertEqual(r_small.status, "violation")
        self.assertEqual(r_def.status, "violation")
        self.assertEqual(
            r_small.violation.curr_file_row, r_def.violation.curr_file_row
        )
        self.assertEqual(r_small.violation.curr_file_row, 501)

    def test_inspect_reports_supported_flag(self):
        t = pa.table({"a": [1, 2], "nested": [{"x": 1}, {"x": 2}]})
        path = write_parquet(t, row_group_size=1, name="inspect.parquet")
        info = checker.inspect_parquet(path)
        cols = {c["name"]: c for c in info["columns"]}
        self.assertTrue(cols["a"]["supported"])
        self.assertFalse(cols["nested"]["supported"])
        self.assertEqual(info["num_row_groups"], 2)
        self.assertEqual(info["row_groups"][0]["row_count"], 1)

    def test_jsonable_helpers(self):
        from decimal import Decimal
        self.assertEqual(checker.to_jsonable(Decimal("1.50")), "1.50")
        self.assertEqual(checker.to_jsonable(float("nan")), "NaN")
        self.assertEqual(checker.to_jsonable(b"\x00\xff"), {"binary_hex": "00ff"})
        self.assertIsNone(checker.to_jsonable(None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
