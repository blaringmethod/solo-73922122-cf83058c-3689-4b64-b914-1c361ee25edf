"""Parquet 排序契约核查引擎。

按"原文件行序"流式核对一个或多个 row group 上的全局字典序（lexicographic）：

* 逐 row group、逐批（batch）读取排序列，不把整个文件载入内存；
* 全局维护"上一行"的键，因此 row group 内部与跨 row group 的边界比较使用同一套逻辑；
* 相同键允许（相等不算违反），但行必须保持原文件行序，相等时继续推进；
* 发现首个违反时立即停止，返回相邻两行的 row group、原文件行号（从 1 开始）与键值；
* 同时汇总每个 row group 的行数、键范围（min/max 复合键）、各列 NULL 数量。

支持的列类型：整数 / 浮点（NaN 约定见下）/ 布尔 / 十进制 / 字符串 / 二进制 /
日期 / 时间（含时区）/ 时间戳（含时区）/ duration。
不支持：list / struct / map / union / dictionary / null / extension 等，
遇到即抛出 :class:`UnsupportedColumnType`，不给出通过结论。

NULL 规则（每列可独立指定 NULLS FIRST / NULLS LAST，与 ASC/DESC 正交）：

* NULLS FIRST：NULL 小于该列任何非 NULL 值；
* NULLS LAST ：NULL 大于该列任何非 NULL 值。

浮点 NaN 约定：为保证可比较的全序，NaN 被视为大于所有非 NaN 的有限值（与
Spark 的 NaN 排序约定一致）；同列两个 NaN 视为相等。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

# 每批读取的行数：控制内存占用，同时保持逐行比较语义
DEFAULT_BATCH_SIZE = 50_000


def _is_supported_arrow_type(arrow_type: pa.DataType) -> bool:
    """该 Arrow 类型是否支持按排序规则比较。

    使用 pa.types 谓词而非具体 Type 类（PyArrow 26 起具体类型类已移除）。
    """
    t = arrow_type
    return (
        pa.types.is_integer(t)
        or pa.types.is_floating(t)
        or pa.types.is_boolean(t)
        or pa.types.is_decimal(t)
        or pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
        or pa.types.is_fixed_size_binary(t)
        or pa.types.is_date(t)
        or pa.types.is_time(t)
        or pa.types.is_timestamp(t)
        or pa.types.is_duration(t)
    )


class CheckerError(Exception):
    """核查错误基类（出现即不给出通过结论）。"""

    code = "checker_error"


class ParquetReadError(CheckerError):
    """Parquet 文件无法读取或元数据解析失败。"""

    code = "parquet_read_error"


class UnsupportedColumnType(CheckerError):
    """排序列的类型不支持按规则比较。"""

    code = "unsupported_column_type"


class RuleValidationError(CheckerError):
    """提交的排序规则非法（列数、列名、方向/NULL 规则等）。"""

    code = "rule_validation_error"


class ComparisonError(CheckerError):
    """按规则比较时出现无法处理的情况。"""

    code = "comparison_error"


@dataclass(frozen=True)
class SortRule:
    """单列排序规则。"""

    column: str
    ascending: bool
    nulls_first: bool

    @property
    def direction(self) -> str:
        return "ASC" if self.ascending else "DESC"

    @property
    def nulls_rule(self) -> str:
        return "NULLS FIRST" if self.nulls_first else "NULLS LAST"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "direction": self.direction,
            "nulls": self.nulls_rule,
        }


@dataclass
class ColumnRange:
    """单个 row group 内单列的摘要。"""

    column: str
    arrow_type: str
    null_count: int = 0
    min_value: Any = None
    max_value: Any = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "arrow_type": self.arrow_type,
            "null_count": self.null_count,
            "min": to_jsonable(self.min_value),
            "max": to_jsonable(self.max_value),
        }


@dataclass
class RowGroupSummary:
    """单个 row group 的键范围摘要。"""

    row_group_index: int          # 从 0 开始
    row_group_id: int             # 展示用，从 1 开始
    row_count: int
    first_file_row: Optional[int]  # 1-based，空组为 None
    last_file_row: Optional[int]
    min_key: Optional[List[Any]] = None
    max_key: Optional[List[Any]] = None
    columns: List[ColumnRange] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "row_group_index": self.row_group_index,
            "row_group_id": self.row_group_id,
            "row_count": self.row_count,
            "first_file_row": self.first_file_row,
            "last_file_row": self.last_file_row,
            "min_key": None if self.min_key is None else [to_jsonable(v) for v in self.min_key],
            "max_key": None if self.max_key is None else [to_jsonable(v) for v in self.max_key],
            "columns": [c.as_dict() for c in self.columns],
        }


@dataclass
class Violation:
    """首个违反顺序的相邻两行。"""

    reason: str
    prev_row_group_index: int
    prev_row_group_id: int
    prev_file_row: int          # 1-based
    prev_key: List[Any]
    curr_row_group_index: int
    curr_row_group_id: int
    curr_file_row: int          # 1-based
    curr_key: List[Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "reason": self.reason,
            "prev": {
                "row_group_index": self.prev_row_group_index,
                "row_group_id": self.prev_row_group_id,
                "file_row": self.prev_file_row,
                "key": [to_jsonable(v) for v in self.prev_key],
            },
            "curr": {
                "row_group_index": self.curr_row_group_index,
                "row_group_id": self.curr_row_group_id,
                "file_row": self.curr_file_row,
                "key": [to_jsonable(v) for v in self.curr_key],
            },
        }


@dataclass
class CheckResult:
    status: str                 # "pass" | "violation" | "error"
    file_name: str
    total_rows: int
    num_row_groups: int
    batch_size: int
    rules: List[SortRule]
    row_group_summaries: List[RowGroupSummary]
    violation: Optional[Violation] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    nan_note: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "file_name": self.file_name,
            "total_rows": self.total_rows,
            "num_row_groups": self.num_row_groups,
            "batch_size": self.batch_size,
            "rules": [r.as_dict() for r in self.rules],
            "row_group_summaries": [s.as_dict() for s in self.row_group_summaries],
            "violation": None if self.violation is None else self.violation.as_dict(),
            "error_code": self.error_code,
            "error_message": self.error_message,
            "nan_note": self.nan_note,
        }


# --------------------------------------------------------------------------- #
# 规则与类型校验
# --------------------------------------------------------------------------- #

def build_rules(raw_rules: Sequence[Dict[str, Any]]) -> List[SortRule]:
    """把 API 传入的原始规则字典转成 :class:`SortRule` 并做合法性校验。"""
    if not raw_rules:
        raise RuleValidationError("至少需要提供 1 个排序列，最多 3 个。")
    if len(raw_rules) > 3:
        raise RuleValidationError("排序列最多 3 个，收到 %d 个。" % len(raw_rules))

    rules: List[SortRule] = []
    seen: set = set()
    for i, raw in enumerate(raw_rules):
        if not isinstance(raw, dict):
            raise RuleValidationError("第 %d 条规则格式非法。" % (i + 1))
        col = raw.get("column")
        if not isinstance(col, str) or not col.strip():
            raise RuleValidationError("第 %d 条规则缺少有效的列名。" % (i + 1))
        col = col.strip()
        if col in seen:
            raise RuleValidationError("排序列重复：%s" % col)
        seen.add(col)

        direction = str(raw.get("direction", "ASC")).upper()
        if direction not in ("ASC", "DESC"):
            raise RuleValidationError("列 %s 的方向必须是 ASC 或 DESC，收到 %r。" % (col, direction))

        nulls = str(raw.get("nulls", "NULLS FIRST")).upper().replace("_", " ")
        if nulls not in ("NULLS FIRST", "NULLS LAST"):
            raise RuleValidationError(
                "列 %s 的 NULL 规则必须是 NULLS FIRST 或 NULLS LAST，收到 %r。" % (col, nulls)
            )

        rules.append(
            SortRule(
                column=col,
                ascending=(direction == "ASC"),
                nulls_first=(nulls == "NULLS FIRST"),
            )
        )
    return rules


def _ensure_supported(schema: pa.Schema, rules: Sequence[SortRule]) -> None:
    names = set(schema.names)
    for rule in rules:
        if rule.column not in names:
            raise RuleValidationError(
                "列 %r 不存在。可用列：%s" % (rule.column, ", ".join(schema.names))
            )
        arrow_type = schema.field(rule.column).type
        if pa.types.is_dictionary(arrow_type):
            # dictionary 需先解码，语义依赖 index/value，这里明确拒绝，不做隐式假设
            raise UnsupportedColumnType(
                "列 %r 为 dictionary 类型（%s），不支持按规则比较；"
                "请先解码为普通类型后再核查。" % (rule.column, arrow_type)
            )
        if pa.types.is_null(arrow_type):
            raise UnsupportedColumnType(
                "列 %r 为 null 类型（全部为空值），无法建立排序契约。" % rule.column
            )
        if not _is_supported_arrow_type(arrow_type):
            raise UnsupportedColumnType(
                "列 %r 的类型 %s（%s）不支持按规则比较。"
                % (rule.column, arrow_type, type(arrow_type).__name__)
            )


# --------------------------------------------------------------------------- #
# 值比较
# --------------------------------------------------------------------------- #

def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _compare_scalars(a: Any, b: Any, rule: SortRule) -> int:
    """单列标量比较，返回 -1 / 0 / 1（基于规则方向的"期望顺序"比较）。

    返回 -1 表示 a 应排在 b 之前（合规），0 表示相等，1 表示 a 应排在 b 之后
    （若 a 是上一行、b 是当前行，则构成违反）。
    """
    # 1) NULL 优先规则（与 ASC/DESC 正交）
    a_null = a is None
    b_null = b is None
    if a_null or b_null:
        if a_null and b_null:
            return 0
        if a_null:
            return -1 if rule.nulls_first else 1
        return 1 if rule.nulls_first else -1

    # 2) NaN：约定 NaN 大于一切非 NaN 有限值；两个 NaN 相等
    a_nan, b_nan = _is_nan(a), _is_nan(b)
    if a_nan or b_nan:
        if a_nan and b_nan:
            return 0
        return 1 if a_nan else -1

    # 3) 常规比较（同列物理类型一致；Decimal/int/float 之间也可比较）
    try:
        if a < b:
            natural = -1
        elif a > b:
            natural = 1
        else:
            natural = 0
    except TypeError as exc:  # pragma: no cover - 理论上已被类型校验拦截
        raise ComparisonError(
            "列 %r 的值 %r 与 %r 无法按规则比较：%s" % (rule.column, a, b, exc)
        ) from exc

    if rule.ascending:
        return natural
    return -natural


def _compare_keys(
    prev_key: Sequence[Any], curr_key: Sequence[Any], rules: Sequence[SortRule]
) -> int:
    """复合键字典序比较：返回 -1 / 0 / 1（期望顺序下）。"""
    for a, b, rule in zip(prev_key, curr_key, rules):
        cmp = _compare_scalars(a, b, rule)
        if cmp != 0:
            return cmp
    return 0


# --------------------------------------------------------------------------- #
# JSON 序列化
# --------------------------------------------------------------------------- #

def to_jsonable(value: Any) -> Any:
    """把 PyArrow / 标准库标量转成 JSON 可序列化的值。"""
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return {"binary_hex": bytes(value).hex()}
    if isinstance(value, pa.Scalar):  # 兜底：正常路径不会走到
        return to_jsonable(value.as_py())
    return str(value)


# --------------------------------------------------------------------------- #
# 主核查流程（流式）
# --------------------------------------------------------------------------- #

def _error_result(
    file_name: str,
    rules: List[SortRule],
    exc: CheckerError,
    num_row_groups: int = 0,
    total_rows: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    summaries: Optional[List[RowGroupSummary]] = None,
) -> CheckResult:
    return CheckResult(
        status="error",
        file_name=file_name,
        total_rows=total_rows,
        num_row_groups=num_row_groups,
        batch_size=batch_size,
        rules=rules,
        row_group_summaries=summaries or [],
        error_code=exc.code,
        error_message=str(exc),
    )


def check_parquet(
    path: str,
    raw_rules: Sequence[Dict[str, Any]],
    *,
    file_name: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> CheckResult:
    """核查 Parquet 文件是否满足排序契约。

    :param path: 服务器上的 Parquet 文件路径
    :param raw_rules: 排序规则字典列表（1~3 条）
    :param batch_size: 每批读取行数
    :returns: :class:`CheckResult`，状态为 pass / violation / error
    """
    display_name = file_name or path
    try:
        rules = build_rules(raw_rules)
    except RuleValidationError as exc:
        return _error_result(display_name, [], exc)

    if batch_size <= 0:
        batch_size = DEFAULT_BATCH_SIZE

    try:
        try:
            parquet_file = pq.ParquetFile(path)
        except Exception as exc:
            raise ParquetReadError("无法以 Parquet 格式打开文件：%s" % exc) from exc

        schema = parquet_file.schema_arrow
        num_groups = parquet_file.num_row_groups
        total_rows = parquet_file.metadata.num_rows
        _ensure_supported(schema, rules)

        columns = [r.column for r in rules]
        # 只要任一排序列含浮点，就提示 NaN 约定
        nan_note = None
        if any(pa.types.is_floating(schema.field(c).type) for c in columns):
            nan_note = "浮点列中的 NaN 约定为大于所有非 NaN 值（同 Spark 排序约定），两个 NaN 视为相等。"

        summaries: List[RowGroupSummary] = []
        # 上一行的全局状态（跨 row group 保留，实现边界比较）
        prev_key: Optional[List[Any]] = None
        prev_rg_index = -1
        prev_file_row = 0
        violation: Optional[Violation] = None

        for rg_index in range(num_groups):
            rg_meta = parquet_file.metadata.row_group(rg_index)
            rg_rows = rg_meta.num_rows
            # 原文件行号 = 此前所有 row group 行数之和 + 组内偏移 + 1
            rows_before = sum(
                parquet_file.metadata.row_group(g).num_rows for g in range(rg_index)
            )

            col_ranges: Dict[str, ColumnRange] = {
                r.column: ColumnRange(column=r.column, arrow_type=str(schema.field(r.column).type))
                for r in rules
            }
            min_key: Optional[List[Any]] = None
            max_key: Optional[List[Any]] = None
            first_file_row: Optional[int] = None

            try:
                batch_iter = parquet_file.iter_batches(
                    batch_size=batch_size,
                    row_groups=[rg_index],
                    columns=columns,
                )
            except Exception as exc:
                raise ParquetReadError(
                    "读取 row group %d 失败：%s" % (rg_index + 1, exc)
                ) from exc

            local_row = 0
            try:
                for batch in batch_iter:
                    arrays = [batch.column(i) for i in range(len(columns))]
                    for offset in range(batch.num_rows):
                        key = [arr[offset].as_py() for arr in arrays]
                        file_row = rows_before + local_row + 1
                        if first_file_row is None:
                            first_file_row = file_row

                        # 组内范围摘要
                        for idx, rule in enumerate(rules):
                            value = key[idx]
                            crange = col_ranges[rule.column]
                            if value is None:
                                crange.null_count += 1
                            else:
                                if crange.min_value is None or _compare_scalars(
                                    value, crange.min_value, rule
                                ) < 0:
                                    crange.min_value = value
                                if crange.max_value is None or _compare_scalars(
                                    value, crange.max_value, rule
                                ) > 0:
                                    crange.max_value = value

                        if min_key is None or _compare_keys(key, min_key, rules) < 0:
                            min_key = list(key)
                        if max_key is None or _compare_keys(key, max_key, rules) > 0:
                            max_key = list(key)

                        # 全局顺序核对（相等允许，保持原行序）
                        if prev_key is not None:
                            cmp = _compare_keys(prev_key, key, rules)
                            if cmp > 0:
                                reason = _build_violation_reason(
                                    prev_key, key, rules
                                )
                                violation = Violation(
                                    reason=reason,
                                    prev_row_group_index=prev_rg_index,
                                    prev_row_group_id=prev_rg_index + 1,
                                    prev_file_row=prev_file_row,
                                    prev_key=list(prev_key),
                                    curr_row_group_index=rg_index,
                                    curr_row_group_id=rg_index + 1,
                                    curr_file_row=file_row,
                                    curr_key=list(key),
                                )
                                break

                        prev_key = list(key)
                        prev_rg_index = rg_index
                        prev_file_row = file_row
                        local_row += 1
            except ComparisonError:
                raise
            except Exception as exc:
                raise ParquetReadError(
                    "解析 row group %d 的数据时失败：%s" % (rg_index + 1, exc)
                ) from exc

            summaries.append(
                RowGroupSummary(
                    row_group_index=rg_index,
                    row_group_id=rg_index + 1,
                    row_count=rg_rows,
                    first_file_row=first_file_row,
                    last_file_row=(None if rg_rows == 0 else rows_before + rg_rows),
                    min_key=min_key,
                    max_key=max_key,
                    columns=list(col_ranges.values()),
                )
            )

            if violation is not None:
                break

    except CheckerError as exc:
        return _error_result(
            display_name,
            rules,
            exc,
            num_row_groups=locals().get("num_groups", 0),
            total_rows=locals().get("total_rows", 0),
            batch_size=batch_size,
            summaries=locals().get("summaries", []),
        )

    if violation is not None:
        return CheckResult(
            status="violation",
            file_name=display_name,
            total_rows=total_rows,
            num_row_groups=num_groups,
            batch_size=batch_size,
            rules=rules,
            row_group_summaries=summaries,
            violation=violation,
            nan_note=nan_note,
        )

    return CheckResult(
        status="pass",
        file_name=display_name,
        total_rows=total_rows,
        num_row_groups=num_groups,
        batch_size=batch_size,
        rules=rules,
        row_group_summaries=summaries,
        nan_note=nan_note,
    )


def _build_violation_reason(
    prev_key: Sequence[Any], curr_key: Sequence[Any], rules: Sequence[SortRule]
) -> str:
    """定位首个决定违反的列，生成中文原因。"""
    for idx, rule in enumerate(rules):
        cmp = _compare_scalars(prev_key[idx], curr_key[idx], rule)
        if cmp != 0:
            return (
                "列 %r 违反 %s %s：上一行值 %s 应排在当前行值 %s 之后。"
                % (
                    rule.column,
                    rule.direction,
                    rule.nulls_rule,
                    to_jsonable(prev_key[idx]),
                    to_jsonable(curr_key[idx]),
                )
            )
    return "复合键顺序违反（理论上相等键不会触发违反）。"


def inspect_parquet(path: str, *, file_name: Optional[str] = None) -> Dict[str, Any]:
    """只读取元数据，返回 row group 概况与列 schema，供前端渲染表单。"""
    display_name = file_name or path
    try:
        pf = pq.ParquetFile(path)
    except Exception as exc:
        raise ParquetReadError("无法以 Parquet 格式打开文件：%s" % exc) from exc

    meta = pf.metadata
    row_groups = []
    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        row_groups.append({
            "row_group_id": i + 1,
            "row_count": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
        })

    columns = []
    schema = pf.schema_arrow
    for field_ in schema:
        columns.append({
            "name": field_.name,
            "arrow_type": str(field_.type),
            "supported": _is_supported_arrow_type(field_.type)
            and not pa.types.is_dictionary(field_.type)
            and not pa.types.is_null(field_.type),
        })

    return {
        "file_name": display_name,
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "row_groups": row_groups,
        "columns": columns,
    }
