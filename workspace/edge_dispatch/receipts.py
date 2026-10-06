"""回执正文的规范化、摘要与校验。

摘要规则（前后端、命令行必须一致）：
  1. 取回执对象并移除 checksum 字段；
  2. 以键排序、无空白的 UTF-8 JSON 重新序列化（sort_keys, separators=(",", ":")）；
  3. 对结果计算 SHA-256 十六进制摘要。
"""
from __future__ import annotations

import hashlib
import json

# 回执正文必须包含的字段
REQUIRED_FIELDS = ("device_id", "seq", "package_id", "version", "batch_id")


def canonical_body(receipt: dict) -> bytes:
    body = {k: v for k, v in receipt.items() if k != "checksum"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def compute_checksum(receipt: dict) -> str:
    return hashlib.sha256(canonical_body(receipt)).hexdigest()


def verify_checksum(receipt: dict) -> bool:
    claimed = receipt.get("checksum")
    if not isinstance(claimed, str):
        return False
    return hashlib.sha256(canonical_body(receipt)).hexdigest() == claimed.lower()


def missing_fields(receipt: dict) -> list[str]:
    return [f for f in REQUIRED_FIELDS if f not in receipt]


def compare_versions(a: str, b: str) -> int:
    """简单语义化版本比较：a < b 返回 -1，相等 0，a > b 返回 1。

    非数字段落按 0 处理，避免对外部包产生依赖。
    """
    def parts(v: str) -> list[int]:
        out = []
        for p in str(v).split("."):
            num = ""
            for ch in p:
                if ch.isdigit():
                    num += ch
                else:
                    break
            out.append(int(num) if num else 0)
        return out

    pa, pb = parts(a), parts(b)
    width = max(len(pa), len(pb))
    pa += [0] * (width - len(pa))
    pb += [0] * (width - len(pb))
    return (pa > pb) - (pa < pb)
