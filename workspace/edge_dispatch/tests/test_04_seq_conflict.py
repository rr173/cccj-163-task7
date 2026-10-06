"""验收 4：同号冲突——同设备同序号不同正文必须拒绝并记录原因。"""
from __future__ import annotations

import copy

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_same_seq_different_body_conflicts(env):
    client = env["client"]
    batch = create_simple_batch(client, groups=("north",), threshold=3, concurrency=1)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=1)
    device = next(iter(seqs))

    # 首份 seq=1 回执（额外字段不同 → 正文不同 → 摘要不同）
    a = make_receipt(env["orch"], bid, device, 1, extra={"firmware": "5.0"})
    assert client.post("/api/receipts", json=a).json()["outcome"] == "accepted"

    # 同号不同正文
    b = make_receipt(env["orch"], bid, device, 1, extra={"firmware": "5.1"})
    r = client.post("/api/receipts", json=b)
    assert r.status_code == 409
    assert r.json()["outcome"] == "conflict"
    assert "不同正文" in r.json()["reason"]

    # 冲突不改变首次结论：原始正文仍是 accepted，计数不变
    view = client.get(f"/api/batches/{bid}").json()
    assert view["acked_count"] == 1
    ragain = client.post("/api/receipts", json=copy.deepcopy(a))
    assert ragain.json()["outcome"] == "accepted"
    assert ragain.json()["duplicate"] is True

    # 冲突进入审计日志
    tl = client.get(f"/api/batches/{bid}/timeline").json()
    assert any(e["type"] == "receipt_conflict" for e in tl)


def test_conflict_against_buffered(env):
    """对已暂存的乱序回执提交同号不同正文 → 同样冲突拒绝。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=2, concurrency=2)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=2)
    device = sorted(seqs)[0]

    later = make_receipt(env["orch"], bid, device, 2, extra={"rssi": -70})
    assert client.post("/api/receipts", json=later).json()["outcome"] == "buffered"

    evil = make_receipt(env["orch"], bid, device, 2, extra={"rssi": -90})
    r = client.post("/api/receipts", json=evil)
    assert r.status_code == 409
    assert r.json()["outcome"] == "conflict"

    # 暂存的原始版本不受影响：seq1 到达后仍正常补齐
    first = make_receipt(env["orch"], bid, device, 1)
    out = client.post("/api/receipts", json=first).json()
    assert out["drained"][0]["seq"] == 2
