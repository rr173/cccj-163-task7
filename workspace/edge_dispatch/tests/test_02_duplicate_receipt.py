"""验收 2：重复回执——按设备序号只处理一次，重复返回首次结果，绝不重复计票。"""
from __future__ import annotations

import copy

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_duplicate_receipt_returns_first_result(env):
    client = env["client"]
    batch = create_simple_batch(client, groups=("north",), threshold=3, concurrency=1)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=1)
    device = next(iter(seqs))

    receipt = make_receipt(env["orch"], bid, device, seqs[device])

    first = client.post("/api/receipts", json=receipt)
    assert first.json()["outcome"] == "accepted"

    # 再次提交完全相同的回执：duplicate，返回首次结论 accepted
    second = client.post("/api/receipts", json=copy.deepcopy(receipt))
    assert second.status_code == 200
    body = second.json()
    assert body["outcome"] == "accepted"
    assert body["duplicate"] is True

    # 再重复两次，计数仍为 1
    for _ in range(2):
        r = client.post("/api/receipts", json=copy.deepcopy(receipt))
        assert r.json()["duplicate"] is True
    view = client.get(f"/api/batches/{bid}").json()
    assert view["acked_count"] == 1

    # 重复事件进入审计日志
    tl = client.get(f"/api/batches/{bid}/timeline").json()
    dup_events = [e for e in tl if e["type"] == "receipt_duplicate"]
    assert len(dup_events) == 3


def test_duplicate_of_rejected_stays_rejected(env):
    """被拒绝的序号同样只处理一次：重复旧包回执仍返回首次拒绝。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("north",), threshold=1, concurrency=1)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=1)
    device = next(iter(seqs))

    # 旧版本回执首次拒绝
    old = make_receipt(env["orch"], bid, device, seqs[device], version="1.0.0")
    r1 = client.post("/api/receipts", json=old)
    assert r1.json()["outcome"] == "rejected"
    assert r1.json()["reason_code"] == "stale_package"

    # 完全相同的旧包回执再来：duplicate 且首次结论仍是 rejected
    r2 = client.post("/api/receipts", json=copy.deepcopy(old))
    assert r2.json()["outcome"] == "rejected"
    assert r2.json()["duplicate"] is True
    view = client.get(f"/api/batches/{bid}").json()
    assert view["acked_count"] == 0
