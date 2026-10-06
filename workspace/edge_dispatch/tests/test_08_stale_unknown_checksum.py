"""验收 8：旧版本包、未知设备与摘要校验失败的回执必须拒绝并记录原因。"""
from __future__ import annotations

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def _one_in_flight(env, group="north"):
    batch = create_simple_batch(env["client"], groups=(group,), threshold=3, concurrency=1)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=1)
    device = next(iter(seqs))
    return bid, device, seqs[device]


def test_stale_package_rejected(env):
    client = env["client"]
    bid, device, seq = _one_in_flight(env)
    r = client.post("/api/receipts",
                    json=make_receipt(env["orch"], bid, device, seq, version="2.3.0"))
    assert r.status_code == 200
    body = r.json()
    assert body["outcome"] == "rejected"
    assert body["reason_code"] == "stale_package"
    # 拒绝也要计入审计时间线
    tl = client.get(f"/api/batches/{bid}/timeline").json()
    ev = [e for e in tl if e["type"] == "receipt_rejected"]
    assert ev and ev[0]["payload"]["reason_code"] == "stale_package"


def test_unknown_package_and_future_version_rejected(env):
    client = env["client"]
    bid, device, seq = _one_in_flight(env)
    r1 = client.post("/api/receipts",
                     json=make_receipt(env["orch"], bid, device, seq,
                                       package_id="map-nonexistent"))
    assert r1.json()["outcome"] == "rejected"
    assert r1.json()["reason_code"] == "unknown_package"

    # 被拒序号已索引；当前在途仍是 seq 1，合法版本重发同 seq 属同号不同正文冲突
    r2 = client.post("/api/receipts",
                     json=make_receipt(env["orch"], bid, device, seq, version="9.9.9"))
    # 9.9.9 正文与未知包正文不同 → 冲突（首次结论保留为 unknown_package 拒绝）
    assert r2.status_code == 409


def test_unknown_device_rejected_but_indexed_once(env):
    client = env["client"]
    bid, _, _ = _one_in_flight(env, group="east")
    receipt = make_receipt(env["orch"], bid, "edge-ghost-99", 1)
    r = client.post("/api/receipts", json=receipt)
    assert r.json()["outcome"] == "rejected"
    assert r.json()["reason"] == "未知设备，回执拒绝"

    # 未知设备的重复回执仍返回首次结果
    import copy
    r2 = client.post("/api/receipts", json=copy.deepcopy(receipt))
    assert r2.json()["outcome"] == "rejected"
    assert r2.json()["duplicate"] is True

    # 同号不同正文 → 冲突
    evil = make_receipt(env["orch"], bid, "edge-ghost-99", 1, extra={"x": 1})
    r3 = client.post("/api/receipts", json=evil)
    assert r3.status_code == 409
    assert r3.json()["outcome"] == "conflict"


def test_bad_checksum_rejected(env):
    client = env["client"]
    bid, device, seq = _one_in_flight(env)
    r = client.post("/api/receipts",
                    json=make_receipt(env["orch"], bid, device, seq, bad_checksum=True))
    assert r.json()["outcome"] == "rejected"
    assert r.json()["reason_code"] == "checksum_mismatch"
    assert client.get(f"/api/batches/{bid}").json()["acked_count"] == 0


def test_malformed_receipt_rejected(env):
    client = env["client"]
    # 缺字段 → 400
    r = client.post("/api/receipts", json={"device_id": "x"})
    assert r.status_code == 400
    # 非 JSON → 400
    r2 = client.post("/api/receipts", content=b"not-json",
                     headers={"Content-Type": "application/json"})
    assert r2.status_code == 400
