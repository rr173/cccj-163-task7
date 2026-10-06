"""验收 1：正常投放——创建、时间窗激活、按分组并发下发、回执达标完成。"""
from __future__ import annotations

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_normal_dispatch_happy_path(env):
    client = client = env["client"]
    # 华北分组 4 台设备，阈值 2，分组并发 2
    batch = create_simple_batch(env["client"], groups=("north",), threshold=2,
                                concurrency=2, timeout=30)
    bid = batch["batch_id"]

    # 一拍：时间窗激活，按并发上限恰好下发 2 台
    seqs = dispatch_and_get_seqs(env, bid, expected=2)
    assert set(seqs.values()) == {1}          # 两台设备各自的 1 号回执
    devices = list(seqs)

    view = client.get(f"/api/batches/{bid}").json()
    assert view["status"] == "dispatching"
    assert view["in_flight_count"] == 2

    # 第一台回执 → 未达标，批次继续
    r1 = client.post("/api/receipts",
                     json=make_receipt(env["orch"], bid, devices[0], seqs[devices[0]]))
    assert r1.status_code == 200
    assert r1.json()["outcome"] == "accepted"

    # 第二台回执 → 达到阈值 2，批次完成，剩余未决投放全部取消
    r2 = client.post("/api/receipts",
                     json=make_receipt(env["orch"], bid, devices[1], seqs[devices[1]]))
    assert r2.json()["outcome"] == "accepted"

    view = client.get(f"/api/batches/{bid}").json()
    assert view["status"] == "completed"
    assert view["acked_count"] == 2
    assert view["state_counts"]["canceled"] == 2

    # 完成后不再下发新设备：再推进一拍，dispatched 为空
    again = env["orch"].tick()
    assert again["dispatched"] == []

    # 时间线包含完整生命周期事件
    tl = client.get(f"/api/batches/{bid}/timeline").json()
    types = [e["type"] for e in tl]
    for required in ("batch_created", "batch_activated", "delivery_dispatched",
                     "delivery_acked", "batch_completed", "delivery_canceled"):
        assert required in types, f"时间线缺少 {required}"

    # 摘要可下载，JSON 与 CSV 两种格式
    summary = client.get(f"/api/batches/{bid}/summary").json()
    assert summary["threshold_met"] is True
    assert summary["acked"] == 2
    csv_text = client.get(f"/api/batches/{bid}/summary?format=csv").text
    assert "device_id,group,state" in csv_text
    assert "acked" in csv_text
