"""验收 7：崩溃重启——从追加日志恢复队列、计数、时间窗与暂存区，继续投放不重复计票。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import create_simple_batch, make_receipt

from edge_dispatch.api import create_app
from edge_dispatch.events import EventStore
from edge_dispatch.orchestrator import Orchestrator


def _reopen(store, clock):
    """模拟服务重启：同一日志文件、同一时钟，构造全新编排器实例。"""
    path = store.path
    store.close()
    new_store = EventStore(path)
    return new_store, Orchestrator(new_store, clock=clock)


def test_restart_recovers_and_does_not_double_count(env):
    clock = env["clock"]
    # 单设备分组，确保队列恢复的断言精确
    env["orch"].register_device("solo-north-1", "solo")
    client = TestClient(create_app(env["orch"]))
    body = {"package_id": "map-basemap", "groups": ["solo"],
            "window_start": clock.now(), "window_end": clock.now() + 1000,
            "max_concurrency": 1, "ack_threshold": 1,
            "timeout_secs": 10, "max_attempts": 2}
    bid = client.post("/api/batches", json=body).json()["batch_id"]

    # 下发（seq1），暂停后让其超时 → 停留在待重试队列，便于恢复后验证
    assert len(env["orch"].tick()["dispatched"]) == 1
    env["orch"].pause_batch(bid)
    clock.advance(11)
    env["orch"].tick()
    view = client.get(f"/api/batches/{bid}").json()
    assert view["state_counts"]["retry_wait"] == 1

    # ---- 崩溃重启 #1：队列、时间窗恢复 ----
    store2, orch2 = _reopen(env["store"], clock)
    env["store"], env["orch"] = store2, orch2
    client = TestClient(create_app(orch2))

    b = orch2.batches[bid]
    assert b.status.value == "paused"
    assert b.acked_count == 0
    assert len([d for d in b.deliveries.values()
                if d.state.value == "retry_wait"]) == 1
    assert b.window_end > clock.now()

    # 恢复投放：待重试重新下发，序号翻为 2，attempt=2
    orch2.resume_batch(bid)
    clock.advance(1)
    out = orch2.tick()
    assert len(out["dispatched"]) == 1
    assert orch2._group_inflight("solo") == 1
    dlv = list(b.deliveries.values())[0]
    assert dlv.attempt == 2 and dlv.seq == 2

    # ---- 崩溃重启 #2：在途状态与设备序号游标恢复 ----
    store3, orch3 = _reopen(store2, clock)
    env["store"], env["orch"] = store3, orch3
    client = TestClient(create_app(orch3))
    d = list(orch3.batches[bid].deliveries.values())[0]
    assert d.state.value == "in_flight"
    assert d.seq == 2 and d.attempt == 2

    # 用新序号回执 → 接受
    r = client.post("/api/receipts", json=make_receipt(orch3, bid, "solo-north-1", 2))
    assert r.json()["outcome"] == "accepted"
    assert orch3.batches[bid].status.value == "completed"
    assert orch3.batches[bid].acked_count == 1

    # ---- 崩溃重启 #3：计数恢复，重放不重复计票 ----
    store4, orch4 = _reopen(store3, clock)
    env["store"], env["orch"] = store4, orch4
    client = TestClient(create_app(orch4))
    assert orch4.batches[bid].status.value == "completed"
    assert orch4.batches[bid].acked_count == 1
    again = client.post("/api/receipts", json=make_receipt(orch4, bid, "solo-north-1", 2))
    assert again.json()["duplicate"] is True
    assert orch4.batches[bid].acked_count == 1


def test_restart_preserves_buffered_and_paused_status(env):
    client = env["client"]
    clock = env["clock"]
    batch = create_simple_batch(client, groups=("east",), threshold=2, concurrency=2)
    bid = batch["batch_id"]
    env["orch"].tick()
    env["orch"].pause_batch(bid)
    dev = client.get(f"/api/batches/{bid}").json()["deliveries"][0]["device_id"]

    later = make_receipt(env["orch"], bid, dev, 2)
    assert client.post("/api/receipts", json=later).json()["outcome"] == "buffered"

    # 重启：暂停状态、在途计数、暂存序号全部恢复
    store2, orch2 = _reopen(env["store"], clock)
    env["store"], env["orch"] = store2, orch2
    client = TestClient(create_app(orch2))
    assert orch2.batches[bid].status.value == "paused"
    assert orch2._group_inflight("east") == 2
    assert (bid, dev, 2) in orch2.buffered

    # 恢复投放后，先超时作废 seq1、重投 seq2，暂存回执自动接受
    orch2.resume_batch(bid)
    clock.advance(31)
    orch2.tick()          # 超时（含 dev），seq1 作废
    clock.advance(1)
    orch2.tick()          # 重投 seq2，pump 自动裁决
    assert (bid, dev, 2) not in orch2.buffered
    assert orch2.batches[bid].acked_count >= 1
