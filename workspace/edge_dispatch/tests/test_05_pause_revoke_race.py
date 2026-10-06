"""验收 5：暂停/撤销与回执到达竞争——只能产生一个确定状态。"""
from __future__ import annotations

import threading

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_pause_then_ack_still_counts_and_can_complete(env):
    """暂停期间回执仍可到达，达标即完成（确定性：暂停不丢回执）。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("north",), threshold=1, concurrency=1)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=1)
    device = next(iter(seqs))

    env["orch"].pause_batch(bid)
    assert client.get(f"/api/batches/{bid}").json()["status"] == "paused"

    # 暂停时推进：不会有新下发
    assert env["orch"].tick()["dispatched"] == []

    # 回执到达 → 达标，批次完成
    r = client.post("/api/receipts", json=make_receipt(env["orch"], bid, device, seqs[device]))
    assert r.json()["outcome"] == "accepted"
    assert client.get(f"/api/batches/{bid}").json()["status"] == "completed"


def test_revoke_cancels_inflight_and_rejects_later_receipt(env):
    """撤销后在途设备全部取消，迟到回执被拒，撤销为终态。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=3, concurrency=2)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=2)

    env["orch"].revoke_batch(bid)
    view = client.get(f"/api/batches/{bid}").json()
    assert view["status"] in ("revoked", "failed")
    assert view["in_flight_count"] == 0
    assert view["state_counts"]["canceled"] >= 2

    # 迟到回执 → 拒绝
    device = sorted(seqs)[0]
    r = client.post("/api/receipts", json=make_receipt(env["orch"], bid, device, seqs[device]))
    assert r.json()["outcome"] == "rejected"
    assert r.json()["reason_code"] == "delivery_canceled"

    # 终态后暂停报 409，重复撤销幂等
    assert client.post(f"/api/batches/{bid}/pause", json={}).status_code == 409
    assert client.post(f"/api/batches/{bid}/revoke", json={}).status_code == 200


def test_concurrent_pause_revoke_receipt_single_outcome(env):
    """竞争压力：暂停、撤销、回执同时发起，任何时刻只能观察到一个确定终态。"""
    client = env["client"]
    for iteration in range(20):
        # 每个批次单设备、阈值 1，让 ack 与 revoke 都可能“赢”
        # 用不同分组避免相互干扰：注册若干临时分组
        grp = f"race-{iteration}"
        env["orch"].register_device(f"race-dev-{iteration}", grp)
        body = {
            "package_id": "map-basemap", "groups": [grp],
            "window_start": env["clock"].now(),
            "window_end": env["clock"].now() + 1000,
            "max_concurrency": 1, "ack_threshold": 1,
            "timeout_secs": 30, "max_attempts": 3,
        }
        r = client.post("/api/batches", json=body)
        assert r.status_code == 201, r.text
        bid = r.json()["batch_id"]
        dispatch_and_get_seqs(env, bid, expected=1)
        device = f"race-dev-{iteration}"
        seq = 1
        receipt = make_receipt(env["orch"], bid, device, seq)

        barrier = threading.Barrier(3)
        outcomes = []

        def pause():
            barrier.wait()
            try:
                outcomes.append(("pause", env["orch"].pause_batch(bid).status.value))
            except Exception as e:
                outcomes.append(("pause-err", getattr(e, "code", str(e))))

        def revoke():
            barrier.wait()
            try:
                outcomes.append(("revoke", env["orch"].revoke_batch(bid).status.value))
            except Exception as e:
                outcomes.append(("revoke-err", getattr(e, "code", str(e))))

        def ack():
            barrier.wait()
            try:
                res = client.post("/api/receipts", json=receipt)
                outcomes.append(("ack", res.json()["outcome"]))
            except Exception as e:
                outcomes.append(("ack-err", str(e)))

        threads = [threading.Thread(target=fn) for fn in (pause, revoke, ack)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = client.get(f"/api/batches/{bid}").json()["status"]
        # 只有两个可能的确定终态：ack 赢→completed，或 revoke 赢→revoked/failed
        assert final in ("completed", "revoked", "failed"), outcomes
        # 且 acked_count 只能是 0 或 1，绝不可能出现“撤销又计票”的矛盾
        acked = client.get(f"/api/batches/{bid}").json()["acked_count"]
        if final == "completed":
            assert acked == 1
        if final in ("revoked", "failed"):
            assert acked in (0, 1)
            # 若撤销赢，ack 必须是 rejected（不可能 accepted 却没完成）
            ack_outcome = [o for k, o in outcomes if k == "ack"][0]
            if acked == 0:
                assert ack_outcome == "rejected"
