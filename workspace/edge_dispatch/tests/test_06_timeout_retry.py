"""验收 6：超时重试——超时进入待重试队列，重试优先且不得突破分组并发上限。"""
from __future__ import annotations

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_timeout_retry_succeeds_with_new_seq(env):
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=1,
                                concurrency=3, timeout=10, max_attempts=3)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=3)
    device = sorted(seqs)[0]

    # 时间快进超过超时：3 台全部超时；同拍待重试优先重投（有名额时），
    # 新序号=2、attempt=2，分组在途仍严格为 3，没有突破上限
    env["clock"].advance(11)
    out = env["orch"].tick()
    assert len(out["timed_out"]) == 3
    assert len(out["dispatched"]) == 3
    view = client.get(f"/api/batches/{bid}").json()
    assert view["in_flight_count"] == 3
    dlv = next(d for d in view["deliveries"] if d["device_id"] == device)
    assert dlv["attempt"] == 2 and dlv["seq"] == 2

    # 第二轮超时 → 第三轮下发（attempt=3, seq=3）
    env["clock"].advance(11)
    env["orch"].tick()
    view = client.get(f"/api/batches/{bid}").json()
    dlv = next(d for d in view["deliveries"] if d["device_id"] == device)
    assert dlv["attempt"] == 3 and dlv["seq"] == 3

    # 一台设备在最后一次尝试用新序号回执成功 → 阈值 1 达标完成
    r = client.post("/api/receipts", json=make_receipt(env["orch"], bid, device, 3))
    assert r.json()["outcome"] == "accepted"
    assert client.get(f"/api/batches/{bid}").json()["status"] == "completed"

    # 旧序号的迟到回执：已作废，拒绝
    stale = client.post("/api/receipts", json=make_receipt(env["orch"], bid, device, 1))
    assert stale.json()["outcome"] == "rejected"
    assert stale.json()["reason_code"] == "delivery_retried"


def test_retry_queue_observable_when_paused(env):
    """批次暂停时超时：设备进入待重试队列并停留，恢复后才重投。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=3,
                                concurrency=3, timeout=10, max_attempts=3)
    bid = batch["batch_id"]
    dispatch_and_get_seqs(env, bid, expected=3)

    env["orch"].pause_batch(bid)
    env["clock"].advance(11)
    env["orch"].tick()   # 暂停批次不参与新投放 → 超时设备停留在待重试队列
    view = client.get(f"/api/batches/{bid}").json()
    assert view["state_counts"]["retry_wait"] == 3
    assert view["in_flight_count"] == 0

    # 恢复后一拍：待重试全部重投
    env["orch"].resume_batch(bid)
    out = env["orch"].tick()
    assert len(out["dispatched"]) == 3
    assert env["orch"]._group_inflight("east") == 3


def test_retry_exhausted_fails_and_window_closes(env):
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=3,
                                concurrency=3, timeout=10, max_attempts=2,
                                window_secs=100)
    bid = batch["batch_id"]
    dispatch_and_get_seqs(env, bid, expected=3)

    # 第一轮超时 → 同拍重投第二轮
    env["clock"].advance(11)
    env["orch"].tick()
    view = client.get(f"/api/batches/{bid}").json()
    assert all(d["attempt"] == 2 for d in view["deliveries"])

    # 第二轮也超时 → 超过最大尝试次数，直接最终失败
    env["clock"].advance(11)
    env["orch"].tick()
    view = client.get(f"/api/batches/{bid}").json()
    assert view["state_counts"]["failed"] == 3

    # 窗口结束后无在途 → 批次失败
    env["clock"].advance(100)
    env["orch"].tick()
    view = client.get(f"/api/batches/{bid}").json()
    assert view["status"] == "failed"


def test_retry_does_not_exceed_group_cap_across_batches(env):
    """共享分组名额：暂停中的批次继续占名额，恢复后重投仍不突破分组上限。"""
    client = env["client"]
    b1 = create_simple_batch(client, groups=("north",), threshold=4, concurrency=2,
                             timeout=10, max_attempts=2)
    b2 = create_simple_batch(client, groups=("north",), threshold=4, concurrency=2,
                             timeout=10, max_attempts=2)
    out = env["orch"].tick()
    assert len(out["dispatched"]) == 2
    assert env["orch"]._group_inflight("north") == 2

    # 批次1暂停：在途名额继续占用（回执随时可能到达），任何新投放都拿不到名额
    env["orch"].pause_batch(b1["batch_id"])
    env["clock"].advance(1)
    assert env["orch"].tick()["dispatched"] == []
    assert env["orch"]._group_inflight("north") == 2

    # 两个批次都暂停：批次1的 2 台在途超时后停在待重试队列，
    # 两个批次都不能新投放 → 分组名额完全释放
    env["orch"].pause_batch(b2["batch_id"])
    env["clock"].advance(11)
    env["orch"].tick()
    assert env["orch"]._group_inflight("north") == 0
    assert env["orch"].batches[b1["batch_id"]].in_flight_count == 0

    # 恢复批次1：一拍内待重试优先重投，分组在途仍不超过上限 2
    env["orch"].resume_batch(b1["batch_id"])
    out = env["orch"].tick()
    assert len(out["dispatched"]) == 2
    assert env["orch"]._group_inflight("north") == 2
