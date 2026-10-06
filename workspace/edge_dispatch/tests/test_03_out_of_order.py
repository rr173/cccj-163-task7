"""验收 3：乱序回执——先暂存，前置序号补齐/作废后按序裁决，不丢不重。"""
from __future__ import annotations

import copy

from conftest import create_simple_batch, dispatch_and_get_seqs, make_receipt


def test_out_of_order_across_retry_drains_automatically(env):
    """真实乱序：seq1 在途超时（序号作废），设备先重投为 seq2，
    设备先发来 seq2 → 暂存；超时重试一拍后 seq2 命中新在途投放，自动接受。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=2,
                                concurrency=2, timeout=10, max_attempts=3)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=2)
    d1, d2 = sorted(seqs)

    # d1 的 seq2 回执先到（游标停在 1）→ 暂存
    later = make_receipt(env["orch"], bid, d1, 2)
    r = client.post("/api/receipts", json=later)
    assert r.status_code == 200
    assert r.json()["outcome"] == "buffered"
    assert client.get(f"/api/batches/{bid}").json()["buffered_seqs"] == [2]

    # 重复提交同一份暂存回执 → 返回首次 buffered 结论
    rdup = client.post("/api/receipts", json=copy.deepcopy(later))
    assert rdup.json()["outcome"] == "buffered"
    assert rdup.json()["duplicate"] is True

    # 时间推进到超时（两台都超时）。该 tick 内：seq1 作废 → retry_wait
    # → 同拍重投（d1 拿到 seq2）→ tick 末尾 pump，暂存的 seq2 命中新在途，自动接受
    env["clock"].advance(11)
    env["orch"].tick()

    # 自动接受：暂存区清空，d1 已计票一次
    assert (bid, d1, 2) not in env["orch"].buffered
    assert (bid, d1, 2) in env["orch"].receipt_index
    view = client.get(f"/api/batches/{bid}").json()
    assert view["acked_count"] == 1

    # 迟到的 seq1 回执：首次结论为“序号作废”，重复语义保持
    stale = make_receipt(env["orch"], bid, d1, 1)
    r1 = client.post("/api/receipts", json=stale)
    assert r1.json()["outcome"] == "rejected"
    assert r1.json()["reason_code"] == "delivery_retried"
    assert client.get(f"/api/batches/{bid}").json()["acked_count"] == 1

    # 已接受的 seq2 重发 → 重复 accepted，计数仍为 1
    r2 = client.post("/api/receipts", json=copy.deepcopy(later))
    assert r2.json()["outcome"] == "accepted"
    assert r2.json()["duplicate"] is True
    assert client.get(f"/api/batches/{bid}").json()["acked_count"] == 1


def test_buffered_rejected_when_batch_revoked(env):
    """暂存期间批次被撤销：暂存回执在撤销当时即被裁决拒绝，之后无滞留、不计票。"""
    client = env["client"]
    batch = create_simple_batch(client, groups=("east",), threshold=2, concurrency=2)
    bid = batch["batch_id"]
    seqs = dispatch_and_get_seqs(env, bid, expected=2)
    d1 = sorted(seqs)[0]

    later = make_receipt(env["orch"], bid, d1, 2)
    assert client.post("/api/receipts", json=later).json()["outcome"] == "buffered"

    # 撤销批次：在途投放全部取消，暂存的 seq2 当场被拒绝并记录原因
    env["orch"].revoke_batch(bid)
    assert env["orch"].batches[bid].status.value == "revoked"
    assert client.get(f"/api/batches/{bid}").json()["buffered_seqs"] == []
    assert (bid, d1, 2) in env["orch"].receipt_index
    assert env["orch"].receipt_index[(bid, d1, 2)]["reason_code"] == "delivery_canceled"

    # 迟到的 seq1：取消拒绝；seq2 重复提交：返回首次拒绝，均不计票
    first = make_receipt(env["orch"], bid, d1, 1)
    r1 = client.post("/api/receipts", json=first)
    assert r1.json()["outcome"] == "rejected"
    assert r1.json()["reason_code"] == "delivery_canceled"
    r2 = client.post("/api/receipts", json=later)
    assert r2.json()["outcome"] == "rejected"
    assert r2.json()["duplicate"] is True
    assert client.get(f"/api/batches/{bid}").json()["acked_count"] == 0

    # 两条拒绝都进入批次时间线
    tl = client.get(f"/api/batches/{bid}/timeline").json()
    rejected = [e for e in tl if e["type"] == "receipt_rejected"]
    assert {e["payload"]["seq"] for e in rejected if e["payload"]["device_id"] == d1} == {1, 2}

