"""并发压力：高并发重复回执 + tick + 暂停/撤销，只计票一次、状态唯一确定。"""
from __future__ import annotations

import threading

from conftest import dispatch_and_get_seqs, make_receipt


def test_parallel_duplicate_receipts_count_once(env):
    client = env["client"]
    env["orch"].register_device("stress-1", "stress")
    body = {"package_id": "map-basemap", "groups": ["stress"],
            "window_start": env["clock"].now(), "window_end": env["clock"].now() + 1000,
            "max_concurrency": 1, "ack_threshold": 1,
            "timeout_secs": 30, "max_attempts": 2}
    bid = client.post("/api/batches", json=body).json()["batch_id"]
    dispatch_and_get_seqs(env, bid, expected=1)
    receipt = make_receipt(env["orch"], bid, "stress-1", 1)

    accepted, duplicate, barrier = [], [], threading.Barrier(16)

    def submit():
        barrier.wait()
        r = env["orch"].receive_receipt(dict(receipt))
        (accepted if not r.get("duplicate") else duplicate).append(r["outcome"])

    def tick():
        barrier.wait()
        for _ in range(5):
            env["orch"].tick()

    threads = [threading.Thread(target=submit) for _ in range(15)]
    threads.append(threading.Thread(target=tick))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    view = client.get(f"/api/batches/{bid}").json()
    assert view["acked_count"] == 1, "高并发重复回执只能计票一次"
    assert view["status"] == "completed"
    firsts = [o for o in accepted]
    assert firsts.count("accepted") == 1, f"首次接受必须恰有一次: {firsts}"
    assert len(duplicate) == 14


def test_parallel_revoke_or_ack_never_both(env):
    """撤销与回执大量并发：终态唯一，accepted 与 revoked 语义绝不并存。"""
    for i in range(10):
        grp = f"rc-{i}"
        env["orch"].register_device(f"rc-dev-{i}", grp)
        body = {"package_id": "map-basemap", "groups": [grp],
                "window_start": env["clock"].now(),
                "window_end": env["clock"].now() + 1000,
                "max_concurrency": 1, "ack_threshold": 1,
                "timeout_secs": 30, "max_attempts": 2}
        bid = client_post(env, body)
        dispatch_and_get_seqs(env, bid, expected=1)
        receipt = make_receipt(env["orch"], bid, f"rc-dev-{i}", 1)

        results = []
        barrier = threading.Barrier(4)

        def ack():
            barrier.wait()
            results.append(("ack", env["orch"].receive_receipt(dict(receipt))["outcome"]))

        def revoke():
            barrier.wait()
            try:
                env["orch"].revoke_batch(bid)
                results.append(("revoke", "revoked"))
            except Exception as e:
                results.append(("revoke", getattr(e, "code", "terminal")))

        threads = [threading.Thread(target=ack), threading.Thread(target=ack),
                   threading.Thread(target=revoke), threading.Thread(target=revoke)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        view = env["client"].get(f"/api/batches/{bid}").json()
        status = view["status"]
        assert status in ("completed", "revoked"), results
        assert view["acked_count"] in (0, 1)
        # 互斥不变量：完成必然 1 票；撤销必然 0 票
        if status == "completed":
            assert view["acked_count"] == 1
        else:
            assert view["acked_count"] == 0
            assert all(o != "accepted" for k, o in results if k == "ack"), results


def client_post(env, body):
    r = env["client"].post("/api/batches", json=body)
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]
