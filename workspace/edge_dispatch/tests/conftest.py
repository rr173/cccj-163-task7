"""pytest 公共夹具：可控时钟 + TestClient + 回执构造助手。"""
from __future__ import annotations

import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from edge_dispatch.api import create_app
from edge_dispatch.events import EventStore
from edge_dispatch.orchestrator import Orchestrator, _Clock
from edge_dispatch.receipts import compute_checksum


class FakeClock(_Clock):
    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, secs: float) -> float:
        self.t += secs
        return self.t


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def env(tmp_path, clock):
    store = EventStore(str(tmp_path / "events.log"))
    orch = Orchestrator(store, clock=clock)
    from edge_dispatch.seed import standard_seed
    standard_seed(orch)
    app = create_app(orch)
    client = TestClient(app)
    yield {"store": store, "orch": orch, "app": app, "client": client,
           "clock": clock, "tmp": tmp_path}
    store.close()


def make_receipt(orch, batch_id: str, device_id: str, seq: int,
                 *, version: str | None = None, bad_checksum: bool = False,
                 package_id: str | None = None,
                 extra: dict | None = None) -> dict:
    """按批次信息构造一份带正确摘要的回执。"""
    batch = orch.batches[batch_id]
    receipt = {
        "device_id": device_id,
        "seq": seq,
        "package_id": package_id or batch.package_id,
        "version": version or batch.package_version,
        "batch_id": batch_id,
    }
    if extra:
        receipt.update(extra)
    receipt["checksum"] = "0" * 64 if bad_checksum else compute_checksum(receipt)
    return receipt


def create_simple_batch(client, *, groups=("north",), threshold=2, concurrency=2,
                        window_secs=100, timeout=30, max_attempts=3,
                        start_offset=0, package_id="map-basemap"):
    body = {
        "package_id": package_id, "groups": list(groups),
        "window_start": None if start_offset == 0 else f"1970-01-01T00:00:00Z",
        "window_end": "2099-01-01T00:00:00Z",
        "max_concurrency": concurrency, "ack_threshold": threshold,
        "timeout_secs": timeout, "max_attempts": max_attempts,
    }
    # 简化：直接用 epoch 数值（API 也接受）
    body["window_start"] = 1_000_000.0 + start_offset
    body["window_end"] = 1_000_000.0 + window_secs
    r = client.post("/api/batches", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def dispatch_and_get_seqs(env, batch_id: str, expected: int) -> dict[str, int]:
    """推进一拍并返回设备→下发序号。"""
    result = env["orch"].tick()
    assert len(result["dispatched"]) == expected, result
    batch = env["orch"].batches[batch_id]
    return {d.device_id: d.seq
            for d in batch.deliveries.values() if d.dispatched_at}
