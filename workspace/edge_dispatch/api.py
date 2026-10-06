"""FastAPI HTTP 入口：网页操作面 + 回执独立入口 + SSE 实时事件流。"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import queue
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import events as ev
from .models import ReceiptOutcome
from .orchestrator import DomainError, Orchestrator, Scheduler
from .receipts import compute_checksum


# ----------------------------------------------------------------- 时间工具

def parse_iso(value) -> Optional[float]:
    """支持 ISO 8601（Z 结尾）或 epoch 秒；空值返回 None。"""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.replace(".", "", 1).isdigit():
        return float(text)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).timestamp()


def iso(ts: Optional[float]) -> Optional[str]:
    if not ts:  # None 与 0.0（未发生）都渲染为空
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def enrich_event(e: dict) -> dict:
    out = dict(e)
    out["iso_ts"] = iso(e["ts"])
    return out


# ----------------------------------------------------------------- 请求模型

class DeviceIn(BaseModel):
    device_id: str
    group: str
    name: str = ""


class PackageIn(BaseModel):
    package_id: str
    version: str
    name: str = ""


class BatchIn(BaseModel):
    package_id: str
    groups: list[str]
    window_start: Optional[str | float] = None   # ISO8601 / epoch，空=立即开始
    window_end: str | float
    max_concurrency: int = Field(ge=1)
    ack_threshold: int = Field(ge=1)
    timeout_secs: Optional[float] = None
    max_attempts: Optional[int] = None


class PauseIn(BaseModel):
    reason: str = "operator_pause"


class RevokeIn(BaseModel):
    reason: str = "operator_revoke"


# ----------------------------------------------------------------- 应用工厂

def create_app(orch: Orchestrator, scheduler: Optional[Scheduler] = None,
               static_dir: Optional[str] = None) -> FastAPI:
    app = FastAPI(title="边缘地图包分批投放编排器", version="1.0.0")
    app.state.orch = orch
    app.state.scheduler = scheduler

    @app.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError):
        return JSONResponse(status_code=exc.http_status,
                            content={"error": exc.code, "message": str(exc)})

    # -- 注册 ---------------------------------------------------------------

    @app.post("/api/devices", status_code=201)
    def add_device(body: DeviceIn):
        d = orch.register_device(body.device_id, body.group, body.name)
        return {"device_id": d.device_id, "group": d.group, "name": d.name}

    @app.post("/api/packages", status_code=201)
    def add_package(body: PackageIn):
        p = orch.register_package(body.package_id, body.version, body.name)
        return {"package_id": p.package_id, "version": p.version, "name": p.name}

    @app.get("/api/devices")
    def list_devices():
        return [{"device_id": d.device_id, "group": d.group, "name": d.name}
                for d in sorted(orch.devices.values(), key=lambda x: (x.group, x.device_id))]

    @app.get("/api/packages")
    def list_packages():
        return [{"package_id": p.package_id, "version": p.version, "name": p.name}
                for p in orch.packages.values()]

    @app.get("/api/groups")
    def list_groups():
        groups: dict[str, list[str]] = {}
        for d in orch.devices.values():
            groups.setdefault(d.group, []).append(d.device_id)
        return [{"group": g, "devices": sorted(devs),
                 "in_flight": orch._group_inflight(g),
                 "cap": orch._group_cap(g)}
                for g, devs in sorted(groups.items())]

    # -- 批次 ---------------------------------------------------------------

    @app.post("/api/batches", status_code=201)
    def create_batch(body: BatchIn):
        start = parse_iso(body.window_start)
        end = parse_iso(body.window_end)
        batch = orch.create_batch(
            package_id=body.package_id, groups=body.groups,
            window_start=start, window_end=end,
            max_concurrency=body.max_concurrency, ack_threshold=body.ack_threshold,
            timeout_secs=body.timeout_secs, max_attempts=body.max_attempts)
        return orch.batch_view(batch.batch_id)

    @app.get("/api/batches")
    def list_batches():
        return orch.list_batches()

    @app.get("/api/batches/{batch_id}")
    def get_batch(batch_id: str):
        view = orch.batch_view(batch_id)
        view["window_start_iso"] = iso(view["window_start"])
        view["window_end_iso"] = iso(view["window_end"])
        view["created_at_iso"] = iso(view["created_at"])
        for d in view["deliveries"]:
            d["dispatched_at_iso"] = iso(d["dispatched_at"])
            d["deadline_iso"] = iso(d["deadline"]) if d["deadline"] else None
            d["acked_at_iso"] = iso(d["acked_at"])
        return view

    @app.post("/api/batches/{batch_id}/pause")
    def pause_batch(batch_id: str, body: PauseIn = PauseIn()):
        return orch.batch_view(orch.pause_batch(batch_id, body.reason).batch_id)

    @app.post("/api/batches/{batch_id}/resume")
    def resume_batch(batch_id: str):
        return orch.batch_view(orch.resume_batch(batch_id).batch_id)

    @app.post("/api/batches/{batch_id}/revoke")
    def revoke_batch(batch_id: str, body: RevokeIn = RevokeIn()):
        return orch.batch_view(orch.revoke_batch(batch_id, body.reason).batch_id)

    @app.post("/api/tick")
    def manual_tick():
        """手动推进一个调度周期（页面/测试使用）。"""
        return orch.tick()

    # -- 回执独立入口 --------------------------------------------------------

    @app.post("/api/receipts")
    async def receive_receipt(request: Request):
        try:
            receipt = await request.json()
        except Exception:
            return JSONResponse(status_code=400,
                                content={"error": "bad_json", "message": "回执必须是 JSON"})
        result = orch.receive_receipt(receipt)
        status = 200
        if result["outcome"] == ReceiptOutcome.CONFLICT.value:
            status = 409
        return JSONResponse(status_code=status, content=result)

    @app.post("/api/receipts/with-checksum")
    async def receive_with_checksum(request: Request):
        """便利入口：服务端按规范计算 checksum，便于命令行/页面模拟设备。

        生产语义上 checksum 应由设备端计算；此入口仅用于测试与演示，
        内部补全后走与正式入口完全相同的只处理一次逻辑。
        """
        try:
            data = await request.json()
        except Exception:
            return JSONResponse(status_code=400,
                                content={"error": "bad_json", "message": "回执必须是 JSON"})
        data.pop("checksum", None)
        data["checksum"] = compute_checksum(data)
        result = orch.receive_receipt(data)
        status = 409 if result["outcome"] == ReceiptOutcome.CONFLICT.value else 200
        return JSONResponse(status_code=status,
                            content={"receipt": data, "result": result})

    # -- 时间线与摘要 --------------------------------------------------------

    @app.get("/api/batches/{batch_id}/timeline")
    def batch_timeline(batch_id: str, group: Optional[str] = None,
                       device_id: Optional[str] = None):
        return [enrich_event(e)
                for e in orch.timeline(batch_id, group=group, device_id=device_id)]

    @app.get("/api/batches/{batch_id}/summary")
    def batch_summary(batch_id: str, format: str = "json"):
        view = orch.batch_view(batch_id)
        counts = view["state_counts"]
        total = len(view["deliveries"])
        summary = {
            "batch_id": batch_id,
            "package_id": view["package_id"],
            "package_version": view["package_version"],
            "groups": view["groups"],
            "status": view["status"],
            "window": {"start": iso(view["window_start"]), "end": iso(view["window_end"])},
            "max_concurrency": view["max_concurrency"],
            "ack_threshold": view["ack_threshold"],
            "total_devices": total,
            "acked": counts.get("acked", 0),
            "in_flight": counts.get("in_flight", 0),
            "pending": counts.get("pending", 0),
            "retry_wait": counts.get("retry_wait", 0),
            "failed": counts.get("failed", 0),
            "canceled": counts.get("canceled", 0),
            "buffered_receipts": len(view["buffered_seqs"]),
            "threshold_met": view["acked_count"] >= view["ack_threshold"],
            "generated_at": iso(time.time()),
            "deliveries": [
                {
                    "device_id": d["device_id"], "group": d["group"],
                    "state": d["state"], "attempt": d["attempt"],
                    "last_seq": d["seq"],
                    "dispatched_at": iso(d["dispatched_at"]),
                    "acked_at": iso(d["acked_at"]),
                    "fail_reason": d["fail_reason"],
                    "canceled_reason": d["canceled_reason"],
                }
                for d in view["deliveries"]
            ],
        }
        if format == "csv":
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["device_id", "group", "state", "attempt", "last_seq",
                        "dispatched_at", "acked_at", "fail_reason", "canceled_reason"])
            for d in summary["deliveries"]:
                w.writerow([d["device_id"], d["group"], d["state"], d["attempt"],
                            d["last_seq"], d["dispatched_at"] or "",
                            d["acked_at"] or "", d["fail_reason"] or "",
                            d["canceled_reason"] or ""])
            return StreamingResponse(
                iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                headers={"Content-Disposition":
                         f'attachment; filename="batch-{batch_id}-summary.csv"'})
        return summary

    # -- 事件流 --------------------------------------------------------------

    @app.get("/api/events")
    async def events(since_id: int = 0):
        q: queue.Queue = queue.Queue(maxsize=200)
        unsub = orch.store.subscribe(lambda e: q.put(e))

        async def stream():
            # 先补发请求时错过的最近事件
            try:
                for e in orch.store.recent:
                    if e["id"] > since_id:
                        yield f"id: {e['id']}\ndata: {json.dumps(enrich_event(e), ensure_ascii=False)}\n\n"
                while True:
                    try:
                        e = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: q.get(timeout=15))
                        yield f"id: {e['id']}\ndata: {json.dumps(enrich_event(e), ensure_ascii=False)}\n\n"
                    except queue.Empty:
                        yield ": keep-alive\n\n"
            finally:
                unsub()

        return StreamingResponse(stream(), media_type="text/event-stream")

    @app.get("/api/events/recent")
    def recent_events(limit: int = 100):
        events = list(orch.store.recent)[-limit:]
        return [enrich_event(e) for e in events]

    # -- 静态页面 ------------------------------------------------------------

    if static_dir is None:
        static_dir = os.path.join(os.path.dirname(__file__), "web")

    @app.get("/", response_class=HTMLResponse)
    def index():
        with open(os.path.join(static_dir, "index.html"), encoding="utf-8") as f:
            return f.read()

    return app
