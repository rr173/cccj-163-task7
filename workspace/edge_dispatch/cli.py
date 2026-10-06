"""命令行入口。

用法示例：
  python -m edge_dispatch.cli serve --data ./data
  python -m edge_dispatch.cli batches create --package map-basemap \\
      --groups north --end 2026-10-06T12:00:00Z --concurrency 2 --threshold 3
  python -m edge_dispatch.cli receipt --device edge-north-01 --seq 1 \\
      --package map-basemap --version 2.4.1 --batch batch_xxx
  python -m edge_dispatch.cli summary batch_xxx --format csv -o summary.csv

回执默认由 CLI 按规范自动计算 checksum（模拟设备端）；
传 --bad-checksum 可故意破坏摘要，验证拒绝链路。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Optional

from .receipts import compute_checksum

DEFAULT_BASE_URL = os.environ.get("EDGE_BASE_URL", "http://127.0.0.1:8088")


# ------------------------------------------------------------- HTTP 小工具

class ApiError(Exception):
    def __init__(self, status: int, body: Any):
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status}: {body}")


def request(method: str, path: str, body: Any = None,
            base_url: str = DEFAULT_BASE_URL) -> Any:
    url = base_url.rstrip("/") + path
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if not raw:
                return None
            ctype = resp.headers.get("Content-Type", "")
            if "json" in ctype:
                return json.loads(raw.decode("utf-8"))
            return raw.decode("utf-8")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
        except Exception:
            payload = raw
        raise ApiError(e.code, payload)


def print_json(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def parse_time(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    # 已是 epoch 数字则透传
    try:
        float(value)
        return value
    except ValueError:
        pass
    # 宽松解析：允许 2026-10-06T12:00:00 / 2026-10-06 12:00:00，补 Z
    text = value.strip().replace(" ", "T")
    if text.endswith("Z") or "+" in text[10:]:
        return value
    try:
        datetime.fromisoformat(text)
        return text + "Z"
    except ValueError:
        raise SystemExit(f"无法解析时间: {value}")


# ------------------------------------------------------------- 子命令

def cmd_devices(args) -> int:
    print_json(request("GET", "/api/devices", base_url=args.url))
    return 0


def cmd_packages(args) -> int:
    print_json(request("GET", "/api/packages", base_url=args.url))
    return 0


def cmd_groups(args) -> int:
    print_json(request("GET", "/api/groups", base_url=args.url))
    return 0


def cmd_register_device(args) -> int:
    body = {"device_id": args.device_id, "group": args.group, "name": args.name}
    print_json(request("POST", "/api/devices", body, base_url=args.url))
    return 0


def cmd_register_package(args) -> int:
    body = {"package_id": args.package_id, "version": args.version, "name": args.name}
    print_json(request("POST", "/api/packages", body, base_url=args.url))
    return 0


def cmd_batches(args) -> int:
    if args.action == "list":
        print_json(request("GET", "/api/batches", base_url=args.url))
        return 0
    if args.action == "create":
        body = {
            "package_id": args.package,
            "groups": args.groups,
            "window_start": parse_time(args.start),
            "window_end": parse_time(args.end),
            "max_concurrency": args.concurrency,
            "ack_threshold": args.threshold,
            "timeout_secs": args.timeout,
            "max_attempts": args.max_attempts,
        }
        print_json(request("POST", "/api/batches", body, base_url=args.url))
        return 0
    if args.action == "show":
        print_json(request("GET", f"/api/batches/{args.batch_id}", base_url=args.url))
        return 0
    if args.action == "pause":
        print_json(request("POST", f"/api/batches/{args.batch_id}/pause",
                           {"reason": args.reason}, base_url=args.url))
        return 0
    if args.action == "resume":
        print_json(request("POST", f"/api/batches/{args.batch_id}/resume",
                           base_url=args.url))
        return 0
    if args.action == "revoke":
        print_json(request("POST", f"/api/batches/{args.batch_id}/revoke",
                           {"reason": args.reason}, base_url=args.url))
        return 0
    return 2


def cmd_tick(args) -> int:
    print_json(request("POST", "/api/tick", base_url=args.url))
    return 0


def cmd_receipt(args) -> int:
    receipt = {
        "device_id": args.device,
        "seq": args.seq,
        "package_id": args.package,
        "version": args.version,
        "batch_id": args.batch,
    }
    if args.payload:
        try:
            receipt.update(json.loads(args.payload))
        except json.JSONDecodeError as e:
            raise SystemExit(f"--payload 不是合法 JSON: {e}")
    if args.bad_checksum:
        receipt["checksum"] = "0" * 64
        resp = request("POST", "/api/receipts", receipt, base_url=args.url)
        print_json(resp)
        return 0
    # 模拟设备端按规范计算摘要
    receipt["checksum"] = compute_checksum(receipt)
    try:
        resp = request("POST", "/api/receipts", receipt, base_url=args.url)
    except ApiError as e:
        # 同号冲突等 409 仍要把结果展示给操作员
        print_json(e.body)
        return 1
    print_json(resp)
    # 非接受/暂存类结论以非零码退出，方便脚本编排
    if resp.get("outcome") == "rejected":
        return 1
    return 0


def cmd_timeline(args) -> int:
    qs = []
    if args.group:
        qs.append(f"group={urllib.parse.quote(args.group)}")
    if args.device:
        qs.append(f"device_id={urllib.parse.quote(args.device)}")
    path = f"/api/batches/{args.batch_id}/timeline"
    if qs:
        path += "?" + "&".join(qs)
    print_json(request("GET", path, base_url=args.url))
    return 0


def cmd_summary(args) -> int:
    if args.format == "csv":
        raw = request("GET", f"/api/batches/{args.batch_id}/summary?format=csv",
                      base_url=args.url)
        if args.output:
            with open(args.output, "w", encoding="utf-8", newline="") as f:
                f.write(raw)
            print(f"CSV 摘要已写入 {args.output}", file=sys.stderr)
        else:
            print(raw)
        return 0
    data = request("GET", f"/api/batches/{args.batch_id}/summary", base_url=args.url)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"JSON 摘要已写入 {args.output}", file=sys.stderr)
    else:
        print_json(data)
    return 0


def cmd_serve(args) -> int:
    import uvicorn

    from .api import create_app
    from .events import EventStore
    from .orchestrator import Orchestrator, Scheduler

    store = EventStore(os.path.join(args.data, "events.log"))
    orch = Orchestrator(store, seed_if_empty=True)
    sched = Scheduler(orch, interval_secs=args.interval)
    app = create_app(orch, sched)
    sched.start()
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    finally:
        sched.stop()
        store.close()
    return 0


# ------------------------------------------------------------- 参数解析

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="edge-dispatch",
                                description="边缘地图包分批投放编排器 CLI")
    p.add_argument("--url", default=DEFAULT_BASE_URL,
                   help="编排器地址（默认 $EDGE_BASE_URL 或 http://127.0.0.1:8088）")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("devices", help="列出设备").set_defaults(func=cmd_devices)
    sub.add_parser("packages", help="列出地图包").set_defaults(func=cmd_packages)
    sub.add_parser("groups", help="列出分组与实时并发").set_defaults(func=cmd_groups)
    sub.add_parser("tick", help="手动推进一个调度周期").set_defaults(func=cmd_tick)

    rd = sub.add_parser("register-device", help="注册设备")
    rd.add_argument("--device-id", required=True)
    rd.add_argument("--group", required=True)
    rd.add_argument("--name", default="")
    rd.set_defaults(func=cmd_register_device)

    rp = sub.add_parser("register-package", help="注册地图包版本")
    rp.add_argument("--package-id", required=True)
    rp.add_argument("--version", required=True)
    rp.add_argument("--name", default="")
    rp.set_defaults(func=cmd_register_package)

    b = sub.add_parser("batches", help="批次操作")
    bsub = b.add_subparsers(dest="action", required=True)
    bsub.add_parser("list")
    bsub.add_parser("show").add_argument("batch_id")
    bc = bsub.add_parser("create")
    bc.add_argument("--package", required=True)
    bc.add_argument("--groups", nargs="+", required=True)
    bc.add_argument("--start", default=None, help="发布时间窗开始（ISO/epoch，默认立即）")
    bc.add_argument("--end", required=True, help="发布时间窗结束（ISO/epoch）")
    bc.add_argument("--concurrency", type=int, default=2, dest="concurrency")
    bc.add_argument("--threshold", type=int, required=True)
    bc.add_argument("--timeout", type=float, default=30.0, dest="timeout")
    bc.add_argument("--max-attempts", type=int, default=3, dest="max_attempts")
    bp = bsub.add_parser("pause")
    bp.add_argument("batch_id")
    bp.add_argument("--reason", default="operator_pause")
    bsub.add_parser("resume").add_argument("batch_id")
    br = bsub.add_parser("revoke")
    br.add_argument("batch_id")
    br.add_argument("--reason", default="operator_revoke")
    b.set_defaults(func=cmd_batches)

    rc = sub.add_parser("receipt", help="提交设备回执（独立入口）")
    rc.add_argument("--device", required=True)
    rc.add_argument("--seq", type=int, required=True, help="设备序号（每设备严格递增）")
    rc.add_argument("--package", required=True)
    rc.add_argument("--version", required=True)
    rc.add_argument("--batch", required=True)
    rc.add_argument("--payload", default=None, help="额外正文字段 JSON")
    rc.add_argument("--bad-checksum", action="store_true",
                    help="故意提交错误摘要（验证校验失败拒绝）")
    rc.set_defaults(func=cmd_receipt)

    tl = sub.add_parser("timeline", help="查看批次完整时间线")
    tl.add_argument("batch_id")
    tl.add_argument("--group")
    tl.add_argument("--device")
    tl.set_defaults(func=cmd_timeline)

    sm = sub.add_parser("summary", help="下载批次摘要")
    sm.add_argument("batch_id")
    sm.add_argument("--format", choices=["json", "csv"], default="json")
    sm.add_argument("-o", "--output", default=None)
    sm.set_defaults(func=cmd_summary)

    sv = sub.add_parser("serve", help="启动编排器服务")
    sv.add_argument("--data", default="./data", help="事件日志目录")
    sv.add_argument("--host", default="0.0.0.0")
    sv.add_argument("--port", type=int, default=8088)
    sv.add_argument("--interval", type=float, default=1.0, help="调度周期秒数")
    sv.set_defaults(func=cmd_serve)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ApiError as e:
        print_json(e.body if isinstance(e.body, dict) else {"error": e.body})
        return 1
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
