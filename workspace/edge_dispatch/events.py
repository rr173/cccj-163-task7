"""追加式事件日志。

这是编排器的唯一持久化事实来源：每条状态变更先以 JSON Lines
追加到日志（并 fsync），再应用到内存状态。服务崩溃重启后逐条
重放即可恢复队列、计数与时间窗，且因为只有一份追加日志，重放
顺序就是真实发生顺序，继续投放不会重复计票。
"""
from __future__ import annotations

import json
import os
import threading
from collections import deque
from typing import Callable, Deque, Optional

# 所有事件类型 ---------------------------------------------------------------
# 注册信息
EV_DEVICE_REGISTERED = "device_registered"
EV_PACKAGE_REGISTERED = "package_registered"
# 批次生命周期
EV_BATCH_CREATED = "batch_created"
EV_BATCH_ACTIVATED = "batch_activated"
EV_BATCH_PAUSED = "batch_paused"
EV_BATCH_RESUMED = "batch_resumed"
EV_BATCH_REVOKED = "batch_revoked"
EV_BATCH_COMPLETED = "batch_completed"
EV_BATCH_FAILED = "batch_failed"
EV_BATCH_WINDOW_CLOSED = "batch_window_closed"
# 设备投放推进
EV_DELIVERY_QUEUED = "delivery_queued"
EV_DELIVERY_DISPATCHED = "delivery_dispatched"
EV_DELIVERY_TIMEOUT = "delivery_timeout"
EV_DELIVERY_ACKED = "delivery_acked"
EV_DELIVERY_FAILED = "delivery_failed"
EV_DELIVERY_CANCELED = "delivery_canceled"
# 回执独立入口
EV_RECEIPT_ACCEPTED = "receipt_accepted"
EV_RECEIPT_DUPLICATE = "receipt_duplicate"     # 审计用，重放时跳过（已被首条事件覆盖）
EV_RECEIPT_BUFFERED = "receipt_buffered"
EV_RECEIPT_REJECTED = "receipt_rejected"
EV_RECEIPT_CONFLICT = "receipt_conflict"

# 重放时无需单独应用的审计事件
AUDIT_ONLY_EVENTS = {EV_RECEIPT_DUPLICATE, EV_RECEIPT_CONFLICT}


class EventStore:
    """文件追加 + 内存环形缓冲（供 SSE 最近事件与时间线查询）。"""

    def __init__(self, path: str, recent_size: int = 1000):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self._fh = open(path, "a+", encoding="utf-8")
        self.recent: Deque[dict] = deque(maxlen=recent_size)
        self._subscribers: list[Callable[[dict], None]] = []
        self._seq = 0
        # 启动时读回已有事件（仅填充内存，应用由 Orchestrator 重放完成）
        self._fh.seek(0)
        for line in self._fh:
            line = line.strip()
            if line:
                self.recent.append(json.loads(line))
        self._fh.seek(0, os.SEEK_END)
        if self.recent:
            self._seq = self.recent[-1]["id"]

    @property
    def is_empty(self) -> bool:
        return len(self.recent) == 0

    def append(self, event_type: str, payload: dict, now: float) -> dict:
        """序列化追加：调用方持编排器全局锁，这里再加文件锁保证落盘原子。

        先 fsync 再发布给订阅者，保证订阅者看到的事件都已持久化。
        """
        event = {"id": self._seq + 1, "type": event_type, "ts": now, "payload": payload}
        line = json.dumps(event, ensure_ascii=False)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._seq = event["id"]
            self.recent.append(event)
        for cb in list(self._subscribers):
            try:
                cb(event)
            except Exception:
                # 订阅者异常不影响编排器
                pass
        return event

    def replay(self) -> list[dict]:
        """按真实发生顺序返回全部持久化事件。"""
        return list(self.recent)

    def subscribe(self, cb: Callable[[dict], None]) -> Callable[[], None]:
        self._subscribers.append(cb)
        return lambda: self._subscribers.remove(cb) if cb in self._subscribers else None

    def close(self) -> None:
        with self._lock:
            self._fh.close()
