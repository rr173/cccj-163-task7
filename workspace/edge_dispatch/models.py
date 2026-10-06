"""领域模型：枚举、数据类与状态转换规则。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class BatchStatus(str, Enum):
    """批次生命周期状态。

    QUEUED      已创建，尚未进入发布时间窗（排队）
    DISPATCHING 时间窗内，调度器正在投放
    PAUSED      操作员暂停，可恢复
    COMPLETED   回执数达到阈值（终态）
    REVOKED     操作员撤销（终态，优先级最高，暂停/撤销竞争时确定胜出）
    FAILED      时间窗耗尽或全部设备最终失败，未达阈值（终态）
    """

    QUEUED = "queued"
    DISPATCHING = "dispatching"
    PAUSED = "paused"
    COMPLETED = "completed"
    REVOKED = "revoked"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (BatchStatus.COMPLETED, BatchStatus.REVOKED, BatchStatus.FAILED)

    @property
    def live(self) -> bool:
        """仍可能产生新投放（排队/投放中）。"""
        return self in (BatchStatus.QUEUED, BatchStatus.DISPATCHING)


class DeliveryState(str, Enum):
    """单个设备的投放状态。"""

    PENDING = "pending"          # 待投放队列中
    IN_FLIGHT = "in_flight"      # 已下发，等待回执
    RETRY_WAIT = "retry_wait"    # 超时等待重试，不得突破并发上限
    ACKED = "acked"              # 收到合规回执
    FAILED = "failed"            # 最终失败（重试耗尽 / 窗口关闭）
    CANCELED = "canceled"        # 批次撤销或暂停时被取消的待投放项

    @property
    def occupies_slot(self) -> bool:
        """是否占用分组并发名额（在途设备）。"""
        return self is DeliveryState.IN_FLIGHT

    @property
    def terminal(self) -> bool:
        return self in (
            DeliveryState.ACKED,
            DeliveryState.FAILED,
            DeliveryState.CANCELED,
        )


class ReceiptOutcome(str, Enum):
    """回执入口对一份回执给出的确定结论（重复回执永远返回首次结论）。"""

    ACCEPTED = "accepted"        # 首次且合规
    DUPLICATE = "duplicate"      # 同设备同序号同正文 → 返回首次结果
    BUFFERED = "buffered"        # 乱序，先暂存
    REJECTED = "rejected"        # 旧包/未知设备/校验失败/过期投放等
    CONFLICT = "conflict"        # 同号不同正文，拒绝并记录原因


# 接收后还要等待序号补齐才能裁决的暂存项
BUFFERABLE_STATES = {DeliveryState.PENDING, DeliveryState.RETRY_WAIT}


@dataclass
class Device:
    device_id: str
    group: str
    name: str = ""


@dataclass
class Package:
    package_id: str
    version: str
    name: str = ""


@dataclass
class Delivery:
    """批次内针对一台设备的一次投放尝试。attempt 从 1 开始。"""

    delivery_id: str
    batch_id: str
    device_id: str
    group: str
    seq: int
    attempt: int = 1
    state: DeliveryState = DeliveryState.PENDING
    enqueued_at: float = 0.0
    dispatched_at: float = 0.0
    deadline: float = 0.0
    acked_at: Optional[float] = None
    retry_at: Optional[float] = None
    fail_reason: Optional[str] = None
    canceled_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "delivery_id": self.delivery_id,
            "batch_id": self.batch_id,
            "device_id": self.device_id,
            "group": self.group,
            "seq": self.seq,
            "attempt": self.attempt,
            "state": self.state.value,
            "enqueued_at": self.enqueued_at,
            "dispatched_at": self.dispatched_at,
            "deadline": self.deadline,
            "acked_at": self.acked_at,
            "retry_at": self.retry_at,
            "fail_reason": self.fail_reason,
            "canceled_reason": self.canceled_reason,
        }


@dataclass
class Batch:
    batch_id: str
    package_id: str
    package_version: str          # 创建时冻结的包版本（旧包判定基准）
    groups: list[str]
    device_ids: list[str]
    window_start: float
    window_end: float
    max_concurrency: int
    ack_threshold: int
    timeout_secs: float
    max_attempts: int
    status: BatchStatus = BatchStatus.QUEUED
    created_at: float = 0.0
    # 计数派生：ack/取消/失败，均来自事件重放，不单独持久化
    deliveries: dict[str, Delivery] = field(default_factory=dict)
    device_delivery: dict[str, str] = field(default_factory=dict)

    @property
    def acked_count(self) -> int:
        return sum(1 for d in self.deliveries.values() if d.state is DeliveryState.ACKED)

    @property
    def in_flight_count(self) -> int:
        return sum(1 for d in self.deliveries.values() if d.state is DeliveryState.IN_FLIGHT)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "package_id": self.package_id,
            "package_version": self.package_version,
            "groups": self.groups,
            "device_ids": list(self.device_ids),
            "window_start": self.window_start,
            "window_end": self.window_end,
            "max_concurrency": self.max_concurrency,
            "ack_threshold": self.ack_threshold,
            "timeout_secs": self.timeout_secs,
            "max_attempts": self.max_attempts,
            "status": self.status.value,
            "created_at": self.created_at,
            "acked_count": self.acked_count,
            "in_flight_count": self.in_flight_count,
        }
