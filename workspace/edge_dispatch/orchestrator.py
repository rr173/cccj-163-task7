"""编排器核心：批次推进、回执只处理一次、暂停/撤销竞争、超时重试。

并发正确性的两条保证：

1. 全局锁串行化所有命令（回执、暂停、撤销、调度 tick），命令内部
   一律「先追加事件日志，再应用到内存」（_emit）。因此暂停/撤销与
   回执到达并发时只有一个确定的胜出者——谁先拿到锁，事件顺序即最终状态。
2. 状态全部可由事件日志重放得到。重启后重放恢复：队列、在途计数、
   每「批次+设备」序号游标、暂存回执与时间窗；ack 以 delivery_acked
   事件为准，重放天然幂等，继续投放不会重复计票。

幂等作用域：一个批次是针对一台设备的一次独立投放会话，序号在
「批次 + 设备」范围内从 1 开始（每次重试递增）。因此回执的幂等键是
(batch_id, device_id, seq)，同一设备在不同批次里的序号互不影响。
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Callable, Optional

from . import events as ev
from .models import (
    BUFFERABLE_STATES,
    Batch,
    BatchStatus,
    Delivery,
    DeliveryState,
    Device,
    Package,
    ReceiptOutcome,
)
from .receipts import compare_versions, compute_checksum, missing_fields, verify_checksum

# 回执键：(批次, 设备, 序号)；未知设备批次可能为任意值，单独索引。
ReceiptKey = tuple[str, str, int]
CursorKey = tuple[str, str]


class DomainError(Exception):
    """领域规则违反，HTTP 层映射为 4xx。"""

    def __init__(self, code: str, message: str, http_status: int = 422):
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class _Clock:
    """可被测试替换的墙钟。"""

    def now(self) -> float:
        return time.time()


class Orchestrator:
    def __init__(self, store: ev.EventStore, clock: Optional[_Clock] = None,
                 default_timeout_secs: float = 30.0, default_max_attempts: int = 3,
                 seed_if_empty: bool = False):
        self.store = store
        self.clock = clock or _Clock()
        self.default_timeout_secs = default_timeout_secs
        self.default_max_attempts = default_max_attempts
        self.lock = threading.RLock()

        self.devices: dict[str, Device] = {}
        self.packages: dict[str, Package] = {}
        self.batches: dict[str, Batch] = {}

        # 已裁决回执 (batch, device, seq) -> 首次结论
        self.receipt_index: dict[ReceiptKey, dict] = {}
        # 乱序暂存 (batch, device, seq) -> 回执（同正文重复返回 BUFFERED）
        self.buffered: dict[ReceiptKey, dict] = {}
        # 每「批次+设备」下一个等待裁决的序号游标
        self.next_seq: dict[CursorKey, int] = {}
        # 未知设备回执索引（批次可能不存在，键仍带 batch_id 以隔离）
        self.unknown_index: dict[ReceiptKey, dict] = {}

        self._replay()
        if seed_if_empty and store.is_empty:
            from .seed import standard_seed
            standard_seed(self)

    # ---------------------------------------------------------------- 工具

    def _now(self) -> float:
        return self.clock.now()

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:12]}"

    def _emit(self, event_type: str, payload: dict, now: float) -> dict:
        """所有实时状态变更的唯一入口：先持久化追加，再应用到内存。

        _apply 对同一事件幂等；崩溃重放逐条执行同一套 _apply，得到
        与实时路径完全一致的状态。
        """
        event = self.store.append(event_type, payload, now)
        self._apply(event_type, payload, now)
        return event

    def _require_batch(self, batch_id: str) -> Batch:
        batch = self.batches.get(batch_id)
        if batch is None:
            raise DomainError("unknown_batch", f"批次不存在: {batch_id}", 404)
        return batch

    @staticmethod
    def _cursor(batch_id: str, device_id: str) -> CursorKey:
        return batch_id, device_id

    # ------------------------------------------------------------ 注册接口

    def register_device(self, device_id: str, group: str, name: str = "") -> Device:
        with self.lock:
            if device_id in self.devices:
                raise DomainError("device_exists", f"设备已注册: {device_id}", 409)
            self._emit(ev.EV_DEVICE_REGISTERED,
                       {"device_id": device_id, "group": group, "name": name},
                       self._now())
            return self.devices[device_id]

    def register_package(self, package_id: str, version: str, name: str = "") -> Package:
        with self.lock:
            if package_id in self.packages:
                raise DomainError("package_exists", f"包已注册: {package_id}", 409)
            self._emit(ev.EV_PACKAGE_REGISTERED,
                       {"package_id": package_id, "version": version, "name": name},
                       self._now())
            return self.packages[package_id]

    # ------------------------------------------------------------ 批次创建

    def create_batch(self, *, package_id: str, groups: list[str],
                     window_start: Optional[float] = None, window_end: float,
                     max_concurrency: int, ack_threshold: int,
                     timeout_secs: Optional[float] = None,
                     max_attempts: Optional[int] = None) -> Batch:
        if max_concurrency < 1:
            raise DomainError("bad_concurrency", "并发上限必须 >= 1")
        if ack_threshold < 1:
            raise DomainError("bad_threshold", "回执阈值必须 >= 1")
        if window_end <= (window_start if window_start is not None else self._now()):
            raise DomainError("bad_window", "发布时间窗结束时间必须晚于开始时间")

        with self.lock:
            pkg = self.packages.get(package_id)
            if pkg is None:
                raise DomainError("unknown_package", f"未知地图包: {package_id}", 404)

            selected = [dev for dev in self.devices.values() if dev.group in groups]
            if not selected:
                raise DomainError("empty_group", "所选分组内没有任何设备")
            if ack_threshold > len(selected):
                raise DomainError("bad_threshold",
                                  f"回执阈值 {ack_threshold} 大于分组设备数 {len(selected)}")

            now = self._now()
            batch_id = self._new_id("batch")
            batch = Batch(
                batch_id=batch_id,
                package_id=package_id,
                package_version=pkg.version,
                groups=list(groups),
                device_ids=[d.device_id for d in selected],
                window_start=window_start if window_start is not None else now,
                window_end=window_end,
                max_concurrency=max_concurrency,
                ack_threshold=ack_threshold,
                timeout_secs=timeout_secs or self.default_timeout_secs,
                max_attempts=max_attempts or self.default_max_attempts,
                created_at=now,
            )
            payload = {**batch.to_dict(),
                       "timeout_secs": batch.timeout_secs,
                       "max_attempts": batch.max_attempts}
            self._emit(ev.EV_BATCH_CREATED, payload, now)

            for dev in selected:
                self._emit(ev.EV_DELIVERY_QUEUED, {
                    "delivery_id": self._new_id("dlv"),
                    "batch_id": batch_id,
                    "device_id": dev.device_id,
                    "group": dev.group,
                    "enqueued_at": now,
                }, now)
            return self.batches[batch_id]

    # ------------------------------------------------------- 暂停/恢复/撤销

    def pause_batch(self, batch_id: str, reason: str = "operator_pause") -> Batch:
        with self.lock:
            batch = self._require_batch(batch_id)
            if batch.status.terminal:
                raise DomainError("batch_terminal",
                                  f"批次已处于终态 {batch.status.value}，无法暂停", 409)
            if batch.status is BatchStatus.PAUSED:
                return batch
            self._emit(ev.EV_BATCH_PAUSED,
                       {"batch_id": batch_id, "reason": reason}, self._now())
            return batch

    def resume_batch(self, batch_id: str) -> Batch:
        with self.lock:
            batch = self._require_batch(batch_id)
            if batch.status.terminal:
                raise DomainError("batch_terminal",
                                  f"批次已处于终态 {batch.status.value}，无法恢复", 409)
            if batch.status is not BatchStatus.PAUSED:
                raise DomainError("not_paused", "批次未处于暂停状态", 409)
            self._emit(ev.EV_BATCH_RESUMED, {"batch_id": batch_id}, self._now())
            return batch

    def revoke_batch(self, batch_id: str, reason: str = "operator_revoke") -> Batch:
        with self.lock:
            batch = self._require_batch(batch_id)
            now = self._now()
            # 撤销为终态且优先级最高：与暂停/完成竞争时，先入日志者胜；
            # 已撤销则幂等，其它终态拒绝。
            if batch.status.terminal:
                if batch.status is BatchStatus.REVOKED:
                    return batch
                raise DomainError("batch_terminal",
                                  f"批次已处于终态 {batch.status.value}，无法撤销", 409)
            self._emit(ev.EV_BATCH_REVOKED,
                       {"batch_id": batch_id, "reason": reason}, now)
            for d in batch.deliveries.values():
                if not d.state.terminal:
                    self._emit(ev.EV_DELIVERY_CANCELED, {
                        "delivery_id": d.delivery_id, "batch_id": batch_id,
                        "device_id": d.device_id, "group": d.group,
                        "reason": "batch_revoked",
                    }, now)
            # 投放全部取消后，暂存的乱序回执做最终拒绝裁决，不再滞留
            self._drain_all_buffered(batch_id, now)
            return batch

    # ----------------------------------------------------------- 回执入口

    def receive_receipt(self, receipt: dict) -> dict:
        """设备回执独立入口。

          * 按 (批次, 设备, 序号) 只处理一次；重复回执返回首次结论；
          * 乱序先暂存，前置序号裁决后按序补处理；
          * 同号不同正文 → conflict，拒绝并记录原因；
          * 未知设备、旧版本包、摘要校验失败 → 拒绝并记录原因。
        """
        with self.lock:
            now = self._now()

            if not isinstance(receipt, dict):
                raise DomainError("bad_receipt", "回执必须是 JSON 对象", 400)
            missing = missing_fields(receipt)
            if missing:
                raise DomainError("bad_receipt", f"回执缺少字段: {', '.join(missing)}", 400)
            device_id = receipt["device_id"]
            batch_id = receipt["batch_id"]
            try:
                seq = int(receipt["seq"])
            except (TypeError, ValueError):
                raise DomainError("bad_receipt", "seq 必须是整数", 400)

            # 未知设备：独立索引，重复/冲突同样“只处理一次”
            if device_id not in self.devices:
                return self._handle_unknown_device(receipt, batch_id, device_id, seq, now)

            key: ReceiptKey = (batch_id, device_id, seq)
            body_hash = compute_checksum(receipt)

            # 已裁决 → 重复 / 冲突
            if key in self.receipt_index:
                first = self.receipt_index[key]
                if first.get("reason_code") == "delivery_retried":
                    # 超时作废是调度器权威结论，与正文无关，迟到回执返回同一结论
                    self._record_audit(ev.EV_RECEIPT_DUPLICATE, receipt, now,
                                       first_outcome=first["outcome"], body_hash=body_hash)
                    self._pump(batch_id, device_id, now)
                    return {"outcome": ReceiptOutcome.REJECTED.value, "duplicate": True,
                            "seq": seq, "device_id": device_id,
                            "reason_code": "delivery_retried",
                            "reason": first.get("reason")}
                return self._duplicate_or_conflict(first, receipt, key, now,
                                                   hash_ok=first["body_hash"] == body_hash)

            # 已暂存 → 同正文重复返回 buffered；不同正文冲突
            if key in self.buffered:
                first = self.buffered[key]
                if first["body_hash"] == body_hash:
                    self._record_audit(ev.EV_RECEIPT_DUPLICATE, receipt, now,
                                       first_outcome=ReceiptOutcome.BUFFERED.value,
                                       body_hash=body_hash)
                    return {"outcome": ReceiptOutcome.BUFFERED.value, "duplicate": True,
                            "seq": seq, "device_id": device_id,
                            "reason": "乱序回执已暂存，重复提交返回首次结论"}
                self._record_audit(ev.EV_RECEIPT_CONFLICT, receipt, now,
                                   reason="同号不同正文（与已暂存回执冲突）",
                                   body_hash=body_hash)
                return {"outcome": ReceiptOutcome.CONFLICT.value, "duplicate": False,
                        "seq": seq, "device_id": device_id,
                        "reason": "同号不同正文，拒绝"}

            # 摘要校验（只对首次正文）
            if not verify_checksum(receipt):
                return self._reject(receipt, key, now, body_hash,
                                    "checksum_mismatch", "摘要校验失败")

            # 包版本静态校验
            pkg = self.packages.get(receipt.get("package_id"))
            if pkg is None:
                return self._reject(receipt, key, now, body_hash,
                                    "unknown_package",
                                    f"未知地图包: {receipt.get('package_id')}")
            cmp = compare_versions(str(receipt.get("version")), pkg.version)
            if cmp < 0:
                return self._reject(receipt, key, now, body_hash,
                                    "stale_package",
                                    f"旧版本包回执: {receipt.get('version')} < 当前 {pkg.version}")
            if cmp > 0:
                return self._reject(receipt, key, now, body_hash,
                                    "future_package",
                                    f"回执版本高于目录版本: {receipt.get('version')} > {pkg.version}")

            # 批次 / 投放定位
            batch = self.batches.get(batch_id)
            if batch is None:
                return self._reject(receipt, key, now, body_hash,
                                    "unknown_batch", f"未知批次: {batch_id}")
            delivery_id = batch.device_delivery.get(device_id)
            d = batch.deliveries.get(delivery_id) if delivery_id else None
            if d is None:
                return self._reject(receipt, key, now, body_hash,
                                    "unknown_delivery", "该设备在此批次中没有投放记录")
            if batch.package_id != receipt["package_id"]:
                return self._reject(receipt, key, now, body_hash,
                                    "delivery_mismatch", "回执包与批次投放的包不一致")

            # 乱序先于状态匹配：seq 大于游标一律暂存（设备超时待重试也先暂存）
            cursor = self.next_seq.get(self._cursor(batch_id, device_id), 1)
            if seq > cursor:
                return self._buffer(receipt, key, now, body_hash)
            # seq 小于游标可能是撤销清理跳号后迟到的前置回执：
            # 不按“越过游标”拒绝，交给下面的投放状态给出确定结论。

            # 在途且序号匹配才接受；否则按投放当前状态裁决
            if d.state is DeliveryState.IN_FLIGHT and d.seq == seq:
                return self._accept_and_drain(receipt, key, now, body_hash, batch, d)
            # 投放恰在待重试/待投放：继续暂存，等重投后按序补齐
            if d.state in BUFFERABLE_STATES:
                return self._buffer(receipt, key, now, body_hash)
            if d.state is DeliveryState.ACKED:
                return self._reject(receipt, key, now, body_hash,
                                    "delivery_acked", "该投放已有回执，序号不匹配的回执无效")
            if d.state is DeliveryState.CANCELED:
                return self._reject(receipt, key, now, body_hash,
                                    "delivery_canceled", "投放已随批次撤销取消")
            if d.state is DeliveryState.FAILED:
                return self._reject(receipt, key, now, body_hash,
                                    "delivery_failed", "投放已最终失败，回执无效")
            return self._reject(receipt, key, now, body_hash, "delivery_stale",
                                f"序号 {seq} 不属于当前在途投放（当前序号 {d.seq}），为过期回执")

    # –– 回执分支辅助 -----------------------------------------------------

    def _handle_unknown_device(self, receipt, batch_id, device_id, seq, now) -> dict:
        key: ReceiptKey = (batch_id, device_id, seq)
        body_hash = compute_checksum(receipt)
        if key in self.unknown_index:
            first = self.unknown_index[key]
            if first["body_hash"] == body_hash:
                self._record_audit(ev.EV_RECEIPT_DUPLICATE, receipt, now,
                                   first_outcome=first["outcome"], body_hash=body_hash)
                return {"outcome": first["outcome"], "duplicate": True,
                        "seq": seq, "device_id": device_id,
                        "reason": "重复回执返回首次结果: " + first["reason"]}
            self._record_audit(ev.EV_RECEIPT_CONFLICT, receipt, now,
                               reason="同号不同正文（未知设备）", body_hash=body_hash)
            return {"outcome": ReceiptOutcome.CONFLICT.value, "duplicate": False,
                    "seq": seq, "device_id": device_id, "reason": "同号不同正文，拒绝"}
        self._record_audit(ev.EV_RECEIPT_REJECTED, receipt, now,
                           reason="未知设备", reason_code="unknown_device",
                           body_hash=body_hash)
        self.unknown_index[key] = {"outcome": ReceiptOutcome.REJECTED.value,
                                   "body_hash": body_hash, "reason": "未知设备"}
        return {"outcome": ReceiptOutcome.REJECTED.value, "duplicate": False,
                "seq": seq, "device_id": device_id, "reason": "未知设备，回执拒绝"}

    def _duplicate_or_conflict(self, first, receipt, key: ReceiptKey, now, hash_ok) -> dict:
        batch_id, device_id, seq = key
        body_hash = compute_checksum(receipt)
        if hash_ok:
            self._record_audit(ev.EV_RECEIPT_DUPLICATE, receipt, now,
                               first_outcome=first["outcome"], body_hash=body_hash)
            self._pump(batch_id, device_id, now)
            return {"outcome": first["outcome"], "duplicate": True,
                    "seq": seq, "device_id": device_id,
                    "reason": "重复回执，返回首次结果",
                    "first_reason": first.get("reason")}
        self._record_audit(ev.EV_RECEIPT_CONFLICT, receipt, now,
                           reason="同号不同正文", body_hash=body_hash)
        return {"outcome": ReceiptOutcome.CONFLICT.value, "duplicate": False,
                "seq": seq, "device_id": device_id,
                "reason": f"同设备同序号 {seq} 出现不同正文，拒绝"}

    def _record_audit(self, event_type, receipt, now, **extra) -> dict:
        payload = {"device_id": receipt.get("device_id"), "seq": receipt.get("seq"),
                   "package_id": receipt.get("package_id"),
                   "batch_id": receipt.get("batch_id"),
                   "version": receipt.get("version")}
        payload.update(extra)
        return self._emit(event_type, payload, now)

    def _reject(self, receipt, key: ReceiptKey, now, body_hash, code, reason) -> dict:
        batch_id, device_id, seq = key
        self._record_audit(ev.EV_RECEIPT_REJECTED, receipt, now,
                           reason=reason, reason_code=code, body_hash=body_hash)
        self.receipt_index[key] = {"outcome": ReceiptOutcome.REJECTED.value,
                                   "body_hash": body_hash, "reason": reason,
                                   "reason_code": code}
        # 拒绝也尝试推进游标（按序拒绝可放行后续暂存链）
        drained = self._pump(batch_id, device_id, now)
        return {"outcome": ReceiptOutcome.REJECTED.value, "duplicate": False,
                "seq": seq, "device_id": device_id,
                "reason_code": code, "reason": reason, "drained": drained}

    def _buffer(self, receipt, key: ReceiptKey, now, body_hash) -> dict:
        batch_id, device_id, seq = key
        self._emit(ev.EV_RECEIPT_BUFFERED, {
            "device_id": device_id, "seq": seq, "batch_id": batch_id,
            "package_id": receipt.get("package_id"),
            "version": receipt.get("version"), "body_hash": body_hash,
        }, now)
        self.buffered[key] = {**receipt, "body_hash": body_hash}
        cursor = self.next_seq.get(self._cursor(batch_id, device_id), 1)
        return {"outcome": ReceiptOutcome.BUFFERED.value, "duplicate": False,
                "seq": seq, "device_id": device_id,
                "reason": f"乱序回执：等待序号 {cursor} 先到达，已暂存"}

    def _accept_and_drain(self, receipt, key, now, body_hash, batch, d) -> dict:
        batch_id, device_id, seq = key
        self._emit(ev.EV_RECEIPT_ACCEPTED, {
            "device_id": device_id, "seq": seq,
            "batch_id": batch.batch_id, "package_id": batch.package_id,
            "delivery_id": d.delivery_id, "body_hash": body_hash,
            "received_at": now,
        }, now)
        self._emit(ev.EV_DELIVERY_ACKED, {
            "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
            "device_id": device_id, "group": d.group, "seq": seq, "acked_at": now,
        }, now)
        self.receipt_index[key] = {"outcome": ReceiptOutcome.ACCEPTED.value,
                                   "body_hash": body_hash,
                                   "delivery_id": d.delivery_id}
        self.next_seq[self._cursor(batch_id, device_id)] = seq + 1

        first = {"outcome": ReceiptOutcome.ACCEPTED.value, "seq": seq,
                 "device_id": device_id, "batch_id": batch.batch_id}
        self._settle_batch(batch, now)
        drained = self._pump(batch_id, device_id, now)
        return {"outcome": ReceiptOutcome.ACCEPTED.value, "duplicate": False,
                "seq": seq, "device_id": device_id,
                "batch_id": batch.batch_id, "delivery_id": d.delivery_id,
                "drained": drained}

    def _pump(self, batch_id: str, device_id: str, now: float) -> list[dict]:
        """推进 (批次,设备) 游标：跳过已裁决序号，按序补齐暂存链。"""
        results = []
        ck = self._cursor(batch_id, device_id)
        while True:
            cursor = self.next_seq.get(ck, 1)
            key = (batch_id, device_id, cursor)
            if key in self.receipt_index:
                self.next_seq[ck] = cursor + 1
                continue
            receipt = self.buffered.get(key)
            if receipt is None:
                break
            body_hash = receipt["body_hash"]
            del self.buffered[key]
            decision = self._decide_buffered(receipt, key, now, body_hash)
            if decision["outcome"] == "waiting":
                self.buffered[key] = receipt
                break
            results.append(decision)
            self.next_seq[ck] = cursor + 1
        return results

    def _decide_buffered(self, receipt, key: ReceiptKey, now, body_hash) -> dict:
        """暂存期满后的最终业务裁决（包/批次状态可能已变化）。"""
        batch_id, device_id, seq = key
        batch = self.batches.get(batch_id)
        if batch is None:
            return self._index_reject(receipt, key, now, body_hash,
                                      "unknown_batch", f"未知批次: {batch_id}")
        d = batch.deliveries.get(batch.device_delivery.get(device_id, ""))
        if d is None:
            return self._index_reject(receipt, key, now, body_hash,
                                      "unknown_delivery", "没有投放记录")
        if d.state is DeliveryState.IN_FLIGHT and d.seq == seq:
            self._emit(ev.EV_RECEIPT_ACCEPTED, {
                "device_id": device_id, "seq": seq, "batch_id": batch_id,
                "package_id": batch.package_id, "delivery_id": d.delivery_id,
                "body_hash": body_hash, "received_at": now, "from_buffer": True,
            }, now)
            self._emit(ev.EV_DELIVERY_ACKED, {
                "delivery_id": d.delivery_id, "batch_id": batch_id,
                "device_id": device_id, "group": d.group, "seq": seq, "acked_at": now,
            }, now)
            self.receipt_index[key] = {"outcome": ReceiptOutcome.ACCEPTED.value,
                                       "body_hash": body_hash,
                                       "delivery_id": d.delivery_id}
            self._settle_batch(batch, now)
            return {"outcome": ReceiptOutcome.ACCEPTED.value, "seq": seq,
                    "device_id": device_id, "batch_id": batch_id, "from_buffer": True}
        if d.state in BUFFERABLE_STATES:
            return {"outcome": "waiting", "seq": seq, "device_id": device_id}
        reason_map = {
            DeliveryState.ACKED: ("delivery_acked", "该投放已被其他序号回执确认"),
            DeliveryState.CANCELED: ("delivery_canceled", "投放已随批次撤销取消"),
            DeliveryState.FAILED: ("delivery_failed", "投放已最终失败"),
        }
        code, reason = reason_map.get(
            d.state, ("delivery_stale", f"当前投放序号为 {d.seq}，原序号失效"))
        return self._index_reject(receipt, key, now, body_hash, code, reason)

    def _index_reject(self, receipt, key, now, body_hash, code, reason) -> dict:
        batch_id, device_id, seq = key
        self._record_audit(ev.EV_RECEIPT_REJECTED, receipt, now,
                           reason=reason, reason_code=code, body_hash=body_hash,
                           from_buffer=True)
        self.receipt_index[key] = {"outcome": ReceiptOutcome.REJECTED.value,
                                   "body_hash": body_hash, "reason": reason,
                                   "reason_code": code}
        return {"outcome": ReceiptOutcome.REJECTED.value, "seq": seq,
                "device_id": device_id, "reason_code": code, "reason": reason,
                "from_buffer": True}

    def _abandon_seq(self, batch_id: str, device_id: str, seq: int) -> None:
        """超时重投：旧序号标记为 delivery_retried（权威首次结论）。

        不立即 pump——同拍稍后会重投新序号，暂存链补齐统一在 tick 末尾。
        """
        key = (batch_id, device_id, seq)
        if key not in self.receipt_index:
            self.receipt_index[key] = {
                "outcome": ReceiptOutcome.REJECTED.value,
                "body_hash": None,
                "reason": "投放超时已重试，该序号作废",
                "reason_code": "delivery_retried"}

    def _drain_all_buffered(self, batch_id: str, now: float) -> None:
        """批次进入终态：对仍暂存的所有回执做最终拒绝（不依赖游标连续）。"""
        keys = sorted((k for k in self.buffered if k[0] == batch_id),
                      key=lambda k: (k[1], k[2]))
        for key in keys:
            receipt = self.buffered.pop(key, None)
            if receipt is None:
                continue
            _b, device_id, seq = key
            body_hash = receipt["body_hash"]
            batch = self.batches.get(batch_id)
            d = batch.deliveries.get(batch.device_delivery.get(device_id, "")) if batch else None
            reason_map = {
                DeliveryState.CANCELED: ("delivery_canceled", "投放已随批次终结被取消"),
                DeliveryState.FAILED: ("delivery_failed", "投放已失败，暂存回执拒绝"),
                DeliveryState.PENDING: ("batch_terminal", "批次终结，设备未及投放"),
                DeliveryState.RETRY_WAIT: ("batch_terminal", "批次终结，待重试不再进行"),
            }
            code, reason = reason_map.get(
                d.state if d else None,
                ("batch_terminal", "批次已终结，暂存回执不再有效"))
            self._record_audit(ev.EV_RECEIPT_REJECTED, receipt, now,
                               reason=reason, reason_code=code,
                               body_hash=body_hash, from_buffer=True)
            self.receipt_index[key] = {"outcome": ReceiptOutcome.REJECTED.value,
                                       "body_hash": body_hash, "reason": reason,
                                       "reason_code": code}
            self.next_seq[self._cursor(batch_id, device_id)] = seq + 1

    def buffered_pairs(self):
        return {(b, dev) for (b, dev, _s) in self.buffered}

    # ------------------------------------------------------------- 调度推进

    def tick(self, now: Optional[float] = None) -> dict:
        """推进所有批次（全部在同一把锁内）。

          1. 时间窗开始 → QUEUED 激活为 DISPATCHING；
          2. 在途超时 → RETRY_WAIT（未耗尽）或 FAILED（耗尽）；
          3. 时间窗结束 → 未投放项失败；
          4. 批次结算（达标完成 / 穷尽失败）；
          5. 按分组并发上限，待重试优先投放；
          6. 补齐可能恰好命中新在途序号的暂存回执。
        """
        with self.lock:
            now = self.clock.now() if now is None else now
            dispatched, timed_out = [], []

            for batch in list(self.batches.values()):
                if batch.status is BatchStatus.QUEUED and now >= batch.window_start:
                    self._emit(ev.EV_BATCH_ACTIVATED,
                               {"batch_id": batch.batch_id}, now)
                if batch.status in (BatchStatus.DISPATCHING, BatchStatus.PAUSED):
                    timed_out += self._expire_timeouts(batch, now)
                    if now >= batch.window_end:
                        self._close_window(batch, now)
                self._settle_batch(batch, now)

            touched: set[CursorKey] = set()
            for group in sorted({g for b in self.batches.values()
                                 if b.status is BatchStatus.DISPATCHING
                                 for g in b.groups}):
                for did in self._dispatch_group(group, now):
                    for b in self.batches.values():
                        if did in b.deliveries:
                            dd = b.deliveries[did]
                            dispatched.append(did)
                            touched.add(self._cursor(b.batch_id, dd.device_id))
                            break

            # 新投放/超时作废后，统一按序补齐暂存链
            for ck in touched | self.buffered_pairs():
                self._pump(ck[0], ck[1], now)

            for batch in list(self.batches.values()):
                self._settle_batch(batch, now)

            return {"ts": now, "dispatched": dispatched, "timed_out": timed_out}

    def _expire_timeouts(self, batch: Batch, now: float) -> list[str]:
        out = []
        for d in batch.deliveries.values():
            if d.state is DeliveryState.IN_FLIGHT and now >= d.deadline:
                if d.attempt >= batch.max_attempts:
                    self._emit(ev.EV_DELIVERY_FAILED, {
                        "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
                        "device_id": d.device_id, "group": d.group,
                        "reason": f"超时且重试耗尽（{d.attempt}/{batch.max_attempts}）",
                        "attempts": d.attempt, "at": now,
                    }, now)
                else:
                    old_seq = d.seq
                    self._emit(ev.EV_DELIVERY_TIMEOUT, {
                        "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
                        "device_id": d.device_id, "group": d.group,
                        "seq": old_seq, "attempt": d.attempt,
                        "next_attempt": d.attempt + 1,
                        "retry_at": now, "at": now,
                    }, now)
                    self._abandon_seq(batch.batch_id, d.device_id, old_seq)
                out.append(d.delivery_id)
        return out

    def _close_window(self, batch: Batch, now: float) -> None:
        pending = [d for d in batch.deliveries.values()
                   if d.state in (DeliveryState.PENDING, DeliveryState.RETRY_WAIT)]
        if not pending:
            return
        self._emit(ev.EV_BATCH_WINDOW_CLOSED, {
            "batch_id": batch.batch_id, "window_end": batch.window_end,
            "canceled_pending": [d.delivery_id for d in pending],
        }, now)
        for d in pending:
            self._emit(ev.EV_DELIVERY_FAILED, {
                "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
                "device_id": d.device_id, "group": d.group,
                "reason": "发布时间窗已关闭，未能完成投放", "at": now,
            }, now)

    def _group_inflight(self, group: str) -> int:
        # 暂停批次的在途仍占名额（回执随时可能到达）；撤销/终态已释放。
        return sum(
            1 for batch in self.batches.values()
            if group in batch.groups
            and batch.status in (BatchStatus.DISPATCHING, BatchStatus.PAUSED)
            for d in batch.deliveries.values()
            if d.group == group and d.state.occupies_slot)

    def _group_cap(self, group: str) -> int:
        """分组有效上限 = 组内在投批次配置上限的最大值。"""
        caps = [b.max_concurrency for b in self.batches.values()
                if b.status is BatchStatus.DISPATCHING and group in b.groups]
        return max(caps, default=0)

    def _dispatch_group(self, group: str, now: float) -> list[str]:
        """待重试优先（FIFO），再普通待投放；绝不突破分组并发上限。"""
        out: list[str] = []
        active = [b for b in self.batches.values()
                  if b.status is BatchStatus.DISPATCHING and group in b.groups
                  and now < b.window_end]
        retry_items, fresh_items = [], []
        for b in active:
            for d in b.deliveries.values():
                if d.group != group:
                    continue
                if d.state is DeliveryState.RETRY_WAIT:
                    retry_items.append((b, d))
                elif d.state is DeliveryState.PENDING:
                    fresh_items.append((b, d))
        retry_items.sort(key=lambda x: (x[1].retry_at or 0, x[1].enqueued_at))
        fresh_items.sort(key=lambda x: x[1].enqueued_at)

        for b, d in retry_items + fresh_items:
            cap = self._group_cap(group)
            if cap <= 0 or self._group_inflight(group) >= cap:
                break  # 分组并发已满；待重试也不得突破
            if b.in_flight_count >= b.max_concurrency:
                continue
            self._dispatch(b, d, now)
            out.append(d.delivery_id)
        return out

    def _dispatch(self, batch: Batch, d: Delivery, now: float) -> None:
        # 批次内每设备序号从 1 开始；超时事件已把 attempt 置为下一次尝试号，
        # 重投直接沿用（首投 attempt=1）。
        new_seq = d.attempt if d.state is DeliveryState.RETRY_WAIT else 1
        self._emit(ev.EV_DELIVERY_DISPATCHED, {
            "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
            "device_id": d.device_id, "group": d.group,
            "seq": new_seq, "attempt": new_seq,
            "dispatched_at": now,
            "deadline": now + batch.timeout_secs,
        }, now)

    def _settle_batch(self, batch: Batch, now: float) -> bool:
        if batch.status.terminal:
            return False
        if batch.acked_count >= batch.ack_threshold:
            self._emit(ev.EV_BATCH_COMPLETED, {
                "batch_id": batch.batch_id,
                "acked_count": batch.acked_count,
                "threshold": batch.ack_threshold, "at": now,
            }, now)
            for d in batch.deliveries.values():
                if not d.state.terminal:
                    self._emit(ev.EV_DELIVERY_CANCELED, {
                        "delivery_id": d.delivery_id, "batch_id": batch.batch_id,
                        "device_id": d.device_id, "group": d.group,
                        "reason": "batch_completed",
                    }, now)
            self._drain_all_buffered(batch.batch_id, now)
            return True
        if now >= batch.window_end:
            if not any(d.state is DeliveryState.IN_FLIGHT
                       for d in batch.deliveries.values()):
                self._emit(ev.EV_BATCH_FAILED, {
                    "batch_id": batch.batch_id,
                    "acked_count": batch.acked_count,
                    "threshold": batch.ack_threshold,
                    "reason": "时间窗结束且无在途设备，未达回执阈值", "at": now,
                }, now)
                self._drain_all_buffered(batch.batch_id, now)
                return True
        return False

    # ------------------------------------------------------------- 查询视图

    def batch_view(self, batch_id: str) -> dict:
        with self.lock:
            batch = self._require_batch(batch_id)
            view = batch.to_dict()
            view["deliveries"] = [d.to_dict()
                                  for d in sorted(batch.deliveries.values(),
                                                  key=lambda x: x.enqueued_at)]
            counts: dict[str, int] = {}
            for d in batch.deliveries.values():
                counts[d.state.value] = counts.get(d.state.value, 0) + 1
            view["state_counts"] = counts
            view["buffered_seqs"] = sorted(
                s for (b, _dev, s) in self.buffered if b == batch_id)
            view["next_seq"] = {
                dev: self.next_seq.get(self._cursor(batch_id, dev), 1)
                for dev in batch.device_ids}
            return view

    def list_batches(self) -> list[dict]:
        with self.lock:
            return [self.batch_view(bid) for bid in sorted(self.batches)]

    def timeline(self, batch_id: str, group: Optional[str] = None,
                 device_id: Optional[str] = None) -> list[dict]:
        with self.lock:
            batch = self._require_batch(batch_id)
            out = []
            for e in self.store.replay():
                p = e.get("payload", {})
                # 时间线只包含明确属于本批次的事件（回执事件均带 batch_id）
                if p.get("batch_id") != batch_id:
                    continue
                if device_id and p.get("device_id") != device_id:
                    continue
                if group and p.get("group") != group and "group" in p:
                    continue
                out.append(e)
            return out

    # ------------------------------------------------------------- 事件重放

    def _replay(self) -> None:
        for e in self.store.replay():
            self._apply(e["type"], e["payload"], e["ts"])
        # 游标对齐：为每个有投放的 (批次,设备) 初始化游标，并跳过已裁决序号
        # （覆盖“仅超时作废、尚未收到任何回执”的设备）。
        for batch in self.batches.values():
            for dev in batch.device_delivery:
                ck = self._cursor(batch.batch_id, dev)
                cursor = self.next_seq.get(ck, 1)
                while (batch.batch_id, dev, cursor) in self.receipt_index:
                    cursor += 1
                self.next_seq[ck] = cursor

    def _apply(self, etype: str, p: dict, ts: float) -> None:
        # -- 注册 ----------------------------------------------------------
        if etype == ev.EV_DEVICE_REGISTERED:
            self.devices[p["device_id"]] = Device(
                p["device_id"], p["group"], p.get("name", ""))
            return
        if etype == ev.EV_PACKAGE_REGISTERED:
            self.packages[p["package_id"]] = Package(
                p["package_id"], p["version"], p.get("name", ""))
            return

        # -- 批次生命周期 --------------------------------------------------
        if etype == ev.EV_BATCH_CREATED:
            batch = Batch(
                batch_id=p["batch_id"], package_id=p["package_id"],
                package_version=p["package_version"], groups=list(p["groups"]),
                device_ids=list(p["device_ids"]),
                window_start=p["window_start"], window_end=p["window_end"],
                max_concurrency=p["max_concurrency"], ack_threshold=p["ack_threshold"],
                timeout_secs=p.get("timeout_secs", self.default_timeout_secs),
                max_attempts=p.get("max_attempts", self.default_max_attempts),
                status=BatchStatus(p["status"]), created_at=p.get("created_at", ts))
            self.batches[batch.batch_id] = batch
            return
        if etype in (ev.EV_BATCH_ACTIVATED, ev.EV_BATCH_PAUSED, ev.EV_BATCH_RESUMED,
                     ev.EV_BATCH_REVOKED, ev.EV_BATCH_COMPLETED, ev.EV_BATCH_FAILED):
            mapping = {
                ev.EV_BATCH_ACTIVATED: BatchStatus.DISPATCHING,
                ev.EV_BATCH_PAUSED: BatchStatus.PAUSED,
                ev.EV_BATCH_RESUMED: BatchStatus.DISPATCHING,
                ev.EV_BATCH_REVOKED: BatchStatus.REVOKED,
                ev.EV_BATCH_COMPLETED: BatchStatus.COMPLETED,
                ev.EV_BATCH_FAILED: BatchStatus.FAILED,
            }
            b = self.batches.get(p["batch_id"])
            if b is not None:
                b.status = mapping[etype]
            return
        if etype == ev.EV_BATCH_WINDOW_CLOSED:
            return

        batch = self.batches.get(p.get("batch_id", ""))

        # -- 投放推进 ------------------------------------------------------
        if etype == ev.EV_DELIVERY_QUEUED and batch is not None:
            d = Delivery(delivery_id=p["delivery_id"], batch_id=batch.batch_id,
                         device_id=p["device_id"], group=p["group"], seq=0,
                         state=DeliveryState.PENDING, enqueued_at=p.get("enqueued_at", ts))
            batch.deliveries[d.delivery_id] = d
            batch.device_delivery[d.device_id] = d.delivery_id
            return
        if etype == ev.EV_DELIVERY_DISPATCHED and batch is not None:
            d = batch.deliveries[p["delivery_id"]]
            d.state = DeliveryState.IN_FLIGHT
            d.attempt = p["attempt"]
            d.seq = p["seq"]
            d.dispatched_at = p["dispatched_at"]
            d.deadline = p["deadline"]
            d.retry_at = None
            return
        if etype == ev.EV_DELIVERY_TIMEOUT and batch is not None:
            d = batch.deliveries[p["delivery_id"]]
            d.state = DeliveryState.RETRY_WAIT
            d.attempt = p["next_attempt"]
            d.retry_at = p.get("retry_at", ts)
            d.deadline = 0.0
            key = (batch.batch_id, d.device_id, p["seq"])
            self.receipt_index.setdefault(key, {
                "outcome": ReceiptOutcome.REJECTED.value, "body_hash": None,
                "reason": "投放超时已重试，该序号作废",
                "reason_code": "delivery_retried"})
            return
        if etype == ev.EV_DELIVERY_ACKED and batch is not None:
            d = batch.deliveries[p["delivery_id"]]
            d.state = DeliveryState.ACKED
            d.acked_at = p.get("acked_at", ts)
            d.deadline = 0.0
            d.seq = p.get("seq", d.seq)
            return
        if etype == ev.EV_DELIVERY_FAILED and batch is not None:
            d = batch.deliveries[p["delivery_id"]]
            d.state = DeliveryState.FAILED
            d.fail_reason = p.get("reason")
            d.deadline = 0.0
            return
        if etype == ev.EV_DELIVERY_CANCELED and batch is not None:
            d = batch.deliveries[p["delivery_id"]]
            d.state = DeliveryState.CANCELED
            d.canceled_reason = p.get("reason")
            d.deadline = 0.0
            return

        # -- 回执 ----------------------------------------------------------
        device_id, seq = p.get("device_id"), p.get("seq")
        if device_id is None or seq is None:
            return
        batch_id = p.get("batch_id")
        key = (batch_id, device_id, seq)

        if device_id not in self.devices:
            if etype == ev.EV_RECEIPT_REJECTED:
                self.unknown_index[key] = {
                    "outcome": ReceiptOutcome.REJECTED.value,
                    "body_hash": p.get("body_hash"),
                    "reason": p.get("reason"),
                    "reason_code": p.get("reason_code")}
            return

        if etype == ev.EV_RECEIPT_ACCEPTED:
            self.buffered.pop(key, None)
            self.receipt_index[key] = {
                "outcome": ReceiptOutcome.ACCEPTED.value,
                "body_hash": p.get("body_hash"),
                "delivery_id": p.get("delivery_id")}
            self.next_seq[(batch_id, device_id)] = seq + 1
            return
        if etype == ev.EV_RECEIPT_BUFFERED:
            if key not in self.buffered:
                self.buffered[key] = {
                    "device_id": device_id, "seq": seq, "batch_id": batch_id,
                    "package_id": p.get("package_id"),
                    "version": p.get("version"), "body_hash": p.get("body_hash")}
            return
        if etype == ev.EV_RECEIPT_REJECTED:
            self.buffered.pop(key, None)
            self.receipt_index[key] = {
                "outcome": ReceiptOutcome.REJECTED.value,
                "body_hash": p.get("body_hash"),
                "reason": p.get("reason"),
                "reason_code": p.get("reason_code")}
            cursor = self.next_seq.get((batch_id, device_id), 1)
            if seq == cursor:
                self.next_seq[(batch_id, device_id)] = seq + 1
            return
        if etype in ev.AUDIT_ONLY_EVENTS:
            return


class Scheduler:
    """后台调度器：固定间隔驱动 tick。"""

    def __init__(self, orch: Orchestrator, interval_secs: float = 1.0,
                 on_tick: Optional[Callable[[dict], None]] = None):
        self.orch = orch
        self.interval = interval_secs
        self.on_tick = on_tick
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="dispatcher",
                                        daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                result = self.orch.tick()
                if self.on_tick and (result["dispatched"] or result["timed_out"]):
                    self.on_tick(result)
            except Exception:
                pass

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
