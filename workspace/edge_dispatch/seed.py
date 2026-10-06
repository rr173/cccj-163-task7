"""内置种子数据：两个设备分组与一个地图包目录，方便网页直接演示。"""
from __future__ import annotations

from .orchestrator import Orchestrator

SEED_DEVICES = [
    # 华北路边节点
    ("edge-north-01", "north", "华北-节点01"),
    ("edge-north-02", "north", "华北-节点02"),
    ("edge-north-03", "north", "华北-节点03"),
    ("edge-north-04", "north", "华北-节点04"),
    # 华东路边节点
    ("edge-east-01", "east", "华东-节点01"),
    ("edge-east-02", "east", "华东-节点02"),
    ("edge-east-03", "east", "华东-节点03"),
]

SEED_PACKAGES = [
    ("map-basemap", "2.4.1", "全国基础地图包"),
    ("map-poi", "1.8.0", "POI 兴趣点包"),
]


def standard_seed(orch: Orchestrator) -> None:
    for device_id, group, name in SEED_DEVICES:
        if device_id not in orch.devices:
            orch.register_device(device_id, group, name)
    for package_id, version, name in SEED_PACKAGES:
        if package_id not in orch.packages:
            orch.register_package(package_id, version, name)
