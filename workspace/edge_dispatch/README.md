# 边缘地图包 · 分批投放编排器

操作员通过**网页**和**命令行**创建投放批次：选择设备分组、发布时间窗、
分组并发上限与回执阈值；调度器按分组推进批次，实时展示排队 / 投放 /
暂停 / 完成 / 撤销，并输出可下载的批次摘要（JSON / CSV）。

设备回执经独立入口流入，满足严格的“只处理一次”与乱序/冲突语义；
服务崩溃后从追加日志完整恢复。

## 快速开始

```bash
pip install -r ../requirements.txt

# 从仓库根目录启动（首次自动种子化：north/east 两个分组 + 两个地图包版本）
PYTHONPATH=/workspace python -m edge_dispatch.cli serve --data ./data --port 8088
# 浏览器打开 http://127.0.0.1:8088
```

命令行（另开终端，服务需在运行）：

```bash
export EDGE_BASE_URL=http://127.0.0.1:8088
export PYTHONPATH=/workspace

# 创建批次（north 分组、并发 2、回执阈值 3、超时 30s、最多 3 次尝试）
python -m edge_dispatch.cli batches create --package map-basemap \
    --groups north --end 2026-10-06T13:00:00Z \
    --concurrency 2 --threshold 3 --timeout 30 --max-attempts 3

# 暂停 / 恢复 / 撤销
python -m edge_dispatch.cli batches pause  <batch_id>
python -m edge_dispatch.cli batches resume <batch_id>
python -m edge_dispatch.cli batches revoke <batch_id>

# 设备回执（checksum 由 CLI 按规范自动计算；--bad-checksum 可验证拒绝链路）
python -m edge_dispatch.cli receipt --device edge-north-01 --seq 1 \
    --package map-basemap --version 2.4.1 --batch <batch_id>

# 完整时间线（可按分组/设备过滤）与摘要下载
python -m edge_dispatch.cli timeline <batch_id> --device edge-north-01
python -m edge_dispatch.cli summary  <batch_id> --format csv -o s.csv
```

## 关键语义

| 主题 | 保证 |
|------|------|
| 只处理一次 | 回执幂等键 `(批次, 设备, 序号)`；重复回执返回**首次结果**（含首次拒绝），绝不重复计票 |
| 乱序 | 序号大于游标先**暂存**；前置序号裁决后按序补齐（等待期间超时重投则命中新序号，批次终结则拒绝） |
| 同号冲突 | 同 `(批次,设备,序号)` 不同正文（SHA-256 摘要不同）→ `conflict` 拒绝并记录，首次结论不变 |
| 暂停/撤销竞争 | 全部命令经同一把锁串行、先写追加日志再改内存；先入日志者胜，终态唯一确定 |
| 超时重试 | 超时入待重试队列（耗尽则失败）；重试优先于新投放，严格不突破分组并发上限 |
| 崩溃恢复 | 所有状态变更为 JSONL 追加事件（fsync）；重启重放恢复队列、在途、计数、时间窗、暂存区 |
| 拒绝并记录 | 旧版本包 / 未知设备 / 摘要校验失败 / 未知批次 → `rejected` + 原因码，进入审计时间线 |
| 时间线 | 批次完成后可按分组、设备查看完整事件流 |

序号在**每个批次内按设备**从 1 开始（每次重试递增），因此同一设备在
不同批次（一次独立投放会话）里序号互不干扰。

## 回执摘要规范

回执为 JSON 对象，必含 `device_id / seq / package_id / version / batch_id / checksum`。
`checksum` = 移除自身后，键排序、无空白的 UTF-8 JSON 的 SHA-256（见 `receipts.py`）。

## HTTP 接口

- `POST /api/batches` 创建批次；`GET /api/batches[/{id}]` 列表/详情
- `POST /api/batches/{id}/pause|resume|revoke`
- `POST /api/receipts` 回执独立入口；`POST /api/receipts/with-checksum`（演示用，服务端补摘要）
- `GET /api/batches/{id}/timeline?group=&device_id=`
- `GET /api/batches/{id}/summary?format=json|csv`
- `GET /api/events`（SSE 实时事件流）、`POST /api/tick`（手动推进一拍）
- `GET /api/devices|/api/packages|/api/groups`

## 模块结构

```
events.py        追加式事件日志（唯一持久化事实来源，fsync + 内存环形缓冲）
models.py        批次/投放状态机与枚举
receipts.py      回执规范化、SHA-256 摘要、语义化版本比较
orchestrator.py  核心：锁串行化、回执裁决、调度推进、超时重试、事件重放
api.py           FastAPI REST + SSE + 摘要导出
cli.py           urllib 标准库命令行（含 serve 子命令）
web/index.html   单文件看板（创建/暂停/恢复/撤销/回执模拟/时间线/摘要，SSE 实时刷新）
tests/           23 个验收与并发压力测试
```

## 验收测试

```bash
cd edge_dispatch
python -m pytest -q
```

覆盖：正常投放、重复回执、乱序回执、同号冲突、暂停/撤销竞争（含多线程压力）、
超时重试与并发上限、崩溃重启恢复、旧包/未知设备/坏摘要拒绝。
