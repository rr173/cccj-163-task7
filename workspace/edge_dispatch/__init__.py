"""边缘地图包分批投放编排器。

模块划分：
  models       领域枚举与数据结构
  receipts     回执正文规范化、摘要与校验
  events       追加式事件日志（崩溃恢复的唯一事实来源）
  orchestrator 核心状态机：批次推进、回执处理、暂停/撤销竞争、超时重试
  seed         内置设备分组与地图包目录
  api          FastAPI / SSE HTTP 入口
  cli          命令行入口（标准库 urllib）
"""

__version__ = "1.0.0"
